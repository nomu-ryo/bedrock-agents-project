// @ts-check
// 必要なモジュールをCommonJS形式で読み込みます
const { 
    S3Client, 
    GetObjectCommand, 
    PutObjectCommand,
} = require("@aws-sdk/client-s3"); 

// ファイルシステム操作ライブラリ
const { promisify } = require('util');
const fs = require('fs');
const path = require('path'); 

// 💡 Layerからのモジュールロードを試みます。
// @ts-ignore
let chromium; 
// @ts-ignore
let puppeteer; 

// promisifyを使用して、コールバックベースのfs関数をPromiseベースに変換
const unlink = promisify(fs.unlink);
const writeFile = promisify(fs.pathExists ? fs.writeFile : (p, d, o) => { throw new Error("fs.writeFile not available directly."); });
const existsSync = fs.existsSync; // fs.existsSyncはpromisify不要

// --- 定数定義 ---
const TMP_DIR = '/tmp';
const FULL_OUTPUT_FILE_NAME = 'full_diagram.png';
const TMP_FULL_OUTPUT_PATH = path.join(TMP_DIR, FULL_OUTPUT_FILE_NAME);
const DIFF_OUTPUT_FILE_NAME = 'diff_diagram.png';
const TMP_DIFF_OUTPUT_PATH = path.join(TMP_DIR, DIFF_OUTPUT_FILE_NAME);
const TEMP_HTML_FILE_NAME = 'mermaid_render.html';
const TMP_HTML_PATH = path.join(TMP_DIR, TEMP_HTML_FILE_NAME);

// 💡 Layerに配置されたローカルパスを想定
const MERMAID_LOCAL_PATH = '/opt/nodejs/node_modules/mermaid/dist/mermaid.min.js'; 

// 🚨 修正: 内部タイムアウトをLambdaの最大実行時間(150秒)より短く(140秒)設定
const RENDER_TIMEOUT_MS = 140000; 

// S3クライアントの初期化
const s3 = new S3Client({ region: process.env.AWS_REGION });

// --- タイム計測ユーティリティ関数 (そのまま) ---

/** @type {bigint} */
let startTime = 0n;
const startTimer = () => {
    startTime = process.hrtime.bigint();
    console.log("------------------------------------------");
    console.log("INFO [TIMER] Profiling started.");
};
const logDuration = (phaseName) => {
    if (startTime === 0n) { return; }
    const endTime = process.hrtime.bigint();
    const durationNs = endTime - startTime;
    const durationMs = Number(durationNs / 1000000n).toFixed(2);
    console.log(`INFO [TIMER] Phase: ${phaseName} Complete. Duration: ${durationMs} ms.`);
    startTime = endTime;
};

// --- S3およびMermaidヘルパー関数 (そのまま) ---

/**
 * ストリームの内容をメモリバッファに収集するヘルパー関数
 * @param {import('stream').Readable} stream - S3から取得したReadableStream
 * @returns {Promise<Buffer>} - 収集されたバッファ
 */
const streamToBuffer = (stream) => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
};

/**
 * MarkdownコンテンツからMermaid定義ブロックを抽出するヘルパー関数
 * @param {string} markdownContent - Markdownテキスト
 * @returns {string | null} - 抽出されたMermaid定義、またはnull
 */
const extractMermaidDefinition = (markdownContent) => {
    const match = markdownContent.match(/```mermaid\s*([\s\S]*?)\s*```/i);
    if (!match || match.length < 2) {
        return null;
    }
    return match[1].trim();
};

/**
 * S3から指定されたキーのオブジェクトを取得し、Mermaid定義を抽出します。
 * @param {string} bucket - S3バケット名
 * @param {string} key - S3キー
 * @returns {Promise<string | null>} - 抽出されたMermaid定義、またはnull
 */
const getMermaidByS3Key = async (bucket, key) => {
    console.log(`Downloading s3://${bucket}/${key}`);
    const s3InputParams = { Bucket: bucket, Key: key };
    
    try {
        const s3Object = await s3.send(new GetObjectCommand(s3InputParams));
        if (!s3Object.Body) {
            console.warn(`S3 object body is empty for key ${key}.`);
            return null;
        }
        const buffer = await streamToBuffer(/** @type {import('stream').Readable} */ (s3Object.Body));
        const markdownContent = buffer.toString('utf8');
        const mermaid = extractMermaidDefinition(markdownContent);
        if (!mermaid) {
            console.warn(`Mermaid definition block not found in S3 object key ${key}.`);
            return null; 
        }
        return mermaid;
    } catch (error) {
        if (error.Code === 'NoSuchKey' || error.name === 'NoSuchKey') {
            console.warn(`S3 object not found for key ${key}. Skipping.`);
            return null;
        }
        console.error(`Error fetching S3 object key ${key}:`, error);
        return null;
    }
};

/**
 * Puppeteerを使用してMermaid定義をPNGとしてレンダリングし、ファイルシステムに保存します。
 * @param {import('puppeteer-core').Browser} browser - Puppeteer Browserインスタンス
 * @param {string} mermaidDefinition - レンダリングするMermaid定義
 * @param {string} outputPath - 出力PNGファイルのパス
 * @returns {Promise<void>}
 */
const renderMermaidToPng = async (browser, mermaidDefinition, outputPath) => {
    let page;
    
    try {
        // 新しいページを開く
        page = await browser.newPage();
        
        // レンダリング用のHTMLコンテンツ
        const htmlContent = `
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <!-- 💡 ローカルパスからMermaidを読み込む (高速かつ安定) -->
                <script src="${MERMAID_LOCAL_PATH}"></script> 
                <style>
                    body { margin: 0; padding: 20px; background-color: white; }
                    #diagram { min-width: 500px; min-height: 300px; }
                    /* 日本語フォントサポートを強化 */
                    .mermaid { font-family: "Noto Sans", "Noto Sans CJK JP", sans-serif; } 
                </style>
            </head>
            <body>
                <div id="diagram" class="mermaid">
                    ${mermaidDefinition}
                </div>
                <script>
                    const renderStartInBrowser = Date.now();
                    
                    const mermaidConfig = JSON.stringify({
                        startOnLoad: false,
                        theme: "neutral",
                        flowchart: {
                            diagramPadding: 10
                        },
                        maxTextSize: 50000 
                    });
                    
                    mermaid.initialize(JSON.parse(mermaidConfig));
                    
                    async function renderAndWait() {
                        const element = document.getElementById('diagram');
                        if (!element) {
                            // @ts-ignore
                            if (typeof window.renderComplete === 'function') {
                                // @ts-ignore
                                window.renderComplete(false, 'Diagram element not found.');
                            }
                            return;
                        }
                        
                        try {
                            const renderId = 'mermaid-render-' + Date.now(); 
                            // mermaid.render APIを使用してSVGを生成し、要素に挿入
                            const { svg, bindFunctions } = await mermaid.render(renderId, element.textContent.trim());
                            element.innerHTML = svg;
                            if (bindFunctions) bindFunctions(element);
                            
                            // 💡 描画完了後、要素のサイズを取得してNode.js側に渡す
                            const svgElement = element.querySelector('svg');
                            const boundingBox = svgElement ? svgElement.getBoundingClientRect() : { width: 1200, height: 800 };

                            // レンダリング成功と、ブラウザ内での描画時間をNode.js側に通知
                            // @ts-ignore
                            if (typeof window.renderComplete === 'function') {
                                // @ts-ignore
                                window.renderComplete(true, null, boundingBox.width, boundingBox.height, Date.now() - renderStartInBrowser); 
                            }
                        } catch(e) {
                            console.error('Mermaid rendering error:', e);
                            // レンダリング失敗をNode.js側に通知
                            // @ts-ignore
                            if (typeof window.renderComplete === 'function') {
                                // @ts-ignore
                                window.renderComplete(false, 'Mermaid rendering failed: ' + e.message);
                            }
                        }
                    }
                    
                    // @ts-ignore
                    window.renderAndWait = renderAndWait; 
                </script>
            </body>
            </html>
        `;

        // 1. レンダリング用HTMLを一時ファイルに書き出す
        await writeFile(TMP_HTML_PATH, htmlContent, 'utf8');
        logDuration("HTML Write");
    
        // 2. Puppeteerのページ設定 (初期値は大きめに設定)
        await page.setViewport({ width: 1600, height: 1200 }); 
        
        // 3. Node.jsとブラウザ間の通信を設定 (レンダリング完了を通知)
        let renderPromiseResolve;
        const renderPromise = new Promise((resolve) => {
            renderPromiseResolve = resolve;
        });

        let diagramWidth = 1200;
        let diagramHeight = 800;

        // 💡 ブラウザ側に`renderComplete`関数を公開
        await page.exposeFunction('renderComplete', (success, errorMessage, width = 1200, height = 800) => {
            if (success) {
                // パディング分を追加
                diagramWidth = Math.ceil(width) + 40; 
                diagramHeight = Math.ceil(height) + 40; 
            }
            renderPromiseResolve({ success, errorMessage });
        });
        
        // 4. ローカルHTMLファイルを開く
        await page.goto(`file://${TMP_HTML_PATH}`, { 
            waitUntil: 'networkidle0', 
            timeout: RENDER_TIMEOUT_MS 
        });
        logDuration("Page Load");
    
        // 5. ブラウザ内のレンダリング関数を明示的に呼び出し、レンダリングを開始
        await page.evaluate(() => {
            // @ts-ignore
            if (typeof window.renderAndWait === 'function') {
                // @ts-ignore
                window.renderAndWait(); // ここでレンダリングを開始
            } else {
                throw new Error("window.renderAndWait function was not available in the page's execution context.");
            }
        });

        // 6. レンダリング完了を待機
        const result = await Promise.race([
            renderPromise,
            new Promise((_, reject) => setTimeout(() => reject(new Error('Mermaid rendering timeout: The diagram complexity exceeded the maximum allowed browser computation time.')), RENDER_TIMEOUT_MS))
        ]);

        if (!result.success) {
            throw new Error(`Mermaid rendering failed in Chromium: ${result.errorMessage || 'Unknown rendering error.'}`);
        }
        logDuration("Mermaid Render Wait");
        
        // 7. レンダリングされた要素のサイズに合わせてビューポートを再設定
        await page.setViewport({ width: diagramWidth, height: diagramHeight });

        // 8. レンダリングされた要素をPNGとして保存
        const diagramElement = await page.$('#diagram');
        if (!diagramElement) {
            throw new Error('Mermaid diagram element (#diagram) not found on the page after rendering.');
        }
        
        await page.screenshot({ 
            path: outputPath,
            type: 'png',
            clip: { x: 0, y: 0, width: diagramWidth, height: diagramHeight }, 
            omitBackground: true,
        });
        console.log(`PNG screenshot saved to ${outputPath}`);
        logDuration("Screenshot and Save");

    } finally {
        // 9. リソースのクリーンアップ
        if (page && !page.isClosed()) {
            await page.close();
        }
        // 一時HTMLファイルのクリーンアップ
        try { if (existsSync(TMP_HTML_PATH)) await unlink(TMP_HTML_PATH); } catch(e) { /* ignore */ }
    }
};

/**
 * 指定されたパスのファイルをS3にアップロードします。
 * @param {string} bucket - S3バケット名
 * @param {string} key - S3キー
 * @param {string} filePath - アップロードするローカルファイルのパス
 * @param {string} contentType - ファイルのContent-Type
 * @returns {Promise<string>} アップロードされたS3 URI
 */
const uploadToS3 = async (bucket, key, filePath, contentType) => {
    console.log(`Uploading output file: s3://${bucket}/${key}`);
    const fileBuffer = fs.readFileSync(filePath);
    const s3OutputParams = {
        Bucket: bucket,
        Key: key,
        Body: fileBuffer,
        ContentType: contentType,
    };
    await s3.send(new PutObjectCommand(s3OutputParams));
    
    // 一時ファイルの削除 (成功時のみ行う)
    try { if (existsSync(filePath)) await unlink(filePath); } catch(e) { console.warn(`Failed to clean up temporary file ${filePath}: ${e.message}`); }
    
    return `s3://${bucket}/${key}`;
};


// --- Agent Input/Output 型定義 ---
/**
 * @typedef {object} AgentInput
 * @property {string} inputBucket - 入力Markdownファイルが格納されているS3バケット
 * @property {string} inputKey - フルダイアグラムMarkdownのS3キー
 * @property {string | undefined} diffInputKey - 差分ダイアグラムMarkdownのS3キー (オプション)
 * @property {string} outputBucket - 出力PNGファイルが格納されるS3バケット
 * @property {string} outputKey - フルダイアグラムPNGのS3キー
 * @property {string | undefined} diffOutputKey - 差分ダイアグラムPNGのS3キー (オプション)
 * @typedef {object} BedrockAgentEvent
 * @property {string} [actionGroup] 
 * @property {string} [apiPath] 
 * @property {string} [httpMethod] 
 * @property {object} requestBody
 * @property {object} requestBody.content
 * @property {object} requestBody.content.application/json
 * @property {{ name: string, type: string, value: string }[]} requestBody.content.application/json.properties
 * @param {BedrockAgentEvent} event - Bedrock Agentからのリクエスト
 * @param {import('aws-lambda').Context} context 
 */
exports.handler = async (event, context) => { 
    
    // 💡 処理開始時刻の記録
    startTimer();

    // Layerからのモジュールロードを試みます。
    try {
        // 💡 Module Not Found エラー回避のため、Layer内の絶対パスを明示的に使用
        // @ts-ignore
        chromium = require('/opt/nodejs/node_modules/@sparticuz/chromium'); 
        // @ts-ignore
        puppeteer = require('/opt/nodejs/node_modules/puppeteer-core');
    } catch (e) {
        console.error("❌ CRITICAL: Failed to load puppeteer or chromium module from Layer. Error: Cannot find module.", e);
        // このエラーは Bedrock Agent のレスポンス形式で返す必要がある
        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup': event.actionGroup || "making-rendering",
                'apiPath': event.apiPath || "/render-diagram",
                'httpMethod': event.httpMethod || "POST",
                'functionResponse': {
                    'responseState': 'FAILURE',
                    'responseBody': {
                        'application/json': { 
                            'body': JSON.stringify({ 
                                status_message: `Rendering initialization failed: Cannot find module. Ensure the custom Layer is correctly attached. Details: ${e.message}`
                            }) 
                        }
                    }
                }
            }
        };
    }

    let browser = null;
    let page = null;
    let success = false;
    let fullOutputUrl = null; 
    let diffOutputUrl = null; 
    
    /** @type {AgentInput} */
    let agentInput = {};

    const agentMetadata = {
        actionGroup: event.actionGroup || "making-rendering", 
        apiPath: event.apiPath || "/render-diagram",
        httpMethod: event.httpMethod || "POST",
    };
    
    // 💡 Bedrock Agentが期待する厳密なJSON応答構造を生成します。（ユーザー提供のコードから流用）
    /**
     * @param {string} responseState - 'SUCCESS' or 'FAILURE'
     * @param {string} bodyMessage - ユーザー向けのメッセージ
     * @param {string} [fullOutputUrl] - 成功時のメインのS3 URI (フルダイアグラム)
     */
    const buildAgentResponse = (responseState, bodyMessage, fullOutputUrl = undefined) => {
        const bodyPayload = { 
            status_message: bodyMessage,
        }; 
        if (fullOutputUrl) {
            bodyPayload.s3_output_uri = fullOutputUrl; 
        }
        if (diffOutputUrl) { // diff URLが存在する場合も追加
            bodyPayload.s3_diff_output_uri = diffOutputUrl;
        }

        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup': agentMetadata.actionGroup,
                'apiPath': agentMetadata.apiPath,
                'httpMethod': agentMetadata.httpMethod,
                'functionResponse': {
                    'responseState': responseState,
                    'responseBody': {
                        'application/json': { 
                            // 🚨 Bedrock Agentの仕様: bodyキーの値はJSON文字列でなければならない
                            'body': JSON.stringify(bodyPayload) 
                        }
                    }
                }
            }
        };
    };

    try {
        // --- 1. Bedrock Agentからの入力をパース ---
        const agentProperties = event.requestBody?.content?.['application/json']?.properties;

        if (!Array.isArray(agentProperties)) {
            throw new Error('Invalid or missing properties array from Bedrock Agent. Cannot extract parameters.');
        }

        agentInput = agentProperties.reduce((acc, prop) => {
            acc[prop.name] = prop.value;
            return acc;
        }, /** @type {AgentInput} */ ({}));
        
        console.log('Parsed Agent Input (Flat):', agentInput);
        
        const { 
            inputBucket, 
            inputKey, 
            diffInputKey, 
            outputBucket, 
            outputKey, 
            diffOutputKey 
        } = agentInput;

        if (!inputBucket || !inputKey || !outputBucket || !outputKey) {
            const missingKeys = [
                !inputBucket && 'inputBucket', 
                !inputKey && 'inputKey', 
                !outputBucket && 'outputBucket', 
                !outputKey && 'outputKey'
            ].filter(Boolean);
            throw new Error(`Missing required S3 keys for full diagram rendering: ${missingKeys.join(', ')}.`);
        }
        
        // 差分レンダリングの有無を決定
        const diffKeyIsSameAsFullKey = diffInputKey === inputKey;
        const shouldRenderDiff = !!(diffInputKey && diffOutputKey && !diffKeyIsSameAsFullKey);
        
        if (shouldRenderDiff) {
            console.log('Diff rendering is ENABLED.');
        } else if (diffKeyIsSameAsFullKey) {
            console.warn('Diff rendering is DISABLED: diffInputKey is identical to inputKey. Skipping diff.');
        } else {
            console.log('Diff rendering is DISABLED (Missing diffInputKey or diffOutputKey).');
        }

        // --- 2. Puppeteer (Chromium) を起動 ---
        const executablePath = await chromium.executablePath();
        console.log('Launching Chromium...');
        const browserArgs = [
            ...chromium.args,
            '--disable-gpu',
            '--single-process', // メモリ効率向上
        ];
        
        browser = await puppeteer.launch({
            args: browserArgs,
            defaultViewport: chromium.defaultViewport,
            executablePath: executablePath, 
            headless: chromium.headless,
            ignoreHTTPSErrors: true,
        });
        
        // --- 3. フルダイアグラムのレンダリングとアップロード ---
        
        // 3a. フルMermaid定義を取得
        const fullMermaidDefinition = await getMermaidByS3Key(inputBucket, inputKey);
        if (!fullMermaidDefinition) {
            // フル図のコンテンツがない場合はエラー
            throw new Error(`Mermaid definition block not found or file not accessible in full key: s3://${inputBucket}/${inputKey}`);
        }
        
        // 3b. レンダリングとアップロード
        await renderMermaidToPng(browser, fullMermaidDefinition, TMP_FULL_OUTPUT_PATH);
        fullOutputUrl = await uploadToS3(outputBucket, outputKey, TMP_FULL_OUTPUT_PATH, 'image/png');
        console.log('Full diagram rendering and upload successful.');


        // --- 4. 差分ダイアグラムのレンダリングとアップロード (オプション) ---
        if (shouldRenderDiff) {
            console.log('Starting Diff Diagram Rendering...');
            
            const diffMermaidDefinition = await getMermaidByS3Key(inputBucket, diffInputKey);
            
            if (diffMermaidDefinition) {
                // レンダリングとアップロード
                await renderMermaidToPng(browser, diffMermaidDefinition, TMP_DIFF_OUTPUT_PATH);
                diffOutputUrl = await uploadToS3(outputBucket, diffOutputKey, TMP_DIFF_OUTPUT_PATH, 'image/png');
                console.log(`Diff diagram rendering and upload successful. S3 URI: ${diffOutputUrl}`);
            } else {
                // 差分キーは渡されたが、ファイル自体がない、または内容が空
                console.log('⚠️ Warning: Diff Mermaid content was empty or missing. Skipping diff PNG upload.');
            }
        }
        
        success = true; // ここまで到達すれば成功

    } catch (error) {
        console.error('Mermaid rendering or S3 upload failed:', error);
        
        // エラー時も Bedrock Agent 形式で応答
        return buildAgentResponse(
            'FAILURE', 
            `Mermaid rendering or S3 upload failed. Details: ${error.message}`,
            fullOutputUrl // 途中でフル図のアップロードに成功している可能性もあるため渡す
        );

    } finally {
        // --- 5. リソースのクリーンアップ ---
        if (browser) {
            try {
                await browser.close();
                console.log('Chromium browser closed.');
            } catch (e) {
                console.warn('Browser close failed:', e);
            }
        }
    }
    
    // 成功した場合のみここで結果を返す
    if (success) {
        let message = `TGWルーティングダイアグラム (フルPNG) の生成とアップロードに成功しました。成果物 S3 URI: ${fullOutputUrl}`;
        
        if (diffOutputUrl) {
            message += ` 差分ダイアグラム (PNG) も生成され、S3にアップロードされました: ${diffOutputUrl}`;
        } else {
            message += ' 差分ダイアグラムのレンダリングはスキップされました。';
        }
        
        return buildAgentResponse(
            'SUCCESS', 
            message,
            fullOutputUrl
        );
    }
    // ここに到達することは通常はないが、念のため
    return buildAgentResponse('FAILURE', 'An unknown error occurred during the final processing stage.');
};