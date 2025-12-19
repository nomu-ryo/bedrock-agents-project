// Lambda関数名: sf-approval-handler (承認リンクの初回クリック時に実行)

const { SFNClient } = require("@aws-sdk/client-sfn");
// DynamoDBへのアクセスやS3署名付きURLの生成は不要になります。

// 環境変数からCFドメイン名と確認画面のキーを取得
const CLOUDFRONT_DOMAIN = process.env.CLOUDFRONT_DOMAIN || 'https://<YOUR_DISTRIBUTION_ID>.cloudfront.net';
const CONFIRM_HTML_KEY = process.env.CONFIRM_HTML_KEY || 'confirm.html';


/**
 * 承認リンクの初回クリック時に実行され、二段階承認の確認画面へリダイレクトします。
 * * @param {object} event - Lambdaイベントオブジェクト (API Gatewayからのリクエスト)
 */
exports.handler = async (event) => {
    const taskToken = event.queryStringParameters?.token;
    const shortId = event.queryStringParameters?.shortid;
    
    // 必須パラメータのチェック
    if (!taskToken || !shortId) {
        console.error("TaskTokenまたはShortIdがクエリパラメータに見つかりません。");
        return {
            statusCode: 400,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message: "Error: TaskToken or ShortId is missing." }),
        };
    }

    try {
        // 1. CloudFront経由の confirm.html へのリダイレクトURLを構築
        //    TaskTokenとShortIdをクエリパラメータとして埋め込むことで、
        //    Lambda@Edgeがこれらのパラメータを使って有効性をチェックできるようにします。
        
        const encodedToken = encodeURIComponent(taskToken);
        
        // CFドメイン + HTMLのパス + クエリパラメータ
        const finalUrl = `${CLOUDFRONT_DOMAIN}/${CONFIRM_HTML_KEY}?token=${encodedToken}&shortid=${shortId}`;

        console.log(`Redirecting to confirmation page: ${finalUrl}`);

        // 2. HTTP 302 リダイレクトを返す
        return {
            statusCode: 302,
            headers: {
                'Location': finalUrl,
                'Cache-Control': 'no-cache, no-store, must-revalidate' // キャッシュ防止
            },
            body: '' // ボディは空
        };

    } catch (error) {
        console.error("リダイレクト処理に失敗:", error);
        return { 
            statusCode: 500, 
            headers: { "Content-Type": "text/html" },
            body: '<h1>エラー</h1><p>確認画面へのリダイレクトに失敗しました。</p>'
        };
    }
};