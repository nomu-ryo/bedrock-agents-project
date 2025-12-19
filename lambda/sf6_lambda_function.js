// Step Functionsのタスク失敗をシグナルするためのLambda関数 (Node.js)
const { SFNClient, SendTaskFailureCommand } = require("@aws-sdk/client-sfn");
// DynamoDBクライアントとDeleteItemCommandを追加
const { DynamoDBClient, DeleteItemCommand } = require("@aws-sdk/client-dynamodb");

// クライアントの初期化
const sfnClient = new SFNClient({});
const ddbClient = new DynamoDBClient({}); // DynamoDBクライアントを追加

// 環境変数からテーブル名を取得。デフォルト値はShortenedUrlStoreとする。（必要に応じて設定）
const DYNAMODB_TABLE_NAME = process.env.DYNAMODB_TABLE_NAME || 'ShortenedUrlStore';


/**
 * Step Functionsにタスク失敗のシグナルを送り、DynamoDBの短縮リンクレコードを削除します。
 * URLクエリパラメータからTaskTokenとShortIdを取得し、ワークフローを失敗させます。
 * @param {object} event - Lambdaイベントオブジェクト (API Gatewayからのリクエスト)
 */
exports.handler = async (event) => {
    // 1. TaskTokenとShortIdの抽出
    const taskToken = event.queryStringParameters?.token;
    // 短縮リンクの削除用IDを取得
    const shortId = event.queryStringParameters?.shortid;

    if (!taskToken) {
        console.error("TaskTokenが見つかりません。");
        return {
            statusCode: 400,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message: "Error: TaskToken is missing." }),
        };
    }

    try {
        // 2. Step Functions APIの呼び出し (ワークフロー失敗シグナル)
        // 失敗時のエラー情報を提供します
        const command = new SendTaskFailureCommand({
            taskToken: taskToken,
            error: "UserRejected", // エラー名（WorkflowがCatchする際に使用）
            cause: "画像レビューがユーザーによって却下されました。", // 詳細な説明
        });

        await sfnClient.send(command);
        console.log("Step Functionsにタスク失敗をシグナルしました。");

        // 3. DynamoDBから短縮リンクレコードを削除 (クリーンアップ)
        if (shortId) {
            const deleteCommand = new DeleteItemCommand({
                TableName: DYNAMODB_TABLE_NAME,
                Key: {
                    // パーティションキー 'ShortId' を指定して削除
                    'ShortId': { S: shortId } 
                }
            });
            await ddbClient.send(deleteCommand);
            console.log(`DynamoDB ShortId ${shortId} のレコードを削除しました。`);
        } else {
            console.log("ShortIdがクエリパラメータになかったため、DynamoDBの削除はスキップされました。");
        }


        // 4. ユーザーへの応答（否認完了画面）
        const rejectHtml = `
            <!DOCTYPE html>
            <html lang="ja">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>却下完了</title>
                <style>
                    body { font-family: 'Arial', sans-serif; text-align: center; padding: 50px; background-color: #f4f4f9; }
                    .container { background-color: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); max-width: 400px; margin: 0 auto; }
                    h1 { color: #f44336; }
                    .icon { font-size: 40px; margin-bottom: 10px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="icon">❌</div>
                    <h1>却下完了</h1>
                    <p>レビューありがとうございます。却下結果をStep Functionsワークフローに送信しました。</p>
                    <p>このウィンドウは閉じていただいて構いません。</p>
                </div>
            </body>
            </html>
        `;

        return {
            statusCode: 200,
            headers: { "Content-Type": "text/html" },
            body: rejectHtml,
        };

    } catch (error) {
        console.error("Step Functionsへのシグナル送信またはDB削除に失敗しました:", error);

        const errorHtml = `
            <!DOCTYPE html>
            <html lang="ja">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>エラー</title>
                <style>
                    body { font-family: 'Arial', sans-serif; text-align: center; padding: 50px; background-color: #f4f4f9; }
                    .container { background-color: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); max-width: 400px; margin: 0 auto; }
                    h1 { color: #f44336; }
                    .icon { font-size: 40px; margin-bottom: 10px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="icon">⚠️</div>
                    <h1>エラーが発生しました</h1>
                    <p>タスクの完了処理中に問題が発生しました。既に処理が完了している可能性があります。</p>
                    <p><small>エラー詳細: ${error.name}</small></p>
                </div>
            </body>
            </html>
        `;

        return {
            statusCode: 500,
            headers: { "Content-Type": "text/html" },
            body: errorHtml,
        };
    }
};