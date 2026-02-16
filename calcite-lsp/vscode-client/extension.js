const path = require("path");
const vscode = require("vscode");
const languageclient = require("vscode-languageclient/node");

let client;

function activate(context) {
  const config = vscode.workspace.getConfiguration("calciteSqlLsp");
  const command = config.get("server.command") || "";
  const args = config.get("server.args") || [];
  const initializationOptions = config.get("server.initializationOptions") || {};

  if (!command) {
    vscode.window.showErrorMessage(
      "calciteSqlLsp.server.command is not set. Point it to the installDist server script."
    );
    return;
  }

  const serverOptions = {
    command: path.resolve(command),
    args,
  };

  const clientOptions = {
    documentSelector: [{ scheme: "file", language: "sql" }],
    initializationOptions,
  };

  client = new languageclient.LanguageClient(
    "calciteSqlLsp",
    "Calcite SQL Language Server",
    serverOptions,
    clientOptions
  );

  context.subscriptions.push(client.start());
}

function deactivate() {
  if (!client) {
    return undefined;
  }
  return client.stop();
}

module.exports = {
  activate,
  deactivate,
};
