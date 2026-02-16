# VS Code Client For Calcite SQL LSP

## 1. Build the language server jar

```bash
cd /Users/henneberger/calcite
./gradlew :calcite-lsp:installDist
```

## 2. Package and install the VS Code extension

```bash
cd /Users/henneberger/calcite/calcite-lsp/vscode-client
npm install
npx vsce package
code --install-extension calcite-sql-language-client-0.0.1.vsix
```

## 3. Configure settings in VS Code

Set these in user or workspace settings:

```json
{
  "calciteSqlLsp.server.command": "/Users/henneberger/calcite/calcite-lsp/build/install/calcite-lsp/bin/calcite-lsp",
  "calciteSqlLsp.server.args": [],
  "calciteSqlLsp.server.initializationOptions": {
    "model": "/absolute/path/to/model.json",
    "schema": "SALES",
    "lex": "JAVA"
  }
}
```

Reload VS Code after changing extension settings.
