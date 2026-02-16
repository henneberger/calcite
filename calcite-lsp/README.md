# Calcite SQL Language Server

This module provides an LSP server for SQL using Apache Calcite for parsing,
validation, completion hints, hover qualified names, formatting, document
symbols, folding ranges, highlights, and rename.

## Build

```bash
./gradlew :calcite-lsp:installDist
```

Runnable script:

`/Users/henneberger/calcite/calcite-lsp/build/install/calcite-lsp/bin/calcite-lsp`

## Run

```bash
/Users/henneberger/calcite/calcite-lsp/build/install/calcite-lsp/bin/calcite-lsp
```

The server communicates over stdio.

## Initialize options

Pass these via LSP `initializationOptions`:

- `model`: path to a Calcite model JSON file (absolute path or file URI)
- `schema`: default schema name
- `lex`: Calcite lex policy (`JAVA`, `MYSQL`, `ORACLE`, `SQL_SERVER`, ...)
- `quotedCasing`
- `unquotedCasing`
- `quoting`
- `caseSensitive`
- `conformance`

If omitted, defaults are based on `Lex.JAVA`.

## VS Code registration

A minimal VS Code client extension is included in:

`/Users/henneberger/calcite/calcite-lsp/vscode-client`

See:

`/Users/henneberger/calcite/calcite-lsp/vscode-client/README.md`
