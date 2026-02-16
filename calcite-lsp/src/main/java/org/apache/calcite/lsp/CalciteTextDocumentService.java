/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.lsp;

import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionList;
import org.eclipse.lsp4j.CompletionParams;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidCloseTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.DidSaveTextDocumentParams;
import org.eclipse.lsp4j.DocumentFormattingParams;
import org.eclipse.lsp4j.DocumentHighlight;
import org.eclipse.lsp4j.DocumentHighlightParams;
import org.eclipse.lsp4j.DocumentRangeFormattingParams;
import org.eclipse.lsp4j.DocumentSymbolParams;
import org.eclipse.lsp4j.FoldingRange;
import org.eclipse.lsp4j.FoldingRangeRequestParams;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.HoverParams;
import org.eclipse.lsp4j.PublishDiagnosticsParams;
import org.eclipse.lsp4j.RenameParams;
import org.eclipse.lsp4j.TextDocumentContentChangeEvent;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.VersionedTextDocumentIdentifier;
import org.eclipse.lsp4j.WorkspaceEdit;
import org.eclipse.lsp4j.services.TextDocumentService;
import org.eclipse.lsp4j.jsonrpc.messages.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** Text document methods for Calcite SQL files. */
class CalciteTextDocumentService implements TextDocumentService {
  private final CalciteLanguageServer server;
  private final Map<String, String> documents = new ConcurrentHashMap<>();

  CalciteTextDocumentService(CalciteLanguageServer server) {
    this.server = server;
  }

  @Override public void didOpen(DidOpenTextDocumentParams params) {
    final String uri = params.getTextDocument().getUri();
    documents.put(uri, params.getTextDocument().getText());
    publishDiagnostics(uri, params.getTextDocument().getText());
  }

  @Override public void didChange(DidChangeTextDocumentParams params) {
    final VersionedTextDocumentIdentifier doc = params.getTextDocument();
    final List<TextDocumentContentChangeEvent> changes = params.getContentChanges();
    if (changes.isEmpty()) {
      return;
    }
    final String uri = doc.getUri();
    // Full text sync is enabled.
    final String text = changes.get(changes.size() - 1).getText();
    documents.put(uri, text);
    publishDiagnostics(uri, text);
  }

  @Override public void didSave(DidSaveTextDocumentParams params) {
    final String uri = params.getTextDocument().getUri();
    final String text = textForUri(uri);
    publishDiagnostics(uri, text);
  }

  @Override public void didClose(DidCloseTextDocumentParams params) {
    final String uri = params.getTextDocument().getUri();
    documents.remove(uri);
    server.client().publishDiagnostics(new PublishDiagnosticsParams(uri, Collections.emptyList()));
  }

  @Override public CompletableFuture<Either<List<CompletionItem>, CompletionList>> completion(
      CompletionParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final String uri = params.getTextDocument().getUri();
      final String text = textForUri(uri);
      final int offset = TextUtil.toOffset(text, params.getPosition());
      final List<CompletionItem> items = server.engine().completions(text, offset);
      return Either.forLeft(items);
    }, server.executor());
  }

  @Override public CompletableFuture<Hover> hover(HoverParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final String uri = params.getTextDocument().getUri();
      final String text = textForUri(uri);
      final int offset = TextUtil.toOffset(text, params.getPosition());
      return server.engine().hover(text, offset);
    }, server.executor());
  }

  @Override public CompletableFuture<List<? extends TextEdit>> formatting(
      DocumentFormattingParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final String uri = params.getTextDocument().getUri();
      final String text = textForUri(uri);
      try {
        final String formatted = server.engine().format(text);
        final List<TextEdit> edits = new ArrayList<>();
        edits.add(new TextEdit(TextUtil.fullRange(text), formatted));
        return edits;
      } catch (Exception e) {
        return Collections.emptyList();
      }
    }, server.executor());
  }

  @Override public CompletableFuture<List<? extends TextEdit>> rangeFormatting(
      DocumentRangeFormattingParams params) {
    return formatting(new DocumentFormattingParams(
        params.getTextDocument(), params.getOptions()));
  }

  @Override
  public CompletableFuture<List<Either<org.eclipse.lsp4j.SymbolInformation, org.eclipse.lsp4j.DocumentSymbol>>>
      documentSymbol(DocumentSymbolParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final String uri = params.getTextDocument().getUri();
      final String text = textForUri(uri);
      return castDocumentSymbols(server.engine().documentSymbols(text));
    }, server.executor());
  }

  @Override public CompletableFuture<List<FoldingRange>> foldingRange(
      FoldingRangeRequestParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final String uri = params.getTextDocument().getUri();
      final String text = textForUri(uri);
      return server.engine().foldingRanges(text);
    }, server.executor());
  }

  @Override public CompletableFuture<List<? extends DocumentHighlight>> documentHighlight(
      DocumentHighlightParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final String uri = params.getTextDocument().getUri();
      final String text = textForUri(uri);
      final int offset = TextUtil.toOffset(text, params.getPosition());
      return server.engine().documentHighlights(text, offset);
    }, server.executor());
  }

  @Override public CompletableFuture<WorkspaceEdit> rename(RenameParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final String uri = params.getTextDocument().getUri();
      final String text = textForUri(uri);
      final int offset = TextUtil.toOffset(text, params.getPosition());
      final List<TextEdit> edits = server.engine().renameEdits(text, offset, params.getNewName());
      final WorkspaceEdit edit = new WorkspaceEdit();
      edit.setChanges(Collections.singletonMap(uri, edits));
      return edit;
    }, server.executor());
  }

  private void publishDiagnostics(String uri, String text) {
    CompletableFuture.runAsync(() -> {
      final SqlAnalysisResult result = server.engine().analyze(text);
      server.client().publishDiagnostics(new PublishDiagnosticsParams(uri, result.diagnostics));
    }, server.executor());
  }

  private String textForUri(String uri) {
    return documents.getOrDefault(uri, "");
  }

  private static List<Either<org.eclipse.lsp4j.SymbolInformation, org.eclipse.lsp4j.DocumentSymbol>>
      castDocumentSymbols(List<Either<SqlAnalysisEngine.SymbolInformationLike,
          org.eclipse.lsp4j.DocumentSymbol>> input) {
    final List<Either<org.eclipse.lsp4j.SymbolInformation, org.eclipse.lsp4j.DocumentSymbol>>
        out = new ArrayList<>();
    for (Either<SqlAnalysisEngine.SymbolInformationLike,
        org.eclipse.lsp4j.DocumentSymbol> value : input) {
      if (value.isRight()) {
        out.add(Either.forRight(value.getRight()));
      }
    }
    return out;
  }
}
