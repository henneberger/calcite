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

import org.eclipse.lsp4j.CompletionOptions;
import org.eclipse.lsp4j.InitializeParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.RenameOptions;
import org.eclipse.lsp4j.ServerCapabilities;
import org.eclipse.lsp4j.TextDocumentSyncKind;
import org.eclipse.lsp4j.services.LanguageClient;
import org.eclipse.lsp4j.services.LanguageClientAware;
import org.eclipse.lsp4j.services.LanguageServer;
import org.eclipse.lsp4j.services.TextDocumentService;
import org.eclipse.lsp4j.services.WorkspaceService;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Main LSP server object. */
class CalciteLanguageServer implements LanguageServer, LanguageClientAware {
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final CalciteWorkspaceService workspaceService = new CalciteWorkspaceService();
  private final CalciteTextDocumentService textDocumentService =
      new CalciteTextDocumentService(this);

  private LanguageClient client;
  private volatile int shutdownCode;
  private volatile SqlAnalysisEngine engine;

  @Override public void connect(LanguageClient client) {
    this.client = client;
  }

  @Override public CompletableFuture<InitializeResult> initialize(InitializeParams params) {
    return CompletableFuture.supplyAsync(() -> {
      final CalciteConfig config = CalciteConfigParser.fromObject(params.getInitializationOptions());
      try {
        this.engine = new SqlAnalysisEngine(config);
      } catch (SQLException e) {
        throw new RuntimeException("Could not initialize Calcite SQL engine: " + e.getMessage(), e);
      }

      final ServerCapabilities capabilities = new ServerCapabilities();
      capabilities.setTextDocumentSync(TextDocumentSyncKind.Full);

      final CompletionOptions completionOptions = new CompletionOptions();
      completionOptions.setResolveProvider(false);
      completionOptions.setTriggerCharacters(Arrays.asList(".", "\"", "["));
      capabilities.setCompletionProvider(completionOptions);
      capabilities.setHoverProvider(EitherUtil.left(true));
      capabilities.setDocumentFormattingProvider(EitherUtil.left(true));
      capabilities.setDocumentRangeFormattingProvider(EitherUtil.left(true));
      capabilities.setDocumentSymbolProvider(EitherUtil.left(true));
      capabilities.setDocumentHighlightProvider(EitherUtil.left(true));
      capabilities.setRenameProvider(new RenameOptions(true));
      capabilities.setFoldingRangeProvider(EitherUtil.left(true));

      return new InitializeResult(capabilities);
    }, executor);
  }

  @Override public CompletableFuture<Object> shutdown() {
    shutdownCode = 0;
    return CompletableFuture.completedFuture(new Object());
  }

  @Override public void exit() {
    shutdownCode = shutdownCode == 0 ? 0 : 1;
    try {
      if (engine != null) {
        engine.close();
      }
    } catch (SQLException ignored) {
      // Ignore on shutdown.
    } finally {
      executor.shutdownNow();
    }
  }

  @Override public TextDocumentService getTextDocumentService() {
    return textDocumentService;
  }

  @Override public WorkspaceService getWorkspaceService() {
    return workspaceService;
  }

  LanguageClient client() {
    return client;
  }

  SqlAnalysisEngine engine() {
    return engine;
  }

  ExecutorService executor() {
    return executor;
  }

  int shutdownCode() {
    return shutdownCode;
  }
}
