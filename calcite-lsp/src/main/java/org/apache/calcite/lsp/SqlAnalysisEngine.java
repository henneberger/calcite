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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionItemKind;
import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.DiagnosticSeverity;
import org.eclipse.lsp4j.DocumentHighlight;
import org.eclipse.lsp4j.DocumentHighlightKind;
import org.eclipse.lsp4j.DocumentSymbol;
import org.eclipse.lsp4j.FoldingRange;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.MarkupContent;
import org.eclipse.lsp4j.MarkupKind;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.SymbolKind;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.jsonrpc.messages.Either;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/** Calcite-backed SQL parsing, validation, completion and formatting. */
class SqlAnalysisEngine implements AutoCloseable {
  private static final Set<SqlKind> QUERY_KINDS = EnumSet.of(
      SqlKind.SELECT,
      SqlKind.UNION,
      SqlKind.INTERSECT,
      SqlKind.EXCEPT,
      SqlKind.ORDER_BY,
      SqlKind.WITH,
      SqlKind.VALUES);

  private final Connection connection;
  private final CalciteConnection calciteConnection;
  private final JavaTypeFactory typeFactory;
  private final CalciteSchema rootSchema;
  private final SqlParser.Config parserConfig;
  private final SqlOperatorTable operatorTable;
  private final List<String> schemaPath;
  private final SqlAdvisor advisor;

  SqlAnalysisEngine(CalciteConfig config) throws SQLException {
    this.connection =
        DriverManager.getConnection("jdbc:calcite:", config.toConnectionProperties());
    this.calciteConnection = connection.unwrap(CalciteConnection.class);
    this.typeFactory = new JavaTypeFactoryImpl();
    this.rootSchema = CalciteSchema.from(
        requireNonNull(calciteConnection.getRootSchema(), "root schema"));
    this.operatorTable = SqlStdOperatorTable.instance();
    this.schemaPath = defaultSchemaPath(calciteConnection);

    this.parserConfig = SqlParser.config()
        .withQuotedCasing(config.quotedCasing)
        .withUnquotedCasing(config.unquotedCasing)
        .withQuoting(config.quoting)
        .withConformance(config.conformance)
        .withCaseSensitive(config.caseSensitive);

    final SqlValidatorWithHints validator = createValidator();
    this.advisor = new SqlAdvisor(validator, parserConfig);
  }

  SqlAnalysisResult analyze(String sql) {
    final SqlAnalysisResult result = new SqlAnalysisResult();
    final SqlNodeList stmtList;
    try {
      stmtList = parse(sql);
    } catch (SqlParseException parseException) {
      result.diagnostics.add(parseDiagnostic(parseException, sql));
      return result;
    }

    final SqlValidator validator = createValidator();
    for (SqlNode statement : stmtList.getList()) {
      result.statements.add(statement);
      try {
        validator.validate(statement);
      } catch (Exception validationError) {
        final Diagnostic diagnostic = toDiagnostic(validationError, sql);
        if (diagnostic != null) {
          result.diagnostics.add(diagnostic);
        }
      }
    }
    return result;
  }

  List<CompletionItem> completions(String sql, int offset) {
    final String[] replaced = new String[1];
    final List<SqlMoniker> hints = advisor.getCompletionHints(sql, offset, replaced);
    final String typed = replaced[0] == null ? "" : replaced[0];
    final List<CompletionItem> items = new ArrayList<>();
    for (SqlMoniker hint : hints) {
      final CompletionItem item = new CompletionItem();
      final List<String> names = hint.getFullyQualifiedNames();
      final String label = hint.id();
      final String replacement = advisor.getReplacement(hint, typed);
      item.setLabel(label);
      item.setDetail(hint.getType().name());
      item.setKind(mapCompletionKind(hint.getType()));
      item.setInsertText(replacement == null || replacement.isEmpty() ? label : replacement);
      item.setSortText(sortPrefix(hint.getType()) + label.toUpperCase(Locale.ROOT));
      item.setDocumentation(String.join(".", names));
      items.add(item);
    }
    return items;
  }

  Hover hover(String sql, int offset) {
    final SqlMoniker qualified = advisor.getQualifiedName(sql, offset);
    if (qualified == null) {
      return null;
    }
    final MarkupContent content = new MarkupContent();
    content.setKind(MarkupKind.MARKDOWN);
    content.setValue("`" + String.join(".", qualified.getFullyQualifiedNames())
        + "`\n\nType: `" + qualified.getType().name() + "`");
    final Hover hover = new Hover();
    hover.setContents(Either.forRight(content));
    return hover;
  }

  String format(String sql) throws SqlParseException {
    final SqlNodeList statementList = parse(sql);
    final SqlPrettyWriter writer = new SqlPrettyWriter();
    final List<String> formatted = new ArrayList<>();
    for (SqlNode statement : statementList) {
      formatted.add(writer.format(statement));
    }
    return formatted.stream().collect(Collectors.joining(";\n\n")) + "\n";
  }

  List<Either<SymbolInformationLike, DocumentSymbol>> documentSymbols(String sql) {
    final List<Either<SymbolInformationLike, DocumentSymbol>> symbols = new ArrayList<>();
    try {
      final SqlNodeList stmtList = parse(sql);
      int index = 1;
      for (SqlNode statement : stmtList) {
        final DocumentSymbol symbol = new DocumentSymbol();
        symbol.setName(statementName(statement, index));
        symbol.setKind(statementSymbolKind(statement));
        final SqlParserPos pos = statement.getParserPosition();
        final Range range = TextUtil.fromParserCoordinates(
            sql, pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum());
        symbol.setRange(range);
        symbol.setSelectionRange(range);
        symbols.add(Either.forRight(symbol));
        index++;
      }
    } catch (SqlParseException ignored) {
      // No symbols for unparseable source.
    }
    return symbols;
  }

  List<FoldingRange> foldingRanges(String sql) {
    try {
      final SqlNodeList stmtList = parse(sql);
      final List<FoldingRange> folds = new ArrayList<>();
      for (SqlNode statement : stmtList) {
        final SqlParserPos pos = statement.getParserPosition();
        if (pos.getEndLineNum() > pos.getLineNum()) {
          final FoldingRange range = new FoldingRange();
          range.setStartLine(pos.getLineNum() - 1);
          range.setEndLine(pos.getEndLineNum() - 1);
          folds.add(range);
        }
      }
      return folds;
    } catch (SqlParseException ignored) {
      return Collections.emptyList();
    }
  }

  List<DocumentHighlight> documentHighlights(String sql, int offset) {
    final Range target = TextUtil.wordRangeAt(sql, offset);
    final int startOffset = TextUtil.toOffset(sql, target.getStart());
    final int endOffset = TextUtil.toOffset(sql, target.getEnd());
    if (startOffset >= endOffset) {
      return Collections.emptyList();
    }
    final String token = sql.substring(startOffset, endOffset);
    if (token.isEmpty()) {
      return Collections.emptyList();
    }
    final List<DocumentHighlight> highlights = new ArrayList<>();
    int fromIndex = 0;
    while (fromIndex < sql.length()) {
      final int found = indexOfIdentifier(sql, token, fromIndex);
      if (found < 0) {
        break;
      }
      final DocumentHighlight highlight = new DocumentHighlight();
      highlight.setKind(DocumentHighlightKind.Text);
      highlight.setRange(new Range(
          TextUtil.toPosition(sql, found),
          TextUtil.toPosition(sql, found + token.length())));
      highlights.add(highlight);
      fromIndex = found + token.length();
    }
    return highlights;
  }

  List<TextEdit> renameEdits(String sql, int offset, String newName) {
    final Range target = TextUtil.wordRangeAt(sql, offset);
    final int startOffset = TextUtil.toOffset(sql, target.getStart());
    final int endOffset = TextUtil.toOffset(sql, target.getEnd());
    if (startOffset >= endOffset) {
      return Collections.emptyList();
    }
    final String token = sql.substring(startOffset, endOffset);
    if (token.isEmpty()) {
      return Collections.emptyList();
    }
    final List<TextEdit> edits = new ArrayList<>();
    int fromIndex = 0;
    while (fromIndex < sql.length()) {
      final int found = indexOfIdentifier(sql, token, fromIndex);
      if (found < 0) {
        break;
      }
      edits.add(new TextEdit(new Range(
          TextUtil.toPosition(sql, found),
          TextUtil.toPosition(sql, found + token.length())), newName));
      fromIndex = found + token.length();
    }
    return edits;
  }

  @Override public void close() throws SQLException {
    connection.close();
  }

  private SqlNodeList parse(String sql) throws SqlParseException {
    return SqlParser.create(sql, parserConfig).parseStmtList();
  }

  private SqlValidatorWithHints createValidator() {
    final CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, schemaPath, typeFactory, calciteConnection.config());
    return SqlValidatorUtil.newValidator(
        operatorTable,
        catalogReader,
        typeFactory,
        SqlValidator.Config.DEFAULT);
  }

  private static List<String> defaultSchemaPath(CalciteConnection connection) throws SQLException {
    final String schema = connection.getSchema();
    if (schema == null || schema.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(schema);
  }

  private static Diagnostic parseDiagnostic(SqlParseException exception, String sql) {
    final SqlParserPos pos = exception.getPos();
    final Diagnostic diagnostic = new Diagnostic();
    diagnostic.setSeverity(DiagnosticSeverity.Error);
    diagnostic.setSource("calcite-parser");
    diagnostic.setMessage(exception.getMessage());
    diagnostic.setRange(TextUtil.fromParserCoordinates(
        sql, pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()));
    return diagnostic;
  }

  private static Diagnostic toDiagnostic(Exception exception, String sql) {
    Throwable current = exception;
    while (current != null) {
      if (current instanceof CalciteContextException) {
        final CalciteContextException context = (CalciteContextException) current;
        final Diagnostic diagnostic = new Diagnostic();
        diagnostic.setSeverity(DiagnosticSeverity.Error);
        diagnostic.setSource("calcite-validator");
        diagnostic.setMessage(context.getMessage());
        diagnostic.setRange(TextUtil.fromParserCoordinates(
            sql,
            context.getPosLine(),
            context.getPosColumn(),
            context.getEndPosLine(),
            context.getEndPosColumn()));
        return diagnostic;
      }
      current = current.getCause();
    }

    final Diagnostic diagnostic = new Diagnostic();
    diagnostic.setSeverity(DiagnosticSeverity.Error);
    diagnostic.setSource("calcite-validator");
    diagnostic.setMessage(exception.getMessage() == null
        ? exception.getClass().getSimpleName()
        : exception.getMessage());
    diagnostic.setRange(new Range(new Position(0, 0), new Position(0, 1)));
    return diagnostic;
  }

  private static CompletionItemKind mapCompletionKind(SqlMonikerType type) {
    switch (type) {
    case COLUMN:
      return CompletionItemKind.Field;
    case TABLE:
      return CompletionItemKind.Class;
    case FUNCTION:
      return CompletionItemKind.Function;
    case KEYWORD:
      return CompletionItemKind.Keyword;
    case SCHEMA:
    case CATALOG:
      return CompletionItemKind.Module;
    default:
      return CompletionItemKind.Text;
    }
  }

  private static String sortPrefix(SqlMonikerType type) {
    switch (type) {
    case KEYWORD:
      return "3_";
    case FUNCTION:
      return "2_";
    case COLUMN:
    case TABLE:
      return "1_";
    default:
      return "4_";
    }
  }

  private static String statementName(SqlNode statement, int index) {
    final SqlKind kind = statement.getKind();
    return kind.sql + " #" + index;
  }

  private static SymbolKind statementSymbolKind(SqlNode statement) {
    final SqlKind kind = statement.getKind();
    if (QUERY_KINDS.contains(kind)) {
      return SymbolKind.Namespace;
    }
    return SymbolKind.Object;
  }

  private static int indexOfIdentifier(String text, String token, int fromIndex) {
    final String upperText = text.toUpperCase(Locale.ROOT);
    final String upperToken = token.toUpperCase(Locale.ROOT);
    int index = upperText.indexOf(upperToken, fromIndex);
    while (index >= 0) {
      final boolean leftOk =
          index == 0 || !TextUtil.isIdentifierPart(text.charAt(index - 1));
      final int end = index + token.length();
      final boolean rightOk =
          end >= text.length() || !TextUtil.isIdentifierPart(text.charAt(end));
      if (leftOk && rightOk) {
        return index;
      }
      index = upperText.indexOf(upperToken, index + 1);
    }
    return -1;
  }

  /** LSP4J allows either symbol type; this server returns document symbols only. */
  static final class SymbolInformationLike {
    private SymbolInformationLike() {
    }
  }
}
