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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.Properties;

/** Runtime configuration for the Calcite SQL language server. */
class CalciteConfig {
  final String modelPath;
  final String schema;
  final Lex lex;
  final Casing quotedCasing;
  final Casing unquotedCasing;
  final Quoting quoting;
  final boolean caseSensitive;
  final SqlConformanceEnum conformance;

  CalciteConfig(
      String modelPath,
      String schema,
      Lex lex,
      Casing quotedCasing,
      Casing unquotedCasing,
      Quoting quoting,
      boolean caseSensitive,
      SqlConformanceEnum conformance) {
    this.modelPath = modelPath;
    this.schema = schema;
    this.lex = lex;
    this.quotedCasing = quotedCasing;
    this.unquotedCasing = unquotedCasing;
    this.quoting = quoting;
    this.caseSensitive = caseSensitive;
    this.conformance = conformance;
  }

  static CalciteConfig defaults() {
    final Lex lex = Lex.JAVA;
    return new CalciteConfig(
        null,
        null,
        lex,
        lex.quotedCasing,
        lex.unquotedCasing,
        lex.quoting,
        lex.caseSensitive,
        SqlConformanceEnum.DEFAULT);
  }

  Properties toConnectionProperties() {
    final Properties properties = new Properties();
    properties.setProperty("lex", lex.name());
    properties.setProperty("quotedCasing", quotedCasing.name());
    properties.setProperty("unquotedCasing", unquotedCasing.name());
    properties.setProperty("quoting", quoting.name());
    properties.setProperty("caseSensitive", Boolean.toString(caseSensitive));
    properties.setProperty("conformance", conformance.name());
    if (modelPath != null && !modelPath.isEmpty()) {
      properties.setProperty("model", modelPath);
    }
    if (schema != null && !schema.isEmpty()) {
      properties.setProperty("schema", schema);
    }
    return properties;
  }
}

