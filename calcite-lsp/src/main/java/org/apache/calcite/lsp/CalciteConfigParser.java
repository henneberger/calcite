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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.net.URI;
import java.nio.file.Paths;

/** Parser for initialize options and workspace settings. */
final class CalciteConfigParser {
  private static final Gson GSON = new Gson();

  private CalciteConfigParser() {
  }

  static CalciteConfig fromObject(Object object) {
    if (object == null) {
      return CalciteConfig.defaults();
    }
    final JsonElement element = GSON.toJsonTree(object);
    if (!element.isJsonObject()) {
      return CalciteConfig.defaults();
    }
    final JsonObject json = element.getAsJsonObject();
    final CalciteConfig defaults = CalciteConfig.defaults();

    final Lex lex = parseEnum(json, "lex", Lex.class, defaults.lex);
    final Casing quotedCasing =
        parseEnum(json, "quotedCasing", Casing.class, lex.quotedCasing);
    final Casing unquotedCasing =
        parseEnum(json, "unquotedCasing", Casing.class, lex.unquotedCasing);
    final Quoting quoting = parseEnum(json, "quoting", Quoting.class, lex.quoting);
    final boolean caseSensitive =
        parseBoolean(json, "caseSensitive", lex.caseSensitive);
    final SqlConformanceEnum conformance = parseEnum(
        json, "conformance", SqlConformanceEnum.class, defaults.conformance);

    final String model = normalizeModelPath(parseString(json, "model", null));
    final String schema = parseString(json, "schema", null);

    return new CalciteConfig(
        model,
        schema,
        lex,
        quotedCasing,
        unquotedCasing,
        quoting,
        caseSensitive,
        conformance);
  }

  private static String parseString(JsonObject json, String key, String fallback) {
    if (!json.has(key) || json.get(key).isJsonNull()) {
      return fallback;
    }
    return json.get(key).getAsString();
  }

  private static boolean parseBoolean(JsonObject json, String key, boolean fallback) {
    if (!json.has(key) || json.get(key).isJsonNull()) {
      return fallback;
    }
    return json.get(key).getAsBoolean();
  }

  private static <E extends Enum<E>> E parseEnum(
      JsonObject json, String key, Class<E> enumClass, E fallback) {
    if (!json.has(key) || json.get(key).isJsonNull()) {
      return fallback;
    }
    final String raw = json.get(key).getAsString();
    for (E constant : enumClass.getEnumConstants()) {
      if (constant.name().equalsIgnoreCase(raw)) {
        return constant;
      }
    }
    return fallback;
  }

  private static String normalizeModelPath(String model) {
    if (model == null || model.isEmpty()) {
      return null;
    }
    try {
      final URI uri = URI.create(model);
      if ("file".equalsIgnoreCase(uri.getScheme())) {
        return Paths.get(uri).toAbsolutePath().normalize().toString();
      }
      return model;
    } catch (Exception ignored) {
      return model;
    }
  }
}

