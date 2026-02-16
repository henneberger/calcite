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

import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;

import java.util.ArrayList;
import java.util.List;

/** Text and position conversion helpers. */
final class TextUtil {
  private TextUtil() {
  }

  static int[] lineOffsets(String text) {
    final List<Integer> starts = new ArrayList<>();
    starts.add(0);
    for (int i = 0; i < text.length(); i++) {
      if (text.charAt(i) == '\n') {
        starts.add(i + 1);
      }
    }
    starts.add(text.length());
    final int[] offsets = new int[starts.size()];
    for (int i = 0; i < starts.size(); i++) {
      offsets[i] = starts.get(i);
    }
    return offsets;
  }

  static int toOffset(String text, Position position) {
    final int[] offsets = lineOffsets(text);
    final int line = clamp(position.getLine(), 0, Math.max(0, offsets.length - 2));
    final int lineStart = offsets[line];
    final int lineEnd = offsets[line + 1];
    final int character = clamp(position.getCharacter(), 0, Math.max(0, lineEnd - lineStart));
    return lineStart + character;
  }

  static Position toPosition(String text, int offset) {
    final int[] offsets = lineOffsets(text);
    final int bounded = clamp(offset, 0, text.length());
    int line = 0;
    while (line + 1 < offsets.length && offsets[line + 1] <= bounded) {
      line++;
    }
    return new Position(line, bounded - offsets[line]);
  }

  static Range fullRange(String text) {
    return new Range(new Position(0, 0), toPosition(text, text.length()));
  }

  static Range fromParserCoordinates(
      String text,
      int line,
      int column,
      int endLine,
      int endColumn) {
    if (line <= 0 || column <= 0) {
      return new Range(new Position(0, 0), new Position(0, 1));
    }
    final int startLine = Math.max(0, line - 1);
    final int startColumn = Math.max(0, column - 1);
    final int finishLine = Math.max(startLine, endLine - 1);
    final int finishColumn = Math.max(0, endColumn - 1);
    return new Range(
        safePosition(text, startLine, startColumn),
        safePosition(text, finishLine, finishColumn));
  }

  static Position safePosition(String text, int line, int character) {
    return toPosition(text, toOffset(text, new Position(line, character)));
  }

  static boolean isIdentifierPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }

  static Range wordRangeAt(String text, int offset) {
    if (text.isEmpty()) {
      return new Range(new Position(0, 0), new Position(0, 0));
    }
    int start = clamp(offset, 0, text.length());
    int end = start;
    while (start > 0 && isIdentifierPart(text.charAt(start - 1))) {
      start--;
    }
    while (end < text.length() && isIdentifierPart(text.charAt(end))) {
      end++;
    }
    return new Range(toPosition(text, start), toPosition(text, end));
  }

  private static int clamp(int value, int min, int max) {
    return Math.max(min, Math.min(max, value));
  }
}

