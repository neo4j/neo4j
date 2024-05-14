/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.parser.lexer;

import org.antlr.v4.runtime.Token;
import org.neo4j.cypher.internal.util.InputPosition;

public interface CypherQueryAccess {

    /** Returns the input query
     *  - excluding pre-parser options
     *  - including white space
     *  - including comments
     *  - including unicode escape codes
     */
    String inputQuery();

    /**
     * Returns the offset table to translate from antlr positions to input query positions
     * or null if parser offsets are equal to offsets in input string.
     */
    OffsetTable offsetTable();

    /**
     * Returns a substring of the input query.
     * Note! The query input string at this point is normally
     *       - Not including preparser options (explain/profile/cypher)
     *       - Including unicode escapes (replaced before parsing)
     *       - Including whitespace and comments (on hidden channel in parser)
     * In most situations Token/RuleContext.getText is what you want.
     */
    default String inputText(Token start, Token stop) {
        return inputQuery().substring(inputOffset(start.getStartIndex()), inputOffset(stop.getStopIndex()) + 1);
    }

    /**
     * Converts character offset from the parser (in codepoints) to offset in the input query string.
     * Note! The query input string at this point is normally
     *       - Not including preparser options (explain/profile/cypher)
     *       - Including unicode escapes (replaced before parsing)
     *       - Including whitespace and comments (on hidden channel in parser)
     */
    default int inputOffset(int parserOffset) {
        final var offsetTable = offsetTable();
        if (offsetTable == null || parserOffset < offsetTable.start()) {
            return parserOffset;
        } else {
            return offsetTable.offsets()[(parserOffset - offsetTable.start()) * 3];
        }
    }

    /**
     * Returns the input query input position based on parser positions.
     * Note! The query input string at this point is normally
     *       - Not including preparser options (explain/profile/cypher)
     *       - Including unicode escapes (replaced before parsing)
     *       - Including whitespace and comments (on hidden channel in parser)
     *
     * @param parserIndex code point position as reported by the parser
     * @param parserLine line number as reported by the parser
     * @param parserColumn code point position in the line as reported by the parser
     * @return input position relative to char (NOT codepoint) in the input query (before unicode escapement)
     */
    default InputPosition inputPosition(int parserIndex, int parserLine, int parserColumn) {
        final var offsetTable = offsetTable();
        if (offsetTable == null || parserIndex < offsetTable.start()) {
            return InputPosition.apply(parserIndex, parserLine, parserColumn + 1);
        } else {
            final int i = (parserIndex - offsetTable.start()) * 3;
            final var o = offsetTable.offsets();
            return InputPosition.apply(o[i], o[i + 1], o[i + 2]);
        }
    }

    /**
     * Input position offsets.
     *
     * @param offsets [offset0, line0, col0, offset1, line1, col1, ...]
     * @param start start offset of the offsets table TODO Remove, always offsets[0]
     */
    record OffsetTable(int[] offsets, int start) {}
}
