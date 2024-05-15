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
     * Returns table containing offsets to translate between parser query offset and input query offsets.
     * Or null if all parser offsets are equal to input query offsets.
     * Contains packed offset, line, char in line like this [offset0, line0, col0, offset1, line1, col1, ...].
     */
    int[] offsetTable();

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
        if (offsetTable == null || parserOffset < offsetTable[0]) {
            return parserOffset;
        } else {
            return offsetTable[(parserOffset - offsetTable[0]) * 3];
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
        if (offsetTable == null || parserIndex < offsetTable[0]) {
            return InputPosition.apply(parserIndex, parserLine, parserColumn + 1);
        } else {
            final int i = (parserIndex - offsetTable[0]) * 3;
            return InputPosition.apply(offsetTable[i], offsetTable[i + 1], offsetTable[i + 2]);
        }
    }
}
