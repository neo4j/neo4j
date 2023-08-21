/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema.trigram;

import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

final class TrigramTokenStream extends TokenStream {
    // 'n' value in 'ngram'
    private static final int N = 3;
    private static final int MAX_CHARS = N * Character.charCount(Character.MAX_CODE_POINT);
    private final CodePointBuffer codePointBuffer;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private int offset = -1;

    public TrigramTokenStream(String text) {
        codePointBuffer = getCodePoints(text);
        termAtt.resizeBuffer(MAX_CHARS);
    }

    @Override
    public boolean incrementToken() {
        clearAttributes();
        if (offset == -1) {
            if (codePointBuffer.codePointCount < N) {
                int length = CharacterUtils.toChars(
                        codePointBuffer.codePoints, 0, codePointBuffer.codePointCount, termAtt.buffer(), 0);
                termAtt.setLength(length);
                offset = codePointBuffer.codePointCount;
                return true;
            }
        }

        offset++;

        if (codePointBuffer.codePointCount - offset < N) {
            return false;
        }

        int length = CharacterUtils.toChars(codePointBuffer.codePoints, offset, N, termAtt.buffer(), 0);
        termAtt.setLength(length);
        return true;
    }

    static CodePointBuffer getCodePoints(String text) {
        var codePointBuffer = new int[text.length()];
        int codePointCount = 0;
        for (int i = 0; i < text.length(); ) {
            int cp = text.codePointAt(i);

            int charCount = Character.charCount(cp);
            codePointBuffer[codePointCount++] = cp;
            i += charCount;
        }
        return new CodePointBuffer(codePointBuffer, codePointCount);
    }

    record CodePointBuffer(int[] codePoints, int codePointCount) {}
}
