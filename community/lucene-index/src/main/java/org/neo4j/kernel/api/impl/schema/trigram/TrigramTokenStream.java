/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema.trigram;

import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

final class TrigramTokenStream extends TokenStream {
    private final CodePointBuffer codePointBuffer;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private int offset = -1;

    public TrigramTokenStream(String text) {
        codePointBuffer = getCodePoints(text);
        // a trigram consisting of 3 unicode code points can be up to 6 Java chars long
        termAtt.resizeBuffer(6);
    }

    @Override
    public boolean incrementToken() {
        clearAttributes();
        if (offset == -1) {
            if (codePointBuffer.codePointCount < 3) {
                int length = CharacterUtils.toChars(
                        codePointBuffer.codePoints, 0, codePointBuffer.codePointCount, termAtt.buffer(), 0);
                termAtt.setLength(length);
                offset = codePointBuffer.codePointCount;
                return true;
            }
        }

        offset++;

        if (codePointBuffer.codePointCount - offset < 3) {
            return false;
        }

        int length = CharacterUtils.toChars(codePointBuffer.codePoints, offset, 3, termAtt.buffer(), 0);
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
