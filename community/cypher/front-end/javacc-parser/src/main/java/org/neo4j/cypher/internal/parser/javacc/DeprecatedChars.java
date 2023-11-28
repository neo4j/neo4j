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
package org.neo4j.cypher.internal.parser.javacc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DeprecatedChars {
    private static final char[] DEPRECATED_CHARS;
    private static final char SAFE_FROM = '%';
    private static final char SAFE_TO = '~';

    private DeprecatedChars() {}

    public static boolean containsDeprecatedChar(String s) {
        final int length = s.length();
        for (int offset = 0; offset < length; ) {
            final int codepoint = s.codePointAt(offset);
            if (isDeprecated(codepoint)) {
                return true;
            }
            offset += Character.charCount(codepoint);
        }
        return false;
    }

    public static List<Character> deprecatedChars(String s) {
        final var result = new ArrayList<Character>(1);
        for (char deprecated : DEPRECATED_CHARS) {
            if (s.indexOf(deprecated) != -1) {
                result.add(deprecated);
            }
        }
        return result;
    }

    private static boolean isDeprecated(final int c) {
        if (c >= SAFE_FROM && c <= SAFE_TO) {
            return false;
        }
        for (char deprecated : DEPRECATED_CHARS) {
            if (deprecated == c) {
                return true;
            }
        }
        return false;
    }

    static {
        DEPRECATED_CHARS = new char[] {
            '\u0000',
            '\u0001',
            '\u0002',
            '\u0003',
            '\u0004',
            '\u0005',
            '\u0006',
            '\u0007',
            '\u0008',
            '\u000E',
            '\u000F',
            '\u0010',
            '\u0011',
            '\u0012',
            '\u0013',
            '\u0014',
            '\u0015',
            '\u0016',
            '\u0017',
            '\u0018',
            '\u0019',
            '\u001A',
            '\u001B',
            '\u007F',
            '\u0080',
            '\u0081',
            '\u0082',
            '\u0083',
            '\u0084',
            '\u0085',
            '\u0086',
            '\u0087',
            '\u0088',
            '\u0089',
            '\u008A',
            '\u008B',
            '\u008C',
            '\u008D',
            '\u008E',
            '\u008F',
            '\u0090',
            '\u0091',
            '\u0092',
            '\u0093',
            '\u0094',
            '\u0095',
            '\u0096',
            '\u0097',
            '\u0098',
            '\u0099',
            '\u009A',
            '\u009B',
            '\u009C',
            '\u009D',
            '\u009E',
            '\u009F',
            // Category Sc
            '$',
            '¢',
            '£',
            '¤',
            '¥',
            // Category Cf
            '\u00AD',
            '\u0600',
            '\u0601',
            '\u0602',
            '\u0603',
            '\u0604',
            '\u0605',
            '\u061C',
            '\u06DD',
            '\u070F',
            '\u08E2',
            '\u180E',
            '\u200B',
            '\u200C',
            '\u200D',
            '\u200E',
            '\u200F',
            '\u202A',
            '\u202B',
            '\u202C',
            '\u202D',
            '\u202E',
            '\u2060',
            '\u2061',
            '\u2062',
            '\u2063',
            '\u2064',
            '\u2066',
            '\u2067',
            '\u2068',
            '\u2069',
            '\u206A',
            '\u206B',
            '\u206C',
            '\u206D',
            '\u206E',
            '\u206F',
            '\u2E2F',
            '\uFEFF',
            '\uFFF9',
            '\uFFFA',
            '\uFFFB'
        };
        Arrays.sort(DEPRECATED_CHARS);

        for (char deprecated : DEPRECATED_CHARS) {
            if (deprecated >= SAFE_FROM && deprecated <= SAFE_TO) {
                throw new IllegalStateException("safeFrom/safeTo is incorrect");
            }
        }
    }
}
