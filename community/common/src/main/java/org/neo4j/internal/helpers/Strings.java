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
package org.neo4j.internal.helpers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Helper functions for working with strings.
 */
public final class Strings {
    public static final String TAB = "\t";

    private Strings() {}

    public static String prettyPrint(Object o) {
        if (o == null) {
            return "null";
        }

        Class<?> clazz = o.getClass();
        if (clazz.isArray()) {
            if (clazz == byte[].class) {
                return Arrays.toString((byte[]) o);
            } else if (clazz == short[].class) {
                return Arrays.toString((short[]) o);
            } else if (clazz == int[].class) {
                return Arrays.toString((int[]) o);
            } else if (clazz == long[].class) {
                return Arrays.toString((long[]) o);
            } else if (clazz == float[].class) {
                return Arrays.toString((float[]) o);
            } else if (clazz == double[].class) {
                return Arrays.toString((double[]) o);
            } else if (clazz == char[].class) {
                return Arrays.toString((char[]) o);
            } else if (clazz == boolean[].class) {
                return Arrays.toString((boolean[]) o);
            } else {
                return Arrays.deepToString((Object[]) o);
            }
        } else {
            return String.valueOf(o);
        }
    }

    public static String escape(String arg) {
        StringBuilder builder = new StringBuilder(arg.length());
        try {
            escape(builder, arg);
        } catch (IOException e) {
            throw new AssertionError("IOException from using StringBuilder", e);
        }
        return builder.toString();
    }

    /**
     * Joining independent lines from provided elements into one line with {@link System#lineSeparator()} after
     * each element
     * @param elements - lines to join
     * @return joined line
     */
    public static String joinAsLines(String... elements) {
        return String.join(System.lineSeparator(), elements);
    }

    public static void escape(Appendable output, String arg) throws IOException {
        int len = arg.length();
        for (int i = 0; i < len; i++) {
            char ch = arg.charAt(i);
            switch (ch) {
                case '"' -> output.append("\\\"");
                case '\'' -> output.append("\\'");
                case '\\' -> output.append("\\\\");
                case '\n' -> output.append("\\n");
                case '\t' -> output.append("\\t");
                case '\r' -> output.append("\\r");
                case '\b' -> output.append("\\b");
                case '\f' -> output.append("\\f");
                default -> output.append(ch);
            }
        }
    }

    private static class CodePointsIterator implements Iterator<Integer> {
        private String s;
        private int numCodePoints;
        private int charIndex;
        private int codePointIndex;

        public CodePointsIterator(String s) {
            this.s = s;
            numCodePoints = s.codePointCount(0, s.length());
            charIndex = 0;
            codePointIndex = 0;
        }

        @Override
        public boolean hasNext() {
            return codePointIndex < numCodePoints;
        }

        @Override
        public Integer next() {
            var result = s.codePointAt(charIndex);
            charIndex = s.offsetByCodePoints(charIndex, 1);
            ++codePointIndex;
            return result;
        }
    }

    // This is needed to cross-compile the semantic analysis to Java and Javascript,
    // given String::codePoints cannot be transpiled directly with the library we are using
    public static IntStream codePoints(String s) {
        Iterable<Integer> iterable = () -> new CodePointsIterator(s);
        return StreamSupport.stream(iterable.spliterator(), false).mapToInt(Integer::intValue);
    }
}
