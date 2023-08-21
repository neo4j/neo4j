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
package org.neo4j.string;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

public final class EncodingUtils {
    private EncodingUtils() {}

    /**
     * Get the "native" charset that is used by the environment, should be used when reading
     * from {@link System#in} and writing to {@link System#out} and {@link System#err}.
     * @return the "native" charset that is used by the environment.
     */
    public static Charset getNativeCharset() {
        try {
            String nativeEncoding = System.getProperty("native.encoding");
            if (nativeEncoding != null) {
                return Charset.forName(nativeEncoding);
            }
        } catch (UnsupportedCharsetException ignored) {
        }
        return Charset.defaultCharset();
    }
}
