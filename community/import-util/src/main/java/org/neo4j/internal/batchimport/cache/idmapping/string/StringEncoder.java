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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import java.nio.charset.StandardCharsets;
import org.neo4j.hashing.WyHash;

/**
 * Encodes String into a long with very small chance of collision, i.e. two different Strings encoded into
 * the same long value.
 *
 * Assumes a single thread making all calls to {@link #encode(Object)}.
 */
public class StringEncoder implements Encoder {
    private static final long ID_BITS = 0x00FFFFFF_FFFFFFFFL;

    @Override
    public long encode(Object any) {
        byte[] bytes = convertToString(any).getBytes(StandardCharsets.UTF_8);
        return WyHash.hash(bytes, 0, bytes.length) & ID_BITS | 1; // We cannot return 0 because that is a special value
    }

    private String convertToString(Object any) {
        if (any instanceof String string) {
            return string;
        }
        return any.toString();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
