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
package org.neo4j.values;

import static java.lang.String.format;

import java.util.UUID;

public final class ElementIdDecoder {
    private static final ElementIdDecoderV1 elementIdDecoderV1 = new ElementIdDecoderV1();

    public static long nodeId(String elementId) {
        return getElementIdDecoder(elementId).nodeId(elementId);
    }

    public static UUID database(String elementId) {
        return getElementIdDecoder(elementId).database(elementId);
    }

    public static long relationshipId(String elementId) {
        return getElementIdDecoder(elementId).relationshipId(elementId);
    }

    private static ElementIdDecoderV1 getElementIdDecoder(String elementId) {
        Byte version = getVersion(elementId);
        if (ElementIdDecoderV1.ELEMENT_ID_FORMAT_VERSION == version) {
            return elementIdDecoderV1;
        } else {
            throw new IllegalArgumentException(
                    format("Element ID %s has an unexpected version %s", elementId, version));
        }
    }

    private static Byte getVersion(String elementId) {
        String[] parts = elementId.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException(format("Element ID %s has an unexpected format.", elementId));
        }

        var header = Byte.parseByte(parts[0]);
        return (byte) (header >>> 2);
    }

    public interface VersionedElementIdDecoder {
        long nodeId(String elementId);

        long relationshipId(String elementId);

        UUID database(String elementId);
    }
}
