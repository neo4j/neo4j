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
import org.neo4j.common.EntityType;

public class ElementIdDecoderV1 implements ElementIdDecoder.VersionedElementIdDecoder {

    public static byte ELEMENT_ID_FORMAT_VERSION = 1;

    @Override
    public long nodeId(String elementId) {
        return decode(elementId, EntityType.NODE).entityId;
    }

    @Override
    public long relationshipId(String elementId) {
        return decode(elementId, EntityType.RELATIONSHIP).entityId;
    }

    @Override
    public UUID database(String elementId) {
        return decodeDatabase(elementId);
    }

    protected record ElementId(long entityId, EntityType entityType, UUID database) {}

    protected static ElementId decode(String id, EntityType expectedType) {
        var elementId = decode(id);
        verifyEntityType(id, elementId.entityType, expectedType);
        return elementId;
    }

    protected static ElementId decode(String id) {
        try {
            var parts = readParts(id);
            var header = Byte.parseByte(parts[0]);
            verifyVersion(id, header);

            var database = UUID.fromString(parts[1]);
            var entityId = Long.parseLong(parts[2]);
            var entityType = decodeEntityType(id, header);
            return new ElementId(entityId, entityType, database);
        } catch (IllegalArgumentException iae) {
            throw iae;
        } catch (Exception e) {
            throw new IllegalArgumentException(format("Element ID %s has an unexpected format.", id), e);
        }
    }

    protected static UUID decodeDatabase(String id) {
        try {
            var parts = readParts(id);
            var header = Byte.parseByte(parts[0]);
            verifyVersion(id, header);

            return UUID.fromString(parts[1]);
        } catch (IllegalArgumentException iae) {
            throw iae;
        } catch (Exception e) {
            throw new IllegalArgumentException(format("Element ID %s has an unexpected format.", id), e);
        }
    }

    private static String[] readParts(String id) {
        String[] parts = id.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException(format("Element ID %s has an unexpected format.", id));
        }
        return parts;
    }

    private static void verifyVersion(String id, byte header) {
        byte version = (byte) (header >>> 2);
        if (version != ELEMENT_ID_FORMAT_VERSION) {
            throw new IllegalArgumentException(format("Element ID %s has an unexpected version %d", id, version));
        }
    }

    private static EntityType decodeEntityType(String id, byte header) {
        byte entityTypeId = (byte) (header & 0x3);

        return switch (entityTypeId) {
            case 0 -> EntityType.NODE;
            case 1 -> EntityType.RELATIONSHIP;
            default -> throw new IllegalArgumentException(
                    format("Element ID %s has unknown entity type ID %s", id, entityTypeId));
        };
    }

    private static void verifyEntityType(String id, EntityType actual, EntityType expected) {
        if (actual != expected) {
            throw new IllegalArgumentException(
                    format("Element ID %s has unexpected entity type %s, was expecting %s", id, actual, expected));
        }
    }
}
