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
package org.neo4j.bolt.protocol.common.message.decoder.util;

import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.util.PackstreamConversions;
import org.neo4j.values.virtual.MapValue;

public final class TransactionInitiatingMetadataParser {
    public static final String FIELD_DATABASE_NAME = "db";
    private static final String FIELD_IMPERSONATED_USER = "imp_user";

    private TransactionInitiatingMetadataParser() {}

    public static String readDatabaseName(MapValue meta) throws PackstreamReaderException {
        var databaseName =
                PackstreamConversions.asNullableStringValue(FIELD_DATABASE_NAME, meta.get(FIELD_DATABASE_NAME));

        // we permit both empty strings and null values as a reference to the default user/system
        // database, so we'll unify it at decoder level
        if (databaseName != null && databaseName.isEmpty()) {
            return null;
        }

        return databaseName;
    }

    public static String readImpersonatedUser(MapValue value) throws PackstreamReaderException {
        return PackstreamConversions.asNullableStringValue(FIELD_IMPERSONATED_USER, value.get(FIELD_IMPERSONATED_USER));
    }
}
