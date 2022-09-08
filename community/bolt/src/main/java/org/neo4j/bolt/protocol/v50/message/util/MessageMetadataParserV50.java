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
package org.neo4j.bolt.protocol.v50.message.util;

import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

public class MessageMetadataParserV50 {
    public static final String TX_TYPE_KEY = "tx_type";

    public static String parseTxType(MapValue meta) throws PackstreamReaderException {
        var anyValue = meta.get(TX_TYPE_KEY);
        if (anyValue == Values.NO_VALUE) {
            return null;
        }

        if (anyValue instanceof StringValue) {
            return ((StringValue) anyValue).stringValue();
        }

        throw new IllegalStructArgumentException(
                TX_TYPE_KEY, "Expecting tx type value to be a String value, but got: " + anyValue);
    }
}
