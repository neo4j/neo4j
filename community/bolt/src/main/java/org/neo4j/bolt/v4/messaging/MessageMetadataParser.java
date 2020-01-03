/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v4.messaging;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

public final class MessageMetadataParser
{
    public static final String DB_NAME_KEY = "db";
    public static final String ABSENT_DB_NAME = "";

    /**
     * Empty or null value indicates the default database is selected
     */
    static String parseDatabaseName( MapValue meta ) throws BoltIOException
    {
        AnyValue anyValue = meta.get( DB_NAME_KEY );
        if ( anyValue == Values.NO_VALUE )
        {
            return ABSENT_DB_NAME;
        }
        else if ( anyValue instanceof StringValue )
        {
            return ((StringValue) anyValue).stringValue();
        }
        else
        {
            throw new BoltIOException( Status.Request.Invalid, "Expecting database name value to be a String value, but got: " + anyValue );
        }
    }
}
