/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v3.messaging.response;

import java.util.Arrays;

import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.values.AnyValue;

public class RecordMessage implements ResponseMessage
{
    public static final byte SIGNATURE = 0x71;
    private final AnyValue[] fields;

    public RecordMessage( AnyValue[] fields )
    {
        this.fields = fields;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public ResponseMessage copy()
    {
        return new RecordMessage( Arrays.copyOf( fields, fields.length ) );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        RecordMessage that = (RecordMessage) o;

        return Arrays.equals( fields, that.fields );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( fields );
    }

    @Override
    public String toString()
    {
        return "RECORD " + Arrays.toString( fields );
    }

    public AnyValue[] fields()
    {
        return fields;
    }
}
