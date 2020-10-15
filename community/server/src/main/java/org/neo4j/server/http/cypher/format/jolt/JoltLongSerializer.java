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
package org.neo4j.server.http.cypher.format.jolt;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * A dedicated long serializer is needed to handle writing the correct sigil depending on the value. If the long value is inside the Int32 range we write a Int
 * sigil, otherwise we use the Real sigil.
 *
 * @param <T> long or Long
 */
final class JoltLongSerializer<T> extends StdSerializer<T>
{
    JoltLongSerializer( Class<T> t )
    {
        super( t );
    }

    @Override
    public void serialize( T value, JsonGenerator generator, SerializerProvider provider ) throws IOException
    {
        generator.writeStartObject( value );

        long longValue = (long) value;

        if ( longValue >= Integer.MIN_VALUE && longValue < Integer.MAX_VALUE )
        {
            generator.writeFieldName( Sigil.INTEGER.getValue() );
        }
        else
        {
            generator.writeFieldName( Sigil.REAL.getValue() );
        }

        generator.writeString( String.valueOf( longValue ) );
        generator.writeEndObject();
    }
}
