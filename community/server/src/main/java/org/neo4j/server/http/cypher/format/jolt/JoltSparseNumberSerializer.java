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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.function.Function;

/**
 * Omits the number sigil if the value is in integer range.
 *
 * @param <T>
 */
final class JoltSparseNumberSerializer<T extends Number> extends StdSerializer<T>
{
    private final JsonSerializer<T> delegate;
    private final Sigil sigil;
    private final Function<T,String> converter;

    JoltSparseNumberSerializer( Class<T> t, Sigil sigil, Function<T,String> converter )
    {
        super( t );
        this.sigil = sigil;
        this.converter = converter;

        this.delegate = new JoltDelegatingValueSerializer<>( t, sigil, converter );
    }

    @Override
    public void serialize( T value, JsonGenerator generator, SerializerProvider provider ) throws IOException
    {
        long longValue = value.longValue();
        if ( longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE )
        {
            generator.writeNumber( longValue );
        }
        else
        {
            delegate.serialize( value, generator, provider );
        }
    }
}
