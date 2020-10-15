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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.util.Map;

final class JoltMapSerializer extends StdSerializer<Map<String,?>>
{
    static final JavaType HANDLED_TYPE = TypeFactory.defaultInstance()
                                                    .constructMapType( Map.class, String.class, Object.class );

    JoltMapSerializer()
    {
        super( HANDLED_TYPE );
    }

    @Override
    public void serialize( Map<String,?> map, JsonGenerator generator, SerializerProvider provider )
            throws IOException
    {
        generator.writeStartObject( map );
        generator.writeFieldName( Sigil.MAP.getValue() );
        generator.writeStartObject();

        for ( var entry : map.entrySet() )
        {
            generator.writeFieldName( entry.getKey() );
            generator.writeObject( entry.getValue() );
        }

        generator.writeEndObject();
        generator.writeEndObject();
    }
}
