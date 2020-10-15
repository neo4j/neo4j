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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

final class JoltPathSerializer extends StdSerializer<Path>
{
    JoltPathSerializer()
    {
        super( Path.class );
    }

    @Override
    public void serialize( Path path, JsonGenerator generator, SerializerProvider provider ) throws IOException
    {
        generator.writeStartObject( path );
        generator.writeFieldName( Sigil.PATH.getValue() );

        generator.writeStartArray();

        var it = path.iterator();
        var lastNodeId = 0L;

        while ( it.hasNext() )
        {
            var entity = it.next();
            if ( entity instanceof Node )
            {
                Node node = (Node) entity;
                lastNodeId = node.getId();

                generator.writeObject( node );
            }
            else if ( entity instanceof Relationship )
            {
                Relationship rel = (Relationship) entity;

                if ( rel.getStartNodeId() != lastNodeId )
                {
                    // we want a reversed relationship here so the path flows correctly
                    generator.writeObject( JoltRelationship.fromRelationshipReversed( rel ) );
                }
                else
                {
                    generator.writeObject( rel );
                }
            }
        }

        generator.writeEndArray();

        generator.writeEndObject();
    }
}
