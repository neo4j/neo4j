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
package org.neo4j.server.rest.repr;

import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Label.label;

public class SchemaIndexRepresentationTest
{
    @Test
    public void shouldIncludeLabel()
    {
        // GIVEN
        String labelName = "person";
        String propertyKey = "name";
        IndexDefinition definition = mock( IndexDefinition.class );
        when( definition.getLabel() ).thenReturn( label( labelName ) );
        when( definition.getPropertyKeys() ).thenReturn( asList( propertyKey ) );
        IndexDefinitionRepresentation representation = new IndexDefinitionRepresentation( definition );
        Map<String, Object> serialized = RepresentationTestAccess.serialize( representation );

        // THEN
        assertEquals( asList( propertyKey ), serialized.get( "property_keys" ) );
        assertEquals( labelName, serialized.get( "label" ) );
    }
}
