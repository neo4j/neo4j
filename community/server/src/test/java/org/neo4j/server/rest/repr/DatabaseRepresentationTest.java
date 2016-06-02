/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class DatabaseRepresentationTest
{
    @Test
    public void shouldProvideUrisForTheAvailableRoots()
    {
        // GIVEN
        Node mockNode = mock( Node.class );
        GraphDatabaseService mockDb = mock( GraphDatabaseService.class );
        when( mockDb.getReferenceNode() ).thenReturn( mockNode );
        DatabaseRepresentation representation = new DatabaseRepresentation( mockDb );

        // WHEN
        Map<String, Object> map = RepresentationTestAccess.serialize( representation );

        // THEN
        assertRootExists( map, "node" );
        assertRootExists( map, "node_index", "/index/node" );
        assertRootExists( map, "relationship" );
        assertRootExists( map, "relationship_index", "/index/relationship" );
        assertRootExists( map, "relationship_types", "/relationship/types" );
        assertRootExists( map, "extensions_info", "/ext" );
        assertRootExists( map, "batch" );
        assertRootExists( map, "cypher" );
        assertTrue( map.containsKey( "neo4j_version" ) );
    }

    private void assertRootExists( Map<String, Object> representation, String key, String containsValue )
    {
        assertTrue( "Representation should have contained '" + key + "'", representation.containsKey( key ) );
        assertTrue( "Representation for '" + key + "' should have contained '" + containsValue + "'",
                representation.get( key ).toString().contains( containsValue ) );
    }

    private void assertRootExists( Map<String, Object> representation, String key )
    {
        assertRootExists( representation, key, "/" + key );
    }
}
