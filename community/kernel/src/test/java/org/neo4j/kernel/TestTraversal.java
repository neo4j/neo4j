/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Test;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;

import static org.junit.Assert.*;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class TestTraversal
{
    private static RelationshipType T1 = withName( "T1" ),
            T2 = withName( "T2" ), T3 = withName( "T3" );

    @Test
    public void canCreateExpanderWithMultipleTypesAndDirections()
    {
        assertNotNull( PathExpanders.forTypesAndDirections( T1, INCOMING, T2,
                OUTGOING, T3, BOTH ) );
    }
}
