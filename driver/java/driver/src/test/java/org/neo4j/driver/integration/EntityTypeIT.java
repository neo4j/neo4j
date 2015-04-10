/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.Node;
import org.neo4j.driver.Path;
import org.neo4j.driver.Relationship;
import org.neo4j.driver.util.TestSession;

import static org.junit.Assert.assertTrue;

public class EntityTypeIT
{
    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldReturnIdentitiesOfNodes() throws Throwable
    {
        // When
        Node node = session.run( "CREATE (n) RETURN n" ).single().get( "n" ).asNode();

        // Then
        assertTrue( node.identity().toString(), node.identity().toString().matches( "node/\\d+" ) );
    }

    @Test
    public void shouldReturnIdentitiesOfRelationships() throws Throwable
    {
        // When
        Relationship rel = session.run( "CREATE ()-[r:T]->() RETURN r" ).single().get( "r" ).asRelationship();

        // Then
        assertTrue( rel.start().toString(), rel.start().toString().matches( "node/\\d+" ) );
        assertTrue( rel.end().toString(), rel.end().toString().matches( "node/\\d+" ) );
        assertTrue( rel.identity().toString(), rel.identity().toString().matches( "rel/\\d+" ) );
    }

    @Test
    public void shouldReturnIdentitiesOfPaths() throws Throwable
    {
        // When
        Path path = session.run( "CREATE p=()-[r:T]->() RETURN p" ).single().get( "p" ).asPath();

        // Then
        assertTrue( path.start().identity().toString(), path.start().identity().toString().matches( "node/\\d+" ) );
        assertTrue( path.end().identity().toString(), path.end().identity().toString().matches( "node/\\d+" ) );

        Path.Segment segment = path.iterator().next();

        assertTrue( segment.start().identity().toString(),
                segment.start().identity().toString().matches( "node/\\d+" ) );
        assertTrue( segment.relationship().identity().toString(),
                segment.relationship().identity().toString().matches( "rel/\\d+" ) );
        assertTrue( segment.end().identity().toString(), segment.end().identity().toString().matches( "node/\\d+" ) );
    }

}
