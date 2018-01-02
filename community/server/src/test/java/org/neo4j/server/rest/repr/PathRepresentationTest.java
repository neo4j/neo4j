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
package org.neo4j.server.rest.repr;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;
import static org.neo4j.server.rest.repr.RepresentationTestBase.NODE_URI_PATTERN;
import static org.neo4j.server.rest.repr.RepresentationTestBase.RELATIONSHIP_URI_PATTERN;
import static org.neo4j.server.rest.repr.RepresentationTestBase.assertUriMatches;
import static org.neo4j.test.mocking.GraphMock.node;
import static org.neo4j.test.mocking.GraphMock.path;
import static org.neo4j.test.mocking.GraphMock.relationship;
import static org.neo4j.test.mocking.Link.link;
import static org.neo4j.test.mocking.Properties.properties;

public class PathRepresentationTest
{

    @Test
    public void shouldHaveLength() throws BadInputException
    {
        assertNotNull( pathrep().length() );
    }

    @Test
    public void shouldHaveStartNodeLink() throws BadInputException
    {
        assertUriMatches( NODE_URI_PATTERN, pathrep().startNode() );
    }

    @Test
    public void shouldHaveEndNodeLink() throws BadInputException
    {
        assertUriMatches( NODE_URI_PATTERN, pathrep().endNode() );
    }

    @Test
    public void shouldHaveNodeList() throws BadInputException
    {
        assertNotNull( pathrep().nodes() );
    }

    @Test
    public void shouldHaveRelationshipList() throws BadInputException
    {
        assertNotNull( pathrep().relationships() );
    }

    @Test
    public void shouldHaveDirectionList() throws BadInputException
    {
        assertNotNull( pathrep().directions() );
    }

    @Test
    public void shouldSerialiseToMap()
    {
        Map<String, Object> repr = serialize( pathrep() );
        assertNotNull( repr );
        verifySerialisation( repr );
    }

    /*
     * Construct a sample path representation of the form:
     *
     *     (A)-[:LOVES]->(B)<-[:HATES]-(C)-[:KNOWS]->(D)
     *
     * This contains two forward relationships and one backward relationship
     * which is represented in the "directions" value of the output. We should
     * therefore see something like the following:
     * 
     * {
     *     "length" : 3,
     *     "start" : "http://neo4j.org/node/0",
     *     "end" : "http://neo4j.org/node/3",
     *     "nodes" : [
     *         "http://neo4j.org/node/0", "http://neo4j.org/node/1",
     *         "http://neo4j.org/node/2", "http://neo4j.org/node/3"
     *     ],
     *     "relationships" : [
     *         "http://neo4j.org/relationship/17",
     *         "http://neo4j.org/relationship/18",
     *         "http://neo4j.org/relationship/19"
     *     ],
     *     "directions" : [ "->", "<-", "->" ]
     * }
     *
     */
    private PathRepresentation<Path> pathrep()
    {
        Node a = node( 0, properties() );
        Node b = node( 1, properties() );
        Node c = node( 2, properties() );
        Node d = node( 3, properties() );

        Relationship ab = relationship( 17, a, "LOVES", b );
        Relationship cb = relationship( 18, c, "HATES", b );
        Relationship cd = relationship( 19, c, "KNOWS", d );

        return new PathRepresentation<Path>(
                path( a, link( ab, b ), link( cb, c ), link( cd, d ) ));
    }


    public static void verifySerialisation( Map<String, Object> pathrep )
    {
        assertNotNull( pathrep.get( "length" ) );
        int length = Integer.parseInt(pathrep.get("length").toString());

        assertUriMatches( NODE_URI_PATTERN, pathrep.get( "start" ).toString() );
        assertUriMatches( NODE_URI_PATTERN, pathrep.get( "end" ).toString() );

        Object nodes = pathrep.get( "nodes" );
        assertTrue( nodes instanceof List );
        List nodeList = (List) nodes;
        assertEquals( length + 1, nodeList.size() );
        for ( Object node : nodeList ) {
            assertUriMatches( NODE_URI_PATTERN, node.toString() );
        }

        Object rels = pathrep.get( "relationships" );
        assertTrue( rels instanceof List );
        List relList = (List) rels;
        assertEquals( length, relList.size() );
        for ( Object rel : relList ) {
            assertUriMatches( RELATIONSHIP_URI_PATTERN, rel.toString() );
        }

        Object directions = pathrep.get( "directions" );
        assertTrue( directions instanceof List );
        List directionList = (List) directions;
        assertEquals( length, directionList.size() );
        assertEquals( "->", directionList.get(0).toString() );
        assertEquals( "<-", directionList.get(1).toString() );
        assertEquals( "->", directionList.get(2).toString() );
    }

}
