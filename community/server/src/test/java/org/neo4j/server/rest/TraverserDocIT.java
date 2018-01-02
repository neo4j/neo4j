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
package org.neo4j.server.rest;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;

public class TraverserDocIT extends AbstractRestFunctionalTestBase
{

    @Test
    public void shouldGet404WhenTraversingFromNonExistentNode()
    {
        gen().expectedStatus( Status.NOT_FOUND.getStatusCode() ).payload(
                "{}" ).post( getDataUri() + "node/10000/traverse/node" ).entity();
    }

    @Test
    @Graph( nodes = {@NODE(name="I")} )
    public void shouldGet200WhenNoHitsFromTraversing()
    {
        assertSize( 0,gen().expectedStatus( 200 ).payload( "" ).post(
                getTraverseUriNodes( getNode( "I" ) ) ).entity());
    }

    /**
     * In order to return relationships,
     * simply specify the return type as part of the URL.
     */
    @Test
    @Graph( {"I know you", "I own car"} )
    public void return_relationships_from_a_traversal()
    {
        assertSize( 2, gen().expectedStatus( 200 ).payload( "{\"order\":\"breadth_first\",\"uniqueness\":\"none\",\"return_filter\":{\"language\":\"builtin\",\"name\":\"all\"}}" ).post(
                getTraverseUriRelationships( getNode( "I" ) ) ).entity());
    }


    /**
     * In order to return paths from a traversal,
     * specify the +Path+ return type as part of the URL.
     */
    @Test
    @Graph( {"I know you", "I own car"} )
    public void return_paths_from_a_traversal()
    {
        assertSize( 3, gen().expectedStatus( 200 ).payload( "{\"order\":\"breadth_first\",\"uniqueness\":\"none\",\"return_filter\":{\"language\":\"builtin\",\"name\":\"all\"}}" ).post(
                getTraverseUriPaths( getNode( "I" ) ) ).entity());
    }


    private String getTraverseUriRelationships( Node node )
    {
        return getNodeUri( node) + "/traverse/relationship";
    }
    private String getTraverseUriPaths( Node node )
    {
        return getNodeUri( node) + "/traverse/path";
    }

    private String getTraverseUriNodes( Node node )
    {
        // TODO Auto-generated method stub
        return getNodeUri( node) + "/traverse/node";
    }

    @Test
    @Graph( "I know you" )
    public void shouldGetSomeHitsWhenTraversingWithDefaultDescription()
            throws JsonParseException
    {
        String entity = gen().expectedStatus( Status.OK.getStatusCode() ).payload( "{}" ).post(
                getTraverseUriNodes( getNode( "I" ) ) ).entity();

        expectNodes( entity, getNode( "you" ));
    }

    private void expectNodes( String entity, Node... nodes )
            throws JsonParseException
    {
        Set<String> expected = new HashSet<>();
        for ( Node node : nodes )
        {
            expected.add( getNodeUri( node ) );
        }
        Collection<?> items = (Collection<?>) readJson( entity );
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            String uri = (String) map.get( "self" );
            assertTrue( uri + " not found", expected.remove( uri ) );
        }
        assertTrue( "Expected not empty:" + expected, expected.isEmpty() );
    }

    @Documented( "Traversal using a return filter.\n" +
                 "\n" +
                 "In this example, the +none+ prune evaluator is used and a return filter\n" +
                 "is supplied in order to return all names containing \"t\".\n" +
                 "The result is to be returned as nodes and the max depth is\n" +
                 "set to 3." )
    @Graph( {"Root knows Mattias", "Root knows Johan", "Johan knows Emil", "Emil knows Peter", "Emil knows Tobias", "Tobias loves Sara"} )
    @Test
    public void shouldGetExpectedHitsWhenTraversingWithDescription()
            throws JsonParseException
    {
        Node start = getNode( "Root" );
        List<Map<String, Object>> rels = new ArrayList<>();
        rels.add( map( "type", "knows", "direction", "all" ) );
        rels.add( map( "type", "loves", "direction", "all" ) );
        String description = createJsonFrom( map(
                "order",
                "breadth_first",
                "uniqueness",
                "node_global",
                "prune_evaluator",
                map( "language", "javascript", "body", "position.length() > 10" ),
                "return_filter",
                map( "language", "javascript", "body",
                        "position.endNode().getProperty('name').toLowerCase().contains('t')" ),
                "relationships", rels, "max_depth", 3 ) );
        String entity = gen().expectedStatus( 200 ).payload( description ).post(
                getTraverseUriNodes( start ) ).entity();
        expectNodes( entity, getNodes( "Root", "Mattias", "Peter", "Tobias" ) );
    }

    @Documented( "Traversal returning nodes below a certain depth.\n" +
                 "\n" +
                 "Here, all nodes at a traversal depth below 3 are returned." )
    @Graph( {"Root knows Mattias", "Root knows Johan", "Johan knows Emil", "Emil knows Peter", "Emil knows Tobias", "Tobias loves Sara"} )
    @Test
    public void shouldGetExpectedHitsWhenTraversingAtDepth()
            throws JsonParseException
    {
        Node start = getNode( "Root" );
        String description = createJsonFrom( map(
                "prune_evaluator",
                map( "language", "builtin", "name", "none" ),
                "return_filter",
                map( "language", "javascript", "body",
                        "position.length()<3;" ) ) );
        String entity = gen().expectedStatus( 200 ).payload( description ).post(
                getTraverseUriNodes( start ) ).entity();
        expectNodes( entity, getNodes( "Root", "Mattias", "Johan", "Emil" ) );
    }

    @Test
    @Graph( "I know you" )
    public void shouldGet400WhenSupplyingInvalidTraverserDescriptionFormat()
    {
        gen().expectedStatus( Status.BAD_REQUEST.getStatusCode() ).payload(
                "::not JSON{[ at all" ).post(
                getTraverseUriNodes( getNode( "I" ) ) ).entity();
    }

    @Test
    @Graph( {"Root knows Mattias",
             "Root knows Johan",  "Johan knows Emil", "Emil knows Peter",
             "Root eats Cork",    "Cork hates Root",
             "Root likes Banana", "Banana is_a Fruit"} )
    public void shouldAllowTypeOrderedTraversals()
            throws JsonParseException
    {
        Node start = getNode( "Root" );
        String description = createJsonFrom( map(
                "expander", "order_by_type",
                "relationships",
                    new Map[]{
                        map( "type", "eats"),
                        map( "type", "knows" ),
                        map( "type", "likes" )
                    },
                "prune_evaluator",
                    map( "language", "builtin",
                         "name", "none" ),
                "return_filter",
                    map( "language", "javascript",
                         "body", "position.length()<2;" )
        ) );
        @SuppressWarnings( "unchecked" )
        List<Map<String,Object>> nodes = (List<Map<String, Object>>) readJson( gen().expectedStatus( 200 ).payload(
                description ).post(
                getTraverseUriNodes( start ) ).entity() );

        assertThat( nodes.size(), is( 5 ) );
        assertThat( getName( nodes.get( 0 ) ), is( "Root" ) );
        assertThat( getName( nodes.get( 1 ) ), is( "Cork" ) );

        // We don't really care about the ordering between Johan and Mattias, we just assert that they
        // both are there, in between Root/Cork and Banana
        Set<String> knowsNodes = new HashSet<>( Arrays.asList( "Johan", "Mattias" ) );
        assertTrue( knowsNodes.remove( getName( nodes.get( 2 ) ) ) );
        assertTrue( knowsNodes.remove( getName( nodes.get( 3 ) ) ) );

        assertThat( getName( nodes.get( 4 ) ), is( "Banana" ) );
    }

    @SuppressWarnings( "unchecked" )
    private String getName( Map<String, Object> propContainer )
    {
        return (String) ((Map<String,Object>)propContainer.get( "data" )).get( "name" );
    }
}
