/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;

public class TraverserFunctionalTest extends AbstractRestFunctionalTestBase
{


    @Test
    public void shouldGet404WhenTraversingFromNonExistentNode()
    {
        gen.get().expectedStatus( Status.NOT_FOUND.getStatusCode() ).payload(
                "{}" ).post( getDataUri() + "node/10000/traverse/node" ).entity();
    }

    @Test
    @Graph( nodes = {@NODE(name="I")} )
    public void shouldGet200WhenNoHitsFromTraversing()
            throws DatabaseBlockedException
    {
        assertSize( 0,gen.get().expectedStatus( 200 ).payload( "" ).post(
                getTraverseUriNodes( getNode( "I" ) ) ).entity());
    }
    
    /**
     * In order to return relationships,
     * simply specify the return type as part of the URL.
     */
    @Test
    @Graph( {"I know you", "I own car"} )
    public void return_relationships_from_a_traversal()
            throws DatabaseBlockedException
    {
        assertSize( 2, gen.get().expectedStatus( 200 ).payload( "{\"order\":\"breadth_first\",\"uniqueness\":\"none\",\"return_filter\":{\"language\":\"builtin\",\"name\":\"all\"}}" ).post(
                getTraverseUriRelationships( getNode( "I" ) ) ).entity());
    }

    
    /**
     * In order to return paths from a traversal,
     * specify the +Path+ return type as part of the URL.
     */
    @Test
    @Graph( {"I know you", "I own car"} )
    public void return_paths_from_a_traversal()
            throws DatabaseBlockedException
    {
        assertSize( 3, gen.get().expectedStatus( 200 ).payload( "{\"order\":\"breadth_first\",\"uniqueness\":\"none\",\"return_filter\":{\"language\":\"builtin\",\"name\":\"all\"}}" ).post(
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
            throws PropertyValueException
    {
        String entity = gen.get().expectedStatus( Status.OK.getStatusCode() ).payload( "{}" ).post(
                getTraverseUriNodes( getNode( "I" ) ) ).entity();

        expectNodes( entity, getNode( "you" ));
    }

    private void expectNodes( String entity, Node... nodes )
            throws PropertyValueException
    {
        Set<String> expected = new HashSet<String>();
        for ( Node node : nodes )
        {
            expected.add( getNodeUri( node ) );
        }
        Collection<?> items = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            String uri = (String) map.get( "self" );
            assertTrue( uri + " not found", expected.remove( uri ) );
        }
        assertTrue( "Expected not empty:" + expected, expected.isEmpty() );
    }

    /**
     * Traversal using a return filter.
     * 
     * In this example, the +none+ prune evaluator is used and a return filter
     * is supplied in order to return all names containing "t". 
     * The result is to be returned as nodes and the max depth is
     * set to 3.
     */
    @Documented
    @Graph( {"Root knows Mattias", "Root knows Johan", "Johan knows Emil", "Emil knows Peter", "Emil knows Tobias", "Tobias loves Sara"} )
    @Test
    public void shouldGetExpectedHitsWhenTraversingWithDescription()
            throws PropertyValueException
    {
        Node start = getNode( "Root" );
        List<Map<String, Object>> rels = new ArrayList<Map<String, Object>>();
        rels.add( MapUtil.map( "type", "knows", "direction", "all" ) );
        rels.add( MapUtil.map( "type", "loves", "direction", "all" ) );
        String description = JsonHelper.createJsonFrom( MapUtil.map(
                "order",
                "breadth_first",
                "uniqueness",
                "node_global",
                "prune_evaluator",
                MapUtil.map( "language", "javascript", "body", "position.length() > 10" ),
                "return_filter",
                MapUtil.map( "language", "javascript", "body",
                        "position.endNode().getProperty('name').toLowerCase().contains('t')" ),
                "relationships", rels, "max_depth", 3 ) );
        String entity = gen.get().expectedStatus( 200 ).payload( description ).post(
                getTraverseUriNodes( start ) ).entity();
        expectNodes( entity, getNodes( "Root", "Mattias", "Peter", "Tobias" ) );
    }
    /**
     * Traversal returning nodes below a certain depth.
     * 
     * Here, all nodes at a traversal depth below 3 are returned.
     */
    @Documented
    @Graph( {"Root knows Mattias", "Root knows Johan", "Johan knows Emil", "Emil knows Peter", "Emil knows Tobias", "Tobias loves Sara"} )
    @Test
    public void shouldGetExpectedHitsWhenTraversingAtDepth()
            throws PropertyValueException
    {
        Node start = getNode( "Root" );
        String description = JsonHelper.createJsonFrom( MapUtil.map(
                "prune_evaluator",
                MapUtil.map( "language", "builtin", "name", "none" ),
                "return_filter",
                MapUtil.map( "language", "javascript", "body",
                        "position.length()<3;" ) ) );
        String entity = gen.get().expectedStatus( 200 ).payload( description ).post(
                getTraverseUriNodes( start ) ).entity();
        expectNodes( entity, getNodes( "Root", "Mattias", "Johan", "Emil" ) );
    }

    @Test
    @Graph( "I know you" )
    public void shouldGet400WhenSupplyingInvalidTraverserDescriptionFormat()
            throws DatabaseBlockedException
    {
        gen.get().expectedStatus( Status.BAD_REQUEST.getStatusCode() ).payload(
                "::not JSON{[ at all" ).post(
                getTraverseUriNodes( getNode( "I" ) ) ).entity();
    }
}
