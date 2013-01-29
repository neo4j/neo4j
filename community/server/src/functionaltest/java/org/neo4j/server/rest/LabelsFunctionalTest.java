/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.LABEL;
import org.neo4j.test.GraphDescription.NODE;

public class LabelsFunctionalTest  extends AbstractRestFunctionalTestBase
{
    /**
     * Adding a label to a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", setNameProperty = true ) } )
    public void adding_a_label_to_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "I" ) );

        gen.get()
            .expectedStatus( 204 )
            .payload( createJsonFrom( "MyLabel" ) )
            .post( nodeUri + "/labels"  );
    }

    /**
     * Adding a label with an invalid name.
     *
     * Labels with empty names are not allowed, however, all other valid strings are accepted as label names.
     * Adding an invalid label to a node will lead to a HTTP 400 response.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", setNameProperty = true ) } )
    public void adding_an_invalid_label_to_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "I" ) );

        gen.get()
            .expectedStatus( 400 )
            .payload( createJsonFrom( "" ) )
            .post( nodeUri + "/labels"  );
    }
    
    /**
     * Listing labels for a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", labels = { @LABEL( "Me" ), @LABEL( "You" ) }, setNameProperty = true ) } )
    public void listing_node_labels() throws PropertyValueException
    {
        Map<String, Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "I" ) );

        String body = gen.get()
            .expectedStatus( 200 )
            .get( nodeUri + "/labels"  )
            .entity();
        List<String> labels = (List<String>) JsonHelper.readJson( body );
        assertEquals( asSet( "Me", "You" ), asSet( labels ) );
    }

    /**
     * Removing a label from a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", setNameProperty = true, labels = { @LABEL( "MyLabel" ) } ) } )
    public void removing_a_label_from_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        Node node = nodes.get( "I" );
        String nodeUri = getNodeUri( node );

        String labelName = "MyLabel";
        gen.get()
            .expectedStatus( 204 )
            .delete( nodeUri + "/labels/" + labelName );
        
        node.hasLabel( label( labelName ) );
    }

    /**
     * Removing a non-existent label from a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "I", setNameProperty = true ) } )
    public void removing_a_non_existent_label_from_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        Node node = nodes.get( "I" );
        String nodeUri = getNodeUri( node );

        String labelName = "MyLabel";
        gen.get()
            .expectedStatus( 204 )
            .delete( nodeUri + "/labels/" + labelName );
        
        node.hasLabel( label( labelName ) );
    }
    
    /**
     * Get all nodes with a label.
     * @throws JsonParseException
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {
            @NODE( name = "a", setNameProperty = true, labels = { @LABEL( "first" ), @LABEL( "second" ) } ),
            @NODE( name = "b", setNameProperty = true, labels = { @LABEL( "first" ) } ),
            @NODE( name = "c", setNameProperty = true, labels = { @LABEL( "second" ) } )
            } )
    public void get_all_nodes_with_label() throws JsonParseException
    {
        Map<String,Node> nodes = data.get();
        String uri = getNodesWithLabelUri( "first" );
        String body = gen.get()
            .expectedStatus( 200 )
            .get( uri )
            .entity();
        
        List<?> parsed = (List<?>) JsonHelper.readJson( body );
        assertEquals( asSet( "a", "b" ), asSet( map( new Function<Object, String>()
        {
            @Override
            public String apply( Object from )
            {
                Map<?,?> node = (Map<?, ?>) from;
                Map<?,?> data = (Map<?, ?>) node.get( "data" );
                return (String) data.get( "name" );
            }
        }, parsed ) ) );
    }
}
