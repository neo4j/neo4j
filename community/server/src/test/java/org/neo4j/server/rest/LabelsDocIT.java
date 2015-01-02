/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.LABEL;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.hasLabel;
import static org.neo4j.graphdb.Neo4jMatchers.hasLabels;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;
import static org.neo4j.test.GraphDescription.PropType.ARRAY;
import static org.neo4j.test.GraphDescription.PropType.STRING;

public class LabelsDocIT extends AbstractRestFunctionalTestBase
{

    /**
     * Adding a label to a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "Clint Eastwood", setNameProperty = true ) } )
    public void adding_a_label_to_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Clint Eastwood" ) );

        gen.get()
            .expectedStatus( 204 )
            .payload( createJsonFrom( "Person" ) )
            .post( nodeUri + "/labels"  );
    }

    /**
     * Adding multiple labels to a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "Clint Eastwood", setNameProperty = true ) } )
    public void adding_multiple_labels_to_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Clint Eastwood" ) );

        gen.get()
                .expectedStatus( 204 )
                .payload( createJsonFrom( new String[]{"Person", "Actor"} ) )
                .post( nodeUri + "/labels"  );

        // Then
        assertThat( nodes.get( "Clint Eastwood" ), inTx( graphdb(), hasLabels( "Person", "Actor" ) ) );
    }

    /**
     * Adding a label with an invalid name.
     *
     * Labels with empty names are not allowed, however, all other valid strings are accepted as label names.
     * Adding an invalid label to a node will lead to a HTTP 400 response.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "Clint Eastwood", setNameProperty = true ) } )
    public void adding_an_invalid_label_to_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Clint Eastwood" ) );

        gen.get()
            .expectedStatus( 400 )
            .payload( createJsonFrom( "" ) )
            .post( nodeUri + "/labels"  );
    }

    /**
     * Replacing labels on a node.
     *
     * This removes any labels currently on a node, and replaces them with the labels passed in as the
     * request body.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "Clint Eastwood", setNameProperty = true,
                                              labels = { @LABEL( "Person" ) }) } )
    public void replacing_labels_on_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Clint Eastwood" ) );

        // When
        gen.get()
                .expectedStatus( 204 )
                .payload( createJsonFrom( new String[]{"Actor", "Director"}) )
                .put( nodeUri + "/labels" );

        // Then
        assertThat( nodes.get( "Clint Eastwood" ), inTx(graphdb(), hasLabels("Actor", "Director")) );
    }

    /**
     * Listing labels for a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "Clint Eastwood", labels = { @LABEL( "Actor" ), @LABEL( "Director" ) }, setNameProperty = true ) } )
    public void listing_node_labels() throws PropertyValueException
    {
        Map<String, Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Clint Eastwood" ) );

        String body = gen.get()
            .expectedStatus( 200 )
            .get( nodeUri + "/labels"  )
            .entity();
        @SuppressWarnings("unchecked")
        List<String> labels = (List<String>) readJson( body );
        assertEquals( asSet( "Actor", "Director" ), asSet( labels ) );
    }

    /**
     * Removing a label from a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "Clint Eastwood", setNameProperty = true, labels = { @LABEL( "Person" ) } ) } )
    public void removing_a_label_from_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        Node node = nodes.get( "Clint Eastwood" );
        String nodeUri = getNodeUri( node );

        String labelName = "Person";
        gen.get()
            .expectedStatus( 204 )
            .delete( nodeUri + "/labels/" + labelName );

        assertThat( node, inTx( graphdb(), not( hasLabel( label( labelName ) ) ) ) );
    }

    /**
     * Removing a non-existent label from a node.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = { @NODE( name = "Clint Eastwood", setNameProperty = true ) } )
    public void removing_a_non_existent_label_from_a_node() throws PropertyValueException
    {
        Map<String,Node> nodes = data.get();
        Node node = nodes.get( "Clint Eastwood" );
        String nodeUri = getNodeUri( node );

        String labelName = "Person";
        gen.get()
            .expectedStatus( 204 )
            .delete( nodeUri + "/labels/" + labelName );

        assertThat( node, inTx( graphdb(), not( hasLabel( label( labelName ) ) ) ) );
    }
    
    /**
     * Get all nodes with a label.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {
            @NODE( name = "Clint Eastwood", setNameProperty = true, labels = { @LABEL( "Actor" ), @LABEL( "Director" ) } ),
            @NODE( name = "Donald Sutherland", setNameProperty = true, labels = { @LABEL( "Actor" ) } ),
            @NODE( name = "Steven Spielberg", setNameProperty = true, labels = { @LABEL( "Director" ) } )
            } )
    public void get_all_nodes_with_label() throws JsonParseException
    {
        data.get();
        String uri = getNodesWithLabelUri( "Actor" );
        String body = gen.get()
            .expectedStatus( 200 )
            .get( uri )
            .entity();
        
        List<?> parsed = (List<?>) readJson( body );
        assertEquals( asSet( "Clint Eastwood", "Donald Sutherland" ), asSet( map( getProperty( "name", String.class ), parsed ) ) );
    }

    /**
     * Get nodes by label and property.
     *
     * You can retrieve all nodes with a given label and property by passing one property as a query parameter.
     * Notice that the property value is JSON-encoded and then URL-encoded.
     *
     * If there is an index available on the label/property combination you send, that index will be used. If no
     * index is available, all nodes with the given label will be filtered through to find matching nodes.
     *
     * Currently, it is not possible to search using multiple properties.
     */
    @Test
    @Documented
    @GraphDescription.Graph( nodes = {
            @NODE( name = "Donald Sutherland",   labels={ @LABEL( "Person" )} ),
            @NODE( name = "Clint Eastwood", labels={ @LABEL( "Person" )}, properties = { @PROP( key = "name", value = "Clint Eastwood" )}),
            @NODE( name = "Steven Spielberg", labels={ @LABEL( "Person" )}, properties = { @PROP( key = "name", value = "Steven Spielberg" )})})
    public void get_nodes_with_label_and_property() throws PropertyValueException, UnsupportedEncodingException
    {
        data.get();

        String labelName = "Person";

        String result = gen.get()
                .expectedStatus( 200 )
                .get( getNodesWithLabelAndPropertyUri( labelName, "name", "Clint Eastwood" ) )
                .entity();

        List<?> parsed = (List<?>) readJson( result );
        assertEquals( asSet( "Clint Eastwood" ), asSet( map( getProperty( "name", String.class ), parsed ) ) );
    }

    /**
     * Get nodes by label and array property.
     */
    @Test
    @GraphDescription.Graph( nodes = {
            @NODE(name = "Donald Sutherland", labels = {@LABEL("Person")}),
            @NODE(name = "Clint Eastwood", labels = {@LABEL("Person")}, properties =
                    {@PROP(key = "names", value = "Clint,Eastwood", type = ARRAY, componentType = STRING)}),
            @NODE(name = "Steven Spielberg", labels = {@LABEL("Person")}, properties =
                    {@PROP(key = "names", value = "Steven,Spielberg", type = ARRAY, componentType = STRING)})})
    public void get_nodes_with_label_and_array_property() throws PropertyValueException, UnsupportedEncodingException
    {
        data.get();

        String labelName = "Person";

        String uri = getNodesWithLabelAndPropertyUri( labelName, "names", new String[] { "Clint", "Eastwood" } );

        String result = gen.get()
                .expectedStatus( 200 )
                .get( uri )
                .entity();

        List<?> parsed = (List<?>) readJson( result );
        assertEquals( 1, parsed.size() );

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals( asSet( asList( asList( "Clint", "Eastwood" ) ) ),
                asSet( map( getProperty( "names", List.class ), parsed ) ) );
    }

    /**
     * List all labels.
     */
    @Test
    @Documented
    @GraphDescription.Graph( nodes = {
            @NODE( name = "Clint Eastwood", setNameProperty = true, labels = { @LABEL( "Person" ), @LABEL( "Actor" ), @LABEL( "Director" ) } ),
            @NODE( name = "Donald Sutherland", setNameProperty = true, labels = { @LABEL( "Person" ), @LABEL( "Actor" ) } ),
            @NODE( name = "Steven Spielberg", setNameProperty = true, labels = { @LABEL( "Person" ), @LABEL( "Director" ) } )
    } )
    public void list_all_labels() throws JsonParseException
    {
        data.get();
        String uri = getLabelsUri();
        String body = gen.get()
                .expectedStatus( 200 )
                .get( uri )
                .entity();

        Set<?> parsed = asSet((List<?>) readJson( body ));
        assertTrue( parsed.contains( "Person" ) );
        assertTrue( parsed.contains( "Actor" ) );
        assertTrue( parsed.contains( "Director" ) );
    }

    private <T> Function<Object, T> getProperty( final String propertyKey, final Class<T> propertyType )
    {
        return new Function<Object, T>()
        {
            @Override
            public T apply( Object from )
            {
                Map<?, ?> node = (Map<?, ?>) from;
                Map<?, ?> data = (Map<?, ?>) node.get( "data" );
                return propertyType.cast( data.get( propertyKey ) );
            }
        };
    }
}
