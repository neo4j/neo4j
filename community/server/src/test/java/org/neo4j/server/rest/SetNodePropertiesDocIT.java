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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class SetNodePropertiesDocIT extends
        AbstractRestFunctionalTestBase
{

    @Graph( "jim knows joe" )
    @Documented( "Update node properties.\n" +
                 "\n" +
                 "This will replace all existing properties on the node with the new set\n" +
                 "of attributes." )
    @Test
    public void shouldReturn204WhenPropertiesAreUpdated()
            throws JsonParseException
    {
        Node jim = data.get().get( "jim" );
        assertThat( jim, inTx(graphdb(), not( hasProperty( "age" ) ) ) );
        gen.get().payload(
                JsonHelper.createJsonFrom( MapUtil.map( "age", "18" ) ) ).expectedStatus(
                204 ).put( getPropertiesUri( jim ) );
        assertThat( jim, inTx(graphdb(), hasProperty( "age" ).withValue( "18" ) ) );
    }

    @Graph( "jim knows joe" )
    @Test
    public void set_node_properties_in_Unicode()
            throws JsonParseException
    {
        Node jim = data.get().get( "jim" );
        gen.get().payload(
                JsonHelper.createJsonFrom( MapUtil.map( "name", "\u4f8b\u5b50" ) ) ).expectedStatus(
                204 ).put( getPropertiesUri( jim ) );
        assertThat( jim, inTx( graphdb(), hasProperty( "name" ).withValue( "\u4f8b\u5b50" ) ) );
    }

    @Test
    @Graph( "jim knows joe" )
    public void shouldReturn400WhenSendinIncompatibleJsonProperties()
            throws JsonParseException
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", new HashMap<String, Object>() );
        gen.get().payload( JsonHelper.createJsonFrom( map ) ).expectedStatus(
                400 ).put( getPropertiesUri( data.get().get( "jim" ) ) );
    }

    @Test
    @Graph( "jim knows joe" )
    public void shouldReturn400WhenSendingCorruptJsonProperties()
    {
        JaxRsResponse response = RestRequest.req().put(
                getPropertiesUri( data.get().get( "jim" ) ),
                "this:::Is::notJSON}" );
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    @Test
    @Graph( "jim knows joe" )
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist()
            throws JsonParseException
    {
        gen.get().payload(
                JsonHelper.createJsonFrom( MapUtil.map( "key", "val" ) ) ).expectedStatus(
                404 ).put( getDataUri() + "node/12345/properties" );
    }

    private URI getPropertyUri( Node node, String key ) throws Exception
    {
        return new URI( getPropertiesUri( node ) + "/" + key );
    }

    @Documented( "Set property on node.\n" +
                 "\n" +
                 "Setting different properties will retain the existing ones for this node.\n" +
                 "Note that a single value are submitted not as a map but just as a value\n" +
                 "(which is valid JSON) like in the example\n" +
                 "below." )
    @Graph( nodes = {@NODE(name="jim", properties={@PROP(key="foo2", value="bar2")})} )
    @Test
    public void shouldReturn204WhenPropertyIsSet() throws Exception
    {
        Node jim = data.get().get( "jim" );
        gen.get().payload( JsonHelper.createJsonFrom( "bar" ) ).expectedStatus(
                204 ).put( getPropertyUri( jim, "foo" ).toString() );
        assertThat( jim, inTx(graphdb(), hasProperty( "foo" ) ) );
        assertThat( jim, inTx(graphdb(), hasProperty( "foo2" ) ) );
    }

    @Documented( "Property values can not be nested.\n" +
                 "\n" +
                 "Nesting properties is not supported. You could for example store the\n" +
                 "nested JSON as a string instead." )
    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperty()
            throws Exception
    {
        gen.get()
                .noGraph()
                .payload( "{\"foo\" : {\"bar\" : \"baz\"}}" )
                .expectedStatus(
                400 ).post( getDataUri() + "node/" );
    }

    @Test
    @Graph( "jim knows joe" )
    public void shouldReturn400WhenSendingCorruptJsonProperty()
            throws Exception
    {
        JaxRsResponse response = RestRequest.req().put(
                getPropertyUri( data.get().get( "jim" ), "foo" ),
                "this:::Is::notJSON}" );
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    @Test
    @Graph( "jim knows joe" )
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist()
            throws Exception
    {
        JaxRsResponse response = RestRequest.req().put(
                getDataUri() + "node/1234/foo",
                JsonHelper.createJsonFrom( "bar" ) );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

}
