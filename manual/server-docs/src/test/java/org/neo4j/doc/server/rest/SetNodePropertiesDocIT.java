/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

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
        org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
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
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().put(
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
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().put(
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
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().put(
                getDataUri() + "node/1234/foo",
                JsonHelper.createJsonFrom( "bar" ) );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

}
