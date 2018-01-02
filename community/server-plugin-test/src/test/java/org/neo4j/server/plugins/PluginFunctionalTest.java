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
package org.neo4j.server.plugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.MediaType;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.plugins.PluginFunctionalTestHelper.RegExp;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.NodeRepresentationTest;
import org.neo4j.server.rest.repr.RelationshipRepresentationTest;
import org.neo4j.test.server.SharedServerTestBase;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@SuppressWarnings( "unchecked" )
public class PluginFunctionalTest extends SharedServerTestBase
{
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( SharedServerTestBase.server() );
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( SharedServerTestBase.server() );
    }

    @Test
    public void canGetGraphDatabaseExtensionList() throws Exception
    {
        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.dataUri() );
        assertThat( map.get( "extensions" ), instanceOf( Map.class ) );
    }

    @Test
    public void canGetExtensionDefinitionForReferenceNodeExtension() throws Exception
    {
        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.dataUri() );
        map = (Map<String, Object>) map.get( "extensions" );

        assertThat( map.get( FunctionalTestPlugin.class.getSimpleName() ), instanceOf( Map.class ) );
    }

    @Test
    public void canGetExtensionDataForCreateNode() throws Exception
    {
        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.dataUri() );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );

        assertThat( (String) map.get( FunctionalTestPlugin.CREATE_NODE ), RegExp.endsWith( String.format(
                "/ext/%s/graphdb/%s", FunctionalTestPlugin.class.getSimpleName(),
                FunctionalTestPlugin.CREATE_NODE ) ) );
    }

    @Test
    public void canGetExtensionDescription() throws Exception
    {
        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.dataUri() );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );

        String uri = (String) map.get( FunctionalTestPlugin.CREATE_NODE );
        PluginFunctionalTestHelper.makeGet( uri );
    }

    @Test
    public void canInvokeExtensionMethodWithNoArguments() throws Exception
    {
        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.dataUri() );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );

        String uri = (String) map.get( FunctionalTestPlugin.CREATE_NODE );
        Map<String, Object> description = PluginFunctionalTestHelper.makePostMap( uri );

        NodeRepresentationTest.verifySerialisation( description );
    }

    @Test
    public void canInvokeNodePlugin() throws Exception
    {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();

        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.nodeUri( n ) );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );

        String uri = (String) map.get( FunctionalTestPlugin.GET_CONNECTED_NODES );
        List<Map<String, Object>> response = PluginFunctionalTestHelper.makePostList( uri );
        verifyNodes( response );
    }

    private void verifyNodes( final List<Map<String, Object>> response )
    {
        for ( Map<String, Object> nodeMap : response )
        {
            NodeRepresentationTest.verifySerialisation( nodeMap );
        }
    }

    @Test
    public void canInvokePluginWithParam() throws Exception
    {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();

        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.dataUri() );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );

        String uri = (String) map.get( "methodWithIntParam" );
        Map<String, Object> params = MapUtil.map( "id", n );
        Map<String, Object> node = PluginFunctionalTestHelper.makePostMap( uri, params );

        NodeRepresentationTest.verifySerialisation( node );
    }

    @Test
    public void canInvokePluginOnRelationship() throws Exception
    {
        long n1 = functionalTestHelper.getGraphDbHelper()
                .createNode();
        long n2 = functionalTestHelper.getGraphDbHelper()
                .createNode();
        long relId = functionalTestHelper.getGraphDbHelper()
                .createRelationship( "pals", n1, n2 );

        String uri = getPluginMethodUri( functionalTestHelper.relationshipUri( relId ), "methodOnRelationship" );

        Map<String, Object> params = MapUtil.map( "id", relId );
        List<Map<String, Object>> nodes = PluginFunctionalTestHelper.makePostList( uri, params );

        verifyNodes( nodes );
    }

    private String getPluginMethodUri( String startUrl, String methodName ) throws JsonParseException
    {
        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( startUrl );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );
        return (String) map.get( methodName );
    }

    @Test
    public void shouldBeAbleToInvokePluginWithLotsOfParams() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithAllParams" );
        String a = "a";
        byte b = (byte) 0xff;
        char c = 'c';
        short d = (short) 4;
        int e = 365;
        long f = (long) 4;
        float g = (float) 4.5;
        double h = Math.PI;
        boolean i = false;
        Map<String, Object> params = MapUtil.map( "id", a, "id2", b, "id3", c, "id4", d, "id5", e, "id6", f, "id7", g,
                "id8", h, "id9", i );

        PluginFunctionalTestHelper.makePostMap( methodUri, params );

        assertThat( FunctionalTestPlugin._string, is( a ) );
        assertThat( FunctionalTestPlugin._byte, is( b ) );
        assertThat( FunctionalTestPlugin._character, is( c ) );
        assertThat( FunctionalTestPlugin._short, is( d ) );
        assertThat( FunctionalTestPlugin._integer, is( e ) );
        assertThat( FunctionalTestPlugin._long, is( f ) );
        assertThat( FunctionalTestPlugin._float, is( g ) );
        assertThat( FunctionalTestPlugin._double, is( h ) );
        assertThat( FunctionalTestPlugin._boolean, is( i ) );
    }

    @Test
    public void shouldHandleOptionalValuesCorrectly1() throws Exception
    {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();
        String methodUri = getPluginMethodUri( functionalTestHelper.nodeUri( n ), "getThisNodeOrById" );
        Map<String, Object> map = PluginFunctionalTestHelper.makePostMap( methodUri );
        NodeRepresentationTest.verifySerialisation( map );
    }

    @Test
    public void shouldHandleOptionalValuesCorrectly2() throws Exception
    {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();
        String methodUri = getPluginMethodUri( functionalTestHelper.nodeUri( n ), "getThisNodeOrById" );
        long id = functionalTestHelper.getGraphDbHelper()
                .getFirstNode();
        Map<String, Object> params = MapUtil.map( "id", id );

        PluginFunctionalTestHelper.makePostMap( methodUri, params );

        assertThat( FunctionalTestPlugin.optional, is( id ) );
    }

    @Test
    public void canInvokePluginWithNodeParam() throws Exception
    {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();
        long m = functionalTestHelper.getGraphDbHelper()
                .createNode();
        functionalTestHelper.getGraphDbHelper()
                .createRelationship( "LOVES", n, m );
        functionalTestHelper.getGraphDbHelper()
                .createRelationship( "LOVES", m, n );
        functionalTestHelper.getGraphDbHelper()
                .createRelationship( "KNOWS", m, functionalTestHelper.getGraphDbHelper()
                        .createNode() );
        functionalTestHelper.getGraphDbHelper()
                .createRelationship( "KNOWS", n, functionalTestHelper.getGraphDbHelper()
                        .createNode() );

        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.nodeUri( n ) );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );

        String uri = (String) map.get( "getRelationshipsBetween" );
        List<Map<String, Object>> response = PluginFunctionalTestHelper.makePostList( uri,
                MapUtil.map( "other", functionalTestHelper.nodeUri( m ) ) );
        assertEquals( 2, response.size() );
        verifyRelationships( response );
    }

    @Test
    public void canInvokePluginWithNodeListParam() throws Exception
    {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();
        Map<String, Object> map = PluginFunctionalTestHelper.makeGet( functionalTestHelper.nodeUri( n ) );
        map = (Map<String, Object>) map.get( "extensions" );
        map = (Map<String, Object>) map.get( FunctionalTestPlugin.class.getSimpleName() );
        List<String> nodes = Arrays.asList( functionalTestHelper.nodeUri( functionalTestHelper.getGraphDbHelper()
                .createNode() ), functionalTestHelper.nodeUri( functionalTestHelper.getGraphDbHelper()
                .createNode() ), functionalTestHelper.nodeUri( functionalTestHelper.getGraphDbHelper()
                .createNode() ) );

        String uri = (String) map.get( "createRelationships" );
        List<Map<String, Object>> response = PluginFunctionalTestHelper.makePostList( uri,
                MapUtil.map( "type", "KNOWS", "nodes", nodes ) );
        assertEquals( nodes.size(), response.size() );
        verifyRelationships( response );
    }

    private void verifyRelationships( final List<Map<String, Object>> response )
    {
        for ( Map<String, Object> relMap : response )
        {
            RelationshipRepresentationTest.verifySerialisation( relMap );
        }
    }

    @Test
    public void shouldHandleSets() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithSet" );
        List<String> strings = Arrays.asList( "aaa", "bbb", "aaa" );
        Map<String, Object> params = MapUtil.map( "strings", strings );

        PluginFunctionalTestHelper.makePostMap( methodUri, params );

        Set<String> stringsSet = new HashSet<>( strings );

        assertThat( FunctionalTestPlugin.stringSet, is( stringsSet ) );
    }

    @Test
    public void shouldHandleJsonLists() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithList" );
        List<String> strings = Arrays.asList( "aaa", "bbb", "aaa" );
        Map<String, Object> params = MapUtil.map( "strings", strings );

        PluginFunctionalTestHelper.makePostMap( methodUri, params );

        List<String> stringsList = new ArrayList<>( strings );

        assertThat( FunctionalTestPlugin.stringList, is( stringsList ) );
    }

    @Test
    public void shouldHandleUrlEncodedLists() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithList" );

        String postBody = "strings[]=aaa&strings[]=bbb&strings[]=ccc";

        RestRequest.req().post( methodUri ,postBody,MediaType.APPLICATION_FORM_URLENCODED_TYPE);

        List<String> strings = Arrays.asList( "aaa", "bbb", "ccc" );

        List<String> stringsList = new ArrayList<>( strings );

        assertThat( FunctionalTestPlugin.stringList, is( stringsList ) );
    }

    @Test
    public void shouldHandleUrlEncodedListsAndInt() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithListAndInt" );

        String postBody = "strings[]=aaa&strings[]=bbb&strings[]=ccc&count=3";

        RestRequest.req().post(methodUri,postBody,MediaType.APPLICATION_FORM_URLENCODED_TYPE);

        List<String> strings = Arrays.asList( "aaa", "bbb", "ccc" );

        List<String> stringsList = new ArrayList<>( strings );

        assertThat( FunctionalTestPlugin.stringList, is( stringsList ) );
        assertThat( FunctionalTestPlugin._integer, is( 3 ) );
    }

    @Test
    public void shouldHandleArrays() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithArray" );
        String[] stringArray = { "aaa", "bbb", "aaa" };
        List<String> strings = Arrays.asList( stringArray );
        Map<String, Object> params = MapUtil.map( "strings", strings );

        PluginFunctionalTestHelper.makePostMap( methodUri, params );

        assertThat( FunctionalTestPlugin.stringArray, is( stringArray ) );
    }

    @Test
    public void shouldHandlePrimitiveArrays() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithIntArray" );
        Integer[] intArray = { 5, 6, 7, 8 };
        List<Integer> ints = Arrays.asList( intArray );
        Map<String, Object> params = MapUtil.map( "ints", ints );

        PluginFunctionalTestHelper.makePostMap( methodUri, params );

        assertThat( FunctionalTestPlugin.intArray, is( new int[] { 5, 6, 7, 8 } ) );
    }

    @Test
    public void shouldHandleOptionalArrays() throws Exception
    {
        String methodUri = getPluginMethodUri( functionalTestHelper.dataUri(), "methodWithOptionalArray" );

        PluginFunctionalTestHelper.makePostMap( methodUri );

        assertThat( FunctionalTestPlugin.intArray, is( nullValue() ) );
    }

    @Test
    public void shouldBeAbleToReturnPaths() throws Exception
    {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();
        long r = functionalTestHelper.getGraphDbHelper()
                .getFirstNode();
        functionalTestHelper.getGraphDbHelper()
                .createRelationship( "friend", n, r );

        String methodUri = getPluginMethodUri( functionalTestHelper.nodeUri( n ), "pathToReference" );

        Map<String, Object> maps = PluginFunctionalTestHelper.makePostMap( methodUri );

        assertThat( (String) maps.get( "start" ), endsWith( Long.toString( r ) ) );
        assertThat( (String) maps.get( "end" ), endsWith( Long.toString( n ) ) );
    }

    @Test
    public void shouldHandleNullPath() throws Exception {
        long n = functionalTestHelper.getGraphDbHelper()
                .createNode();

        String url = getPluginMethodUri(functionalTestHelper.nodeUri(n), "pathToReference");

        JaxRsResponse response = new RestRequest().post(url, null);

        assertThat( response.getEntity(), response.getStatus(), is(204) );
        response.close();
    }

}
