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
package org.neo4j.examples.server;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.examples.server.plugins.DepthTwo;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription.Graph;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DepthTwoDocIT extends AbstractPluginTestBase
{
    private static final String NODES_ON_DEPTH_TWO = "nodesOnDepthTwo";
    private static final String RELATIONSHIPS_ON_DEPTH_TWO = "relationshipsOnDepthTwo";
    private static final String PATHS_ON_DEPTH_TWO = "pathsOnDepthTwo";

    protected String getDocumentationSectionName()
    {
        return "rest-api";
    }

    @Test
    public void canFindExtension() throws Exception
    {
        long nodeId = helper.createNode( DynamicLabel.label( "test" ) );
        Map<String, Object> map = getNodeLevelPluginMetadata( DepthTwo.class, nodeId );
        assertTrue( map.keySet().size() > 0 );
    }

    @Documented( "Get nodes at depth two." )
    @Test
    @Graph( { "I know you", "you know him" } )
    public void shouldReturnAllNodesAtDepthTwoOnPost() throws JsonParseException
    {
        Node node = data.get().get( "I" );

        String uri = (String) getNodeLevelPluginMetadata( DepthTwo.class, node.getId() ).get( NODES_ON_DEPTH_TWO );

        String result = performPost( uri );
        List<Map<String, Object>> list = JsonHelper.jsonToList( result );
        assertThat( list, notNullValue() );
        assertThat( list.size(), equalTo( 1 ) );
        Map<String, Object> map = list.get( 0 );
        assertThat( map.get( "data" ).toString(), containsString( "him" ) );
    }

    @Documented( "Get relationships at depth two." )
    @Test
    @Graph( { "I know you", "you know him" } )
    public void shouldReturnAllRelationshipsAtDepthTwoOnPost() throws JsonParseException
    {
        Node node = data.get().get( "I" );

        String uri = (String) getNodeLevelPluginMetadata( DepthTwo.class, node.getId() ).get(
                RELATIONSHIPS_ON_DEPTH_TWO );

        String result = performPost( uri );
        List<Map<String, Object>> list = JsonHelper.jsonToList( result );
        assertThat( list, notNullValue() );
        assertThat( list.size(), equalTo( 1 ) );
        Map<String, Object> map = list.get( 0 );
        assertThat( map.get( "type" ).toString(), containsString( "know" ) );
    }

    @Documented( "Get paths at depth two." )
    @Test
    @Graph( { "I know you", "you know him" } )
    public void shouldReturnAllPathsAtDepthTwoOnPost() throws JsonParseException
    {
        Node node = data.get().get( "I" );

        String uri = (String) getNodeLevelPluginMetadata( DepthTwo.class, node.getId() ).get( PATHS_ON_DEPTH_TWO );

        String result = performPost( uri );
        List<Map<String, Object>> list = JsonHelper.jsonToList( result );
        assertThat( list, notNullValue() );
        assertThat( list.size(), equalTo( 1 ) );
        Map<String, Object> map = list.get( 0 );
        assertThat( (Integer) map.get( "length" ), equalTo( 2 ) );
    }
}
