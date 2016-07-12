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

import org.neo4j.examples.server.plugins.ShortestPath;
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

public class ShortestPathDocIT extends AbstractPluginTestBase
{
    private static final String SHORTEST_PATHS = "shortestPath";

    protected String getDocumentationSectionName()
    {
        return "rest-api";
    }

    @Test
    public void canFindExtension() throws Exception
    {
        long nodeId = helper.createNode( DynamicLabel.label( "test" ) );
        Map<String, Object> map = getNodeLevelPluginMetadata( ShortestPath.class, nodeId );
        assertTrue( map.keySet().size() > 0 );
    }

    @Documented( "Get shortest path." )
    @Test
    @Graph( { "A knows B", "B knew C", "A knows D", "D knows E", "E knows C" } )
    public void shouldReturnAllShortestPathsOnPost() throws JsonParseException
    {
        Node source = data.get().get( "C" );
        String sourceUri = functionalTestHelper.nodeUri( source.getId() );
        Node target = data.get().get( "A" );        
        String targetUri = functionalTestHelper.nodeUri( target.getId() );
        
        String uri = (String) getNodeLevelPluginMetadata( ShortestPath.class, source.getId() ).get( SHORTEST_PATHS );
        String body = "{\"target\":\"" + targetUri + "\"}";

        String result = performPost( uri, body );
        List<Map<String, Object>> list = JsonHelper.jsonToList( result );
        assertThat( list, notNullValue() );
        assertThat( list.size(), equalTo( 1 ) );
        Map<String, Object> map = list.get( 0 );
        assertThat( map.get( "start" ).toString(), containsString( sourceUri ) );
        assertThat( map.get( "end" ).toString(), containsString( targetUri ) );
        assertThat( (Integer) map.get( "length" ), equalTo( 2 ) );
    }

    @Documented( "Get shortest path restricted by relationship type." )
    @Test
    @Graph( { "A knows B", "B likes C", "A knows D", "D knows E", "E knows C" } )
    public void shouldReturnAllShortestPathsRestrictedByReltypesOnPost() throws JsonParseException
    {
        Node source = data.get().get( "C" );
        String sourceUri = functionalTestHelper.nodeUri( source.getId() );
        Node target = data.get().get( "A" );        
        String targetUri = functionalTestHelper.nodeUri( target.getId() );
        
        String uri = (String) getNodeLevelPluginMetadata( ShortestPath.class, source.getId() ).get( SHORTEST_PATHS );

        String body = "{\"target\":\"" + targetUri + "\",\"types\":[\"knows\"]}";

        String result = performPost( uri, body );

        List<Map<String, Object>> list = JsonHelper.jsonToList( result );
        assertThat( list, notNullValue() );
        assertThat( list.size(), equalTo( 1 ) );
        Map<String, Object> map = list.get( 0 );
        assertThat( map.get( "start" ).toString(), containsString( sourceUri ) );
        assertThat( map.get( "end" ).toString(), containsString( targetUri ) );
        assertThat( (Integer) map.get( "length" ), equalTo( 3 ) );
    }
}
