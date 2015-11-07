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

import org.neo4j.examples.server.plugins.GetAll;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class GetAllDocIT extends AbstractPluginTestBase
{
    private static final String GET_ALL_RELATIONSHIPS = "getAllRelationships";
    private static final String GET_ALL_NODES = "get_all_nodes";

    protected String getDocumentationSectionName()
    {
        return "rest-api";
    }

    @Test
    public void testName() throws Exception
    {
        Map<String, Object> map = getDatabaseLevelPluginMetadata( GetAll.class );
        assertFalse( map.isEmpty() );
    }

    @Documented( "Get all nodes." )
    @Test
    public void shouldReturnAllNodesOnPost() throws JsonParseException
    {
        int numberOfNodes = helper.getNumberOfNodes();
        helper.createNode( DynamicLabel.label( "test" ) );

        String uri = (String) getDatabaseLevelPluginMetadata( GetAll.class ).get( GET_ALL_NODES );

        String result = performPost( uri );
        List<Map<String, Object>> list = JsonHelper.jsonToList( result );
        assertThat( list, notNullValue() );
        assertThat( list.size(), equalTo( numberOfNodes + 1 ) );
        Map<String, Object> map = list.get( 0 );
        assertThat( map.get( "data" ), notNullValue() );
    }

    @Test
    public void canGetExtensionDataForGetAllNodes() throws Exception
    {
        checkDatabaseLevelExtensionMetadata( GetAll.class, GET_ALL_NODES, "/ext/%s/graphdb/%s" );
    }

    @Documented( "Get all relationships." )
    @Test
    public void shouldReturnAllRelationshipsOnPost() throws JsonParseException
    {
        int numberOfRelationships = helper.getNumberOfRelationships();
        helper.createRelationship( "test" );

        String uri = (String) getDatabaseLevelPluginMetadata( GetAll.class ).get( GET_ALL_RELATIONSHIPS );

        String result = performPost( uri );
        List<Map<String, Object>> list = JsonHelper.jsonToList( result );
        assertThat( list, notNullValue() );
        assertThat( list.size(), equalTo( numberOfRelationships + 1 ) );
        Map<String, Object> map = list.get( 0 );
        assertThat( map.get( "data" ), notNullValue() );
    }

    @Test
    public void canGetExtensionDataForGetAllRelationships() throws Exception
    {
        checkDatabaseLevelExtensionMetadata( GetAll.class, GET_ALL_RELATIONSHIPS, "/ext/%s/graphdb/%s" );
    }
}
