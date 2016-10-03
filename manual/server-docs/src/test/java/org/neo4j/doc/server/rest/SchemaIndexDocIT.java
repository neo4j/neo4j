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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.Neo4jMatchers;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class SchemaIndexDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
{
    @Documented( "Create index.\n" +
                 "\n" +
                 "This will start a background job in the database that will create and populate the index.\n" +
                 "You can check the status of your index by listing all the indexes for the relevant label." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void create_index() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        Map<String,Object> definition = map( "property_keys", singletonList( propertyKey ) );

        String result = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( createJsonFrom( definition ) )
                .post( getSchemaIndexLabelUri( labelName ) )
                .entity();

        Map<String,Object> serialized = jsonToMap( result );


        Map<String,Object> index = new HashMap<>();
        index.put( "label", labelName );
        index.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serialized, equalTo( index ) );
    }

    @Documented( "List indexes for a label." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_indexes_for_label() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        createIndex( labelName, propertyKey );
        Map<String,Object> definition = map( "property_keys", singletonList( propertyKey ) );

        String result = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( createJsonFrom( definition ) )
                .get( getSchemaIndexLabelUri( labelName ) )
                .entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> index = new HashMap<>();
        index.put( "label", labelName );
        index.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serializedList, hasItem( index ) );
    }

    @SuppressWarnings( "unchecked" )
    @Documented( "Get all indexes." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_indexes() throws JsonParseException
    {
        data.get();

        String labelName1 = labels.newInstance(), propertyKey1 = properties.newInstance();
        String labelName2 = labels.newInstance(), propertyKey2 = properties.newInstance();
        createIndex( labelName1, propertyKey1 );
        createIndex( labelName2, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaIndexUri() ).entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> index1 = new HashMap<>();
        index1.put( "label", labelName1 );
        index1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String,Object> index2 = new HashMap<>();
        index2.put( "label", labelName2 );
        index2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( index1, index2 ) );
    }

    @Documented( "Drop index" )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void drop_index() throws Exception
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        IndexDefinition schemaIndex = createIndex( labelName, propertyKey );
        assertThat( Neo4jMatchers.getIndexes( graphdb(), label( labelName ) ), containsOnly( schemaIndex ) );

        gen.get()
                .noGraph()
                .expectedStatus( 204 )
                .delete( getSchemaIndexLabelPropertyUri( labelName, propertyKey ) )
                .entity();

        assertThat( Neo4jMatchers.getIndexes( graphdb(), label( labelName ) ), not( containsOnly( schemaIndex ) ) );
    }

    /**
     * Create an index for a label and property key which already exists.
     */
    @Test
    public void create_existing_index()
    {
        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        createIndex( labelName, propertyKey );
        Map<String,Object> definition = map( "property_keys", singletonList( propertyKey ) );

        gen.get()
                .noGraph()
                .expectedStatus( 409 )
                .payload( createJsonFrom( definition ) )
                .post( getSchemaIndexLabelUri( labelName ) );
    }

    @Test
    public void drop_non_existent_index() throws Exception
    {
        String labelName = labels.newInstance(), propertyKey = properties.newInstance();

        gen.get()
                .expectedStatus( 404 )
                .delete( getSchemaIndexLabelPropertyUri( labelName, propertyKey ) );
    }

    /**
     * Creating a compound index should not yet be supported
     */
    @Test
    public void create_compound_index()
    {
        Map<String,Object> definition = map( "property_keys", asList( properties.newInstance(), properties.newInstance()) );

        gen.get()
                .noGraph()
                .expectedStatus( 400 )
                .payload( createJsonFrom( definition ) )
                .post( getSchemaIndexLabelUri( labels.newInstance() ) );
    }

    private IndexDefinition createIndex( String labelName, String propertyKey )
    {
        try ( Transaction tx = graphdb().beginTx() )
        {
            IndexDefinition indexDefinition = graphdb().schema().indexFor( label( labelName ) ).on( propertyKey )
                    .create();
            tx.success();
            return indexDefinition;
        }
    }

    private final Factory<String> labels =  UniqueStrings.withPrefix( "label" );
    private final Factory<String> properties =  UniqueStrings.withPrefix( "property" );
}
