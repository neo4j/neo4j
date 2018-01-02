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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.mockito.matcher.Neo4jMatchers;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;

public class SchemaIndexIT extends AbstractRestFunctionalTestBase
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

        String labelName = labels.newInstance();
        String propertyKey = properties.newInstance();
        Map<String,Object> definition = map( "property_keys", singletonList( propertyKey ) );

        String result = gen.get()
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
    public void get_indexes_for_label() throws Exception
    {
        data.get();

        String labelName = labels.newInstance();
        String propertyKey = properties.newInstance();
        createIndex( labelName, propertyKey );
        Map<String,Object> definition = map( "property_keys", singletonList( propertyKey ) );

        List<Map<String,Object>> serializedList = retryOnStillPopulating( () -> gen.get()
                                                                           .expectedStatus( 200 )
                                                                           .payload( createJsonFrom( definition ) )
                                                                           .get( getSchemaIndexLabelUri( labelName ) )
                                                                           .entity() );

        Map<String,Object> index = new HashMap<>();
        index.put( "label", labelName );
        index.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serializedList, hasItem( index ) );
    }

    private List<Map<String,Object>> retryOnStillPopulating( Callable<String> callable ) throws Exception
    {
        long endTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis( 1 );
        List<Map<String,Object>> serializedList;
        do
        {
            String result = callable.call();
            serializedList = jsonToList( result );
            if ( System.currentTimeMillis() > endTime )
            {
                fail( "Indexes didn't populate correctly, last result '" + result + "'" );
            }
        }
        while ( stillPopulating( serializedList ) );
        return serializedList;
    }

    private boolean stillPopulating( List<Map<String,Object>> serializedList )
    {
        // We've created an index. That HTTP call for creating the index will return
        // immediately and indexing continue in the background. Querying the index endpoint
        // while index is populating gives back additional information like population progress.
        // This test below will look at the response of a "get index" result and if still populating
        // then return true so that caller may retry the call later.
        for ( Map<String,Object> map : serializedList )
        {
            if ( map.containsKey( "population_progress" ) )
            {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings( "unchecked" )
    @Documented( "Get all indexes." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_indexes() throws Exception
    {
        data.get();

        String labelName1 = labels.newInstance();
        String propertyKey1 = properties.newInstance();
        String labelName2 = labels.newInstance();
        String propertyKey2 = properties.newInstance();
        createIndex( labelName1, propertyKey1 );
        createIndex( labelName2, propertyKey2 );

        List<Map<String,Object>> serializedList = retryOnStillPopulating(
                () -> gen.get().expectedStatus( 200 ).get( getSchemaIndexUri() ).entity() );

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

        String labelName = labels.newInstance();
        String propertyKey = properties.newInstance();
        IndexDefinition schemaIndex = createIndex( labelName, propertyKey );
        assertThat( Neo4jMatchers.getIndexes( graphdb(), label( labelName ) ), containsOnly( schemaIndex ) );

        gen.get()
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
        String labelName = labels.newInstance();
        String propertyKey = properties.newInstance();
        createIndex( labelName, propertyKey );
        Map<String,Object> definition = map( "property_keys", singletonList( propertyKey ) );

        gen.get()
                .expectedStatus( 409 )
                .payload( createJsonFrom( definition ) )
                .post( getSchemaIndexLabelUri( labelName ) );
    }

    @Test
    public void drop_non_existent_index() throws Exception
    {
        String labelName = labels.newInstance();
        String propertyKey = properties.newInstance();

        gen.get()
                .expectedStatus( 404 )
                .delete( getSchemaIndexLabelPropertyUri( labelName, propertyKey ) );
    }

    @Test
    public void create_compound_index()
    {
        Map<String,Object> definition = map( "property_keys", asList( properties.newInstance(), properties.newInstance()) );

        gen.get()
                .expectedStatus( 200 )
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
