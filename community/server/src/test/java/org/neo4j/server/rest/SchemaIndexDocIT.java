/*
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

import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Neo4jMatchers;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class SchemaIndexDocIT extends AbstractRestFunctionalTestBase
{
    @Before
    public void setup()
    {
        cleanDatabase();
    }

    /**
     * Create index.
     *
     * This will start a background job in the database that will create and populate the index.
     * You can check the status of your index by listing all the indexes for the relevant label.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void create_index() throws JsonParseException
    {
        data.get();

        String labelName = "person", propertyKey = "name";
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        String result = gen.get()
            .noGraph()
            .expectedStatus( 200 )
            .payload( createJsonFrom( definition ) )
            .post( getSchemaIndexLabelUri( labelName ) )
            .entity();

        Map<String, Object> serialized = jsonToMap( result );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property_keys" ) );
    }

    /**
     * List indexes for a label.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_indexes_for_label() throws JsonParseException
    {
        data.get();

        String labelName = "user", propertyKey = "name";
        createIndex( labelName, propertyKey );
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        String result = gen.get()
            .noGraph()
            .expectedStatus( 200 )
            .payload( createJsonFrom( definition ) )
            .get( getSchemaIndexLabelUri( labelName ) )
            .entity();

        List<Map<String, Object>> serializedList = jsonToList( result );
        assertEquals( 1, serializedList.size() );
        Map<String, Object> serialized = serializedList.get( 0 );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property_keys" ) );
    }



    /**
     * Get all indexes.
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_indexes() throws JsonParseException
    {
        data.get();

        String labelName1 = "user", propertyKey1 = "name1";
        String labelName2 = "prog", propertyKey2 = "name2";
        createIndex( labelName1, propertyKey1 );
        createIndex( labelName2, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaIndexUri() ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        assertEquals( 2, serializedList.size() );

        Set<String> labelNames = new HashSet<>();
        Set<List<String>> propertyKeys = new HashSet<>();

        Map<String, Object> serialized1 = serializedList.get( 0 );
        labelNames.add( (String) serialized1.get( "label" ) );
        propertyKeys.add( (List<String>) serialized1.get( "property_keys" ) );

        Map<String, Object> serialized2 = serializedList.get( 1 );
        labelNames.add( (String) serialized2.get( "label" ) );
        propertyKeys.add( (List<String>) serialized2.get( "property_keys" ) );

        assertEquals( asSet( labelName1, labelName2 ), labelNames );
        assertEquals( asSet( asList( propertyKey1 ), asList( propertyKey2 ) ), propertyKeys );
    }

    /**
     * Drop index
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void drop_index() throws Exception
    {
        data.get();

        String labelName = "SomeLabel", propertyKey = "name";
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
        String labelName = "mylabel", propertyKey = "name";
        createIndex( labelName, propertyKey );
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        gen.get()
            .noGraph()
            .expectedStatus( 409 )
            .payload( createJsonFrom( definition ) )
            .post( getSchemaIndexLabelUri( labelName ) );
    }

    @Test
    public void drop_non_existent_index() throws Exception
    {
        // GIVEN
        String labelName = "ALabel", propertyKey = "name";

        // WHEN
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
        Map<String, Object> definition = map( "property_keys", asList( "first", "other" ) );

        gen.get()
                .noGraph()
                .expectedStatus( 400 )
                .payload( createJsonFrom( definition ) )
                .post( getSchemaIndexLabelUri( "a_label" ) );
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
}
