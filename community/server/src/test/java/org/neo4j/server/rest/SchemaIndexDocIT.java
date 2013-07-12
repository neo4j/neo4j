/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.neo4j.graphdb.Neo4jMatchers;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription;

import static java.util.Arrays.asList;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class SchemaIndexDocIT extends AbstractRestFunctionalTestBase
{
    /**
     * Create schema index.
     *
     * This will start a background job in the database that will create and populate the new index.
     * You can check the status of your index by listing all the indexes for the relevant label.
     * The new index will show up, but have a state of "POPULATING" until the index is ready.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void create_schema_index() throws PropertyValueException
    {
        data.get();
        
        String labelName = "person", propertyKey = "name";
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        String result = gen.get()
            .expectedStatus( 200 )
            .payload( createJsonFrom( definition ) )
            .post( getSchemaIndexLabelUri( labelName ) )
            .entity();
        
        Map<String, Object> serialized = jsonToMap( result );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property-keys" ) );
    }
    
    /**
     * List indexes for a label.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_schema_indexes() throws PropertyValueException
    {
        data.get();
        
        String labelName = "user", propertyKey = "name";
        createSchemaIndex( labelName, propertyKey );
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        String result = gen.get()
            .expectedStatus( 200 )
            .payload( createJsonFrom( definition ) )
            .get( getSchemaIndexLabelUri( labelName ) )
            .entity();
        
        List<Map<String, Object>> serializedList = jsonToList( result );
        assertEquals( 1, serializedList.size() );
        Map<String, Object> serialized = serializedList.get( 0 );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property-keys" ) );
    }

    /**
     * Drop schema index
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void drop_schema_index() throws Exception
    {
        data.get();

        String labelName = "SomeLabel", propertyKey = "name";
        IndexDefinition schemaIndex = createSchemaIndex( labelName, propertyKey );
        assertThat( Neo4jMatchers.getIndexes( graphdb(), label( labelName ) ), containsOnly( schemaIndex ) );

        gen.get()
            .expectedStatus( 204 )
            .delete( getSchemaIndexLabelPropertyUri( labelName, propertyKey ) )
            .entity();

        assertThat( Neo4jMatchers.getIndexes( graphdb(), label( labelName ) ), not( containsOnly( schemaIndex ) ) );
    }
    
    /**
     * Create a schema index for a label and property key which already exists.
     */
    @Test
    public void create_existing_schema_index() throws PropertyValueException
    {
        String labelName = "mylabel", propertyKey = "name";
        createSchemaIndex( labelName, propertyKey );
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        gen.get()
            .expectedStatus( 409 )
            .payload( createJsonFrom( definition ) )
            .post( getSchemaIndexLabelUri( labelName ) );
    }
    
    @Test
    public void drop_non_existent_schema_index() throws Exception
    {
        // GIVEN
        String labelName = "ALabel", propertyKey = "name";

        // WHEN
        gen.get()
            .expectedStatus( 404 )
            .delete( getSchemaIndexLabelPropertyUri( labelName, propertyKey ) );
    }

    /**
     * Create a compound schema index should not yet be supported
     */
    @Test
    public void create_compound_schema_index() throws PropertyValueException
    {
        Map<String, Object> definition = map( "property_keys", asList( "first", "other" ) );

        gen.get()
                .expectedStatus( 400 )
                .payload( createJsonFrom( definition ) )
                .post( getSchemaIndexLabelUri( "a_label" ) );
    }
    
    private IndexDefinition createSchemaIndex( String labelName, String propertyKey )
    {
        Transaction tx = graphdb().beginTx();
        try
        {
            IndexDefinition indexDefinition = graphdb().schema().indexFor( label( labelName ) ).on( propertyKey )
                    .create();
            tx.success();
            return indexDefinition;
        }
        finally
        {
            tx.finish();
        }
    }

    private Set<Set<String>> asProperties( Iterable<IndexDefinition> indexes )
    {
        return asSet( map( new Function<IndexDefinition, Set<String>>()
        {
            @Override
            public Set<String> apply( IndexDefinition from )
            {
                return asSet( from.getPropertyKeys() );
            }
        }, indexes ) );
    }
}
