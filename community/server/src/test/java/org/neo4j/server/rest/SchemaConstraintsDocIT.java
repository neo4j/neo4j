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

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.getConstraints;
import static org.neo4j.graphdb.Neo4jMatchers.isEmpty;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class SchemaConstraintsDocIT extends AbstractRestFunctionalTestBase
{
    /**
     * Create property uniqueness constraint.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void create_property_uniqueness_constraint() throws PropertyValueException
    {
        data.get();

        String labelName = "person", propertyKey = "name";
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        String result = gen.get().expectedStatus( 200 ).payload( createJsonFrom( definition ) ).post(
                getSchemaConstraintLabelUniquenessUri( labelName ) ).entity();

        Map<String, Object> serialized = jsonToMap( result );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized.get( "type" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property-keys" ) );
    }

    /**
     * Get a specific uniqueness constraints for a label and a property
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_label_uniqueness_property_constraint() throws PropertyValueException
    {
        data.get();

        String labelName = "user", propertyKey = "name";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey );

        String result = gen.get().expectedStatus( 200 ).get(
                getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );
        assertEquals( 1, serializedList.size() );
        Map<String, Object> serialized = serializedList.get( 0 );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized.get( "type" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property-keys" ) );
    }

    /**
     * Get all uniqueness constraints for a label
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_label_uniqueness_property_constraints() throws PropertyValueException
    {
        data.get();

        String labelName = "user", propertyKey1 = "name1", propertyKey2 = "name2";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey1 );
        createLabelUniquenessPropertyConstraint( labelName, propertyKey2 );

        String result = gen.get().expectedStatus( 200 ).get( getSchemaConstraintLabelUniquenessUri( labelName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        assertEquals( 2, serializedList.size() );

        Map<String, Object> serialized1 = serializedList.get( 0 );
        assertEquals( labelName, serialized1.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized1.get( "type" ) );
        List<String> keyList1 = (List<String>) serialized1.get( "property-keys" );

        Map<String, Object> serialized2 = serializedList.get( 1 );
        assertEquals( labelName, serialized2.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized2.get( "type" ) );
        List<String> keyList2 = (List<String>) serialized2.get( "property-keys" );

        assertEquals( asSet( asList( propertyKey1 ), asList( propertyKey2 ) ), asSet( keyList1, keyList2 ) );
    }

    /**
     * Get all constraints for a label
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_label_property_constraints() throws PropertyValueException
    {
        data.get();

        String labelName = "user", propertyKey1 = "name1", propertyKey2 = "name2";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey1 );
        createLabelUniquenessPropertyConstraint( labelName, propertyKey2 );

        String result = gen.get().expectedStatus( 200 ).get( getSchemaConstraintLabelUri( labelName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        assertEquals( 2, serializedList.size() );

        Map<String, Object> serialized1 = serializedList.get( 0 );
        assertEquals( labelName, serialized1.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized1.get( "type" ) );
        List<String> keyList1 = (List<String>) serialized1.get( "property-keys" );

        Map<String, Object> serialized2 = serializedList.get( 1 );
        assertEquals( labelName, serialized2.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized2.get( "type" ) );
        List<String> keyList2 = (List<String>) serialized2.get( "property-keys" );

        assertEquals( asSet( asList( propertyKey1 ), asList( propertyKey2 ) ), asSet( keyList1, keyList2 ) );
    }

    /**
     * Get all constraints
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_constraints() throws PropertyValueException
    {
        data.get();

        String labelName1 = "user", propertyKey1 = "name1";
        String labelName2 = "prog", propertyKey2 = "name2";
        createLabelUniquenessPropertyConstraint( labelName1, propertyKey1 );
        createLabelUniquenessPropertyConstraint( labelName2, propertyKey2 );

        String result = gen.get().expectedStatus( 200 ).get( getSchemaConstraintUri() ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        assertEquals( 2, serializedList.size() );

        Map<String, Object> serialized1 = serializedList.get( 0 );
        assertEquals( labelName1, serialized1.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized1.get( "type" ) );
        List<String> keyList1 = (List<String>) serialized1.get( "property-keys" );

        Map<String, Object> serialized2 = serializedList.get( 1 );
        assertEquals( labelName2, serialized2.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized2.get( "type" ) );
        List<String> keyList2 = (List<String>) serialized2.get( "property-keys" );

        assertEquals( asSet( asList( propertyKey1 ), asList( propertyKey2 ) ), asSet( keyList1, keyList2 ) );
    }

    /**
     * Drop uniqueness constraint for a label and a property
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void drop_constraint() throws Exception
    {
        data.get();

        String labelName = "SomeLabel", propertyKey = "name";
        ConstraintDefinition constraintDefinition = createLabelUniquenessPropertyConstraint( labelName,
                propertyKey );
        assertThat( getConstraints( graphdb(), label( labelName ) ), containsOnly( constraintDefinition ) );

        gen.get().expectedStatus( 204 ).delete( getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) ).entity();

        assertThat( getConstraints( graphdb(), label( labelName ) ), isEmpty() );
    }

    /**
     * Create a schema index for a label and property key which already exists.
     */
    @Test
    public void create_existing_constraint() throws PropertyValueException
    {
        String labelName = "mylabel", propertyKey = "name";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey );
    }

    @Test
    public void drop_non_existent_constraint() throws Exception
    {
        // GIVEN
        String labelName = "ALabel", propertyKey = "name";

        // WHEN
        gen.get().expectedStatus( 404 ).delete( getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) );
    }

    /**
     * Create a compound schema index should not yet be supported
     */
    @Test
    public void create_compound_schema_index() throws PropertyValueException
    {
        Map<String, Object> definition = map( "property_keys", asList( "first", "other" ) );

        gen.get().expectedStatus( 400 ).payload( createJsonFrom( definition ) ).post(
                getSchemaIndexLabelUri( "a_label" ) );
    }

    private ConstraintDefinition createLabelUniquenessPropertyConstraint( String labelName, String propertyKey )
    {
        Transaction tx = graphdb().beginTx();
        try
        {
            ConstraintDefinition constraintDefinition = graphdb().schema().constraintFor( label( labelName ) ).unique
                    ().on( propertyKey ).create();
            tx.success();
            return constraintDefinition;
        }
        finally
        {
            tx.finish();
        }
    }
}
