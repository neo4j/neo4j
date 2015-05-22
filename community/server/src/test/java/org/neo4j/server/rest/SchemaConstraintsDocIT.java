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

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
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
    @Before
    public void setup()
    {
        cleanDatabase();
    }

    /**
     * Create uniqueness constraint.
     * Create a uniqueness constraint on a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void createPropertyUniquenessConstraint() throws JsonParseException
    {
        data.get();

        String labelName = "Person", propertyKey = "name";
        Map<String, Object> definition = map( "property_keys", asList( propertyKey ) );

        String result = gen.get().noGraph().expectedStatus( 200 ).payload( createJsonFrom( definition ) ).post(
                getSchemaConstraintLabelUniquenessUri( labelName ) ).entity();

        Map<String, Object> serialized = jsonToMap( result );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized.get( "type" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property_keys" ) );
    }

    /**
     * Get a specific uniqueness constraint.
     * Get a specific uniqueness constraint for a label and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelUniquenessPropertyConstraint() throws JsonParseException
    {
        data.get();

        String labelName = "User", propertyKey = "name";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey );

        String result = gen.get().noGraph().expectedStatus( 200 ).get(
                getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );
        assertEquals( 1, serializedList.size() );
        Map<String, Object> serialized = serializedList.get( 0 );
        assertEquals( labelName, serialized.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized.get( "type" ) );
        assertEquals( asList( propertyKey ), serialized.get( "property_keys" ) );
    }

    /**
     * Get all uniqueness constraints for a label.
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelUniquenessPropertyConstraints() throws JsonParseException
    {
        data.get();

        String labelName = "User", propertyKey1 = "name1", propertyKey2 = "name2";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey1 );
        createLabelUniquenessPropertyConstraint( labelName, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaConstraintLabelUniquenessUri( labelName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        assertEquals( 2, serializedList.size() );

        Map<String, Object> serialized1 = serializedList.get( 0 );
        assertEquals( labelName, serialized1.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized1.get( "type" ) );
        List<String> keyList1 = (List<String>) serialized1.get( "property_keys" );

        Map<String, Object> serialized2 = serializedList.get( 1 );
        assertEquals( labelName, serialized2.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized2.get( "type" ) );
        List<String> keyList2 = (List<String>) serialized2.get( "property_keys" );

        assertEquals( asSet( asList( propertyKey1 ), asList( propertyKey2 ) ), asSet( keyList1, keyList2 ) );
    }

    /**
     * Get all constraints for a label.
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelPropertyConstraints() throws JsonParseException
    {
        data.get();

        String labelName = "User", propertyKey1 = "name1", propertyKey2 = "name2";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey1 );
        createLabelUniquenessPropertyConstraint( labelName, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaConstraintLabelUri( labelName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        assertEquals( 2, serializedList.size() );

        Map<String, Object> serialized1 = serializedList.get( 0 );
        assertEquals( labelName, serialized1.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized1.get( "type" ) );
        List<String> keyList1 = (List<String>) serialized1.get( "property_keys" );

        Map<String, Object> serialized2 = serializedList.get( 1 );
        assertEquals( labelName, serialized2.get( "label" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized2.get( "type" ) );
        List<String> keyList2 = (List<String>) serialized2.get( "property_keys" );

        assertEquals( asSet( asList( propertyKey1 ), asList( propertyKey2 ) ), asSet( keyList1, keyList2 ) );
    }

    /**
     * Get all constraints.
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_constraints() throws JsonParseException
    {
        data.get();

        String labelName1 = "User", propertyKey1 = "name1";
        String labelName2 = "Prog", propertyKey2 = "name2";
        createLabelUniquenessPropertyConstraint( labelName1, propertyKey1 );
        createLabelUniquenessPropertyConstraint( labelName2, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaConstraintUri() ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        assertEquals( 2, serializedList.size() );

        Set<String> labelNames = new HashSet<>();
        Set<List<String>> propertyKeys = new HashSet<>();

        Map<String, Object> serialized1 = serializedList.get( 0 );
        labelNames.add( (String) serialized1.get( "label" ) );
        propertyKeys.add( (List<String>) serialized1.get( "property_keys" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized1.get( "type" ) );

        Map<String, Object> serialized2 = serializedList.get( 1 );
        labelNames.add( (String) serialized2.get( "label" ) );
        propertyKeys.add( (List<String>) serialized2.get( "property_keys" ) );
        assertEquals( ConstraintType.UNIQUENESS.name(), serialized2.get( "type" ) );

        assertEquals( asSet( labelName1, labelName2 ), labelNames );
        assertEquals( asSet( asList( propertyKey1 ), asList( propertyKey2 ) ), propertyKeys );
    }

    /**
     * Drop constraint.
     * Drop uniqueness constraint for a label and a property.
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

        gen.get().noGraph().expectedStatus( 204 ).delete( getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) ).entity();

        assertThat( getConstraints( graphdb(), label( labelName ) ), isEmpty() );
    }

    /**
     * Create an index for a label and property key which already exists.
     */
    @Test
    public void create_existing_constraint()
    {
        String labelName = "Mylabel", propertyKey = "name";
        createLabelUniquenessPropertyConstraint( labelName, propertyKey );
    }

    @Test
    public void drop_non_existent_constraint() throws Exception
    {
        // GIVEN
        String labelName = "ALabel", propertyKey = "name";

        // WHEN
        gen.get().noGraph().expectedStatus( 404 ).delete( getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) );
    }

    /**
     * Creating a compound index should not yet be supported.
     */
    @Test
    public void create_compound_schema_index()
    {
        Map<String, Object> definition = map( "property_keys", asList( "first", "other" ) );

        gen.get().noGraph().expectedStatus( 400 ).payload( createJsonFrom( definition ) ).post(
                getSchemaIndexLabelUri( "a_label" ) );
    }

    private ConstraintDefinition createLabelUniquenessPropertyConstraint( String labelName, String propertyKey )
    {
        try ( Transaction tx = graphdb().beginTx() )
        {
            ConstraintDefinition constraintDefinition = graphdb().schema().constraintFor( label( labelName ) )
                    .assertPropertyIsUnique( propertyKey ).create();
            tx.success();
            return constraintDefinition;
        }
    }
}
