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

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.getConstraints;
import static org.neo4j.graphdb.Neo4jMatchers.isEmpty;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class SchemaConstraintsDocIT extends AbstractRestFunctionalTestBase
{
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

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        Map<String, Object> definition = map( "property_keys", singletonList( propertyKey ) );

        String result = gen.get().noGraph().expectedStatus( 200 ).payload( createJsonFrom( definition ) ).post(
                getSchemaConstraintLabelUniquenessUri( labelName ) ).entity();

        Map<String, Object> serialized = jsonToMap( result );

        Map<String, Object> constraint = new HashMap<>(  );
        constraint.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint.put( "label", labelName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serialized, equalTo( constraint ) );
    }

    /**
     * Create node property existence constraint.
     * Create a node property existence constraint for a label and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void createNodePropertyExistenceConstraint() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        Map<String, Object> definition = map( "property_keys", singletonList( propertyKey ) );

        String result = gen.get().noGraph().expectedStatus( 200 ).payload( createJsonFrom( definition ) ).post(
                getSchemaConstraintLabelExistenceUri( labelName ) ).entity();

        Map<String, Object> serialized = jsonToMap( result );

        Map<String, Object> constraint = new HashMap<>(  );
        constraint.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint.put( "label", labelName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serialized, equalTo( constraint ) );
    }

    /**
     * Create relationship property existence constraint.
     * Create a relationship property existence constraint for a relationship type and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void createRelationshipPropertyExistenceConstraint() throws JsonParseException
    {
        data.get();

        String relationshipTypeName = relationshipTypes.newInstance(), propertyKey = properties.newInstance();
        Map<String, Object> definition = map( "property_keys", singletonList( propertyKey ) );

        String result = gen.get().noGraph().expectedStatus( 200 ).payload( createJsonFrom( definition ) ).post(
                getSchemaRelationshipConstraintTypeExistenceUri( relationshipTypeName ) ).entity();

        Map<String, Object> serialized = jsonToMap( result );

        Map<String, Object> constraint = new HashMap<>(  );
        constraint.put( "type", ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE.name() );
        constraint.put( "relationshipType", relationshipTypeName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serialized, equalTo( constraint ) );
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

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        createLabelUniquenessPropertyConstraint( labelName, propertyKey );

        String result = gen.get().noGraph().expectedStatus( 200 ).get(
                getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint = new HashMap<>(  );
        constraint.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint.put( "label", labelName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serializedList, hasItem( constraint ) );
    }

    /**
     * Get a specific node property existence constraint.
     * Get a specific node property existence constraint for a label and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelPropertyExistenceConstraint() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        createLabelPropertyExistenceConstraint( labelName, propertyKey );

        String result = gen.get().noGraph().expectedStatus( 200 ).get(
                getSchemaConstraintLabelExistencePropertyUri( labelName, propertyKey ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint = new HashMap<>(  );
        constraint.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint.put( "label", labelName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serializedList, hasItem( constraint ) );
    }

    /**
     * Get a specific relationship property existence constraint.
     * Get a specific relationship property existence constraint for a label and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getRelationshipTypePropertyExistenceConstraint() throws JsonParseException
    {
        data.get();

        String typeName = relationshipTypes.newInstance(), propertyKey = properties.newInstance();
        createRelationshipTypePropertyExistenceConstraint( typeName, propertyKey );

        String result = gen.get().noGraph().expectedStatus( 200 ).get(
                getSchemaRelationshipConstraintTypeExistencePropertyUri( typeName, propertyKey ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint = new HashMap<>(  );
        constraint.put( "type", ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE.name() );
        constraint.put( "relationshipType", typeName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serializedList, hasItem( constraint ) );
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

        String labelName = labels.newInstance(), propertyKey1 = properties.newInstance(), propertyKey2 = properties.newInstance();
        createLabelUniquenessPropertyConstraint( labelName, propertyKey1 );
        createLabelUniquenessPropertyConstraint( labelName, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaConstraintLabelUniquenessUri( labelName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint1 = new HashMap<>(  );
        constraint1.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint1.put( "label", labelName );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String, Object> constraint2 = new HashMap<>(  );
        constraint2.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint2.put( "label", labelName );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
    }

    /**
     * Get all node property existence constraints for a label.
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelPropertyExistenceConstraints() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey1 = properties.newInstance(), propertyKey2 = properties.newInstance();
        createLabelPropertyExistenceConstraint( labelName, propertyKey1 );
        createLabelPropertyExistenceConstraint( labelName, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaConstraintLabelExistenceUri( labelName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint1 = new HashMap<>(  );
        constraint1.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint1.put( "label", labelName );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String, Object> constraint2 = new HashMap<>(  );
        constraint2.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint2.put( "label", labelName );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
    }

    /**
     * Get all relationship property existence constraints for a type.
     */
    @SuppressWarnings( "unchecked" )
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getRelationshipTypePropertyExistenceConstraints() throws JsonParseException
    {
        data.get();

        String typeName = relationshipTypes.newInstance(), propertyKey1 = properties.newInstance(),
                propertyKey2 = properties.newInstance();
        createRelationshipTypePropertyExistenceConstraint( typeName, propertyKey1 );
        createRelationshipTypePropertyExistenceConstraint( typeName, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 )
                .get( getSchemaRelationshipConstraintTypeExistenceUri( typeName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint1 = new HashMap<>(  );
        constraint1.put( "type", ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE.name() );
        constraint1.put( "relationshipType", typeName );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String, Object> constraint2 = new HashMap<>(  );
        constraint2.put( "type", ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE.name() );
        constraint2.put( "relationshipType", typeName );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
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

        String labelName = labels.newInstance(), propertyKey1 = properties.newInstance(), propertyKey2 = properties.newInstance();
        createLabelUniquenessPropertyConstraint( labelName, propertyKey1 );
        createLabelPropertyExistenceConstraint( labelName, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaConstraintLabelUri( labelName ) ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint1 = new HashMap<>(  );
        constraint1.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint1.put( "label", labelName );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String, Object> constraint2 = new HashMap<>(  );
        constraint2.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint2.put( "label", labelName );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
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

        String labelName1 = labels.newInstance(), propertyKey1 = properties.newInstance();
        String labelName2 = labels.newInstance(), propertyKey2 = properties.newInstance();
        createLabelUniquenessPropertyConstraint( labelName1, propertyKey1 );
        createLabelPropertyExistenceConstraint( labelName2, propertyKey2 );

        String result = gen.get().noGraph().expectedStatus( 200 ).get( getSchemaConstraintUri() ).entity();

        List<Map<String, Object>> serializedList = jsonToList( result );

        Map<String, Object> constraint1 = new HashMap<>(  );
        constraint1.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint1.put( "label", labelName1 );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String, Object> constraint2 = new HashMap<>(  );
        constraint2.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint2.put( "label", labelName2 );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
    }

    /**
     * Drop uniqueness constraint.
     * Drop uniqueness constraint for a label and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void drop_constraint() throws Exception
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        ConstraintDefinition constraintDefinition = createLabelUniquenessPropertyConstraint( labelName, propertyKey );
        assertThat( getConstraints( graphdb(), label( labelName ) ), containsOnly( constraintDefinition ) );

        gen.get().noGraph().expectedStatus( 204 ).delete( getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) ).entity();

        assertThat( getConstraints( graphdb(), label( labelName ) ), isEmpty() );
    }

    /**
     * Drop node property existence constraint.
     * Drop node property existence constraint for a label and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void drop_node_property_existence_constraint() throws Exception
    {
        data.get();

        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        ConstraintDefinition constraintDefinition = createLabelPropertyExistenceConstraint( labelName, propertyKey );
        assertThat( getConstraints( graphdb(), label( labelName ) ), containsOnly( constraintDefinition ) );

        gen.get().noGraph().expectedStatus( 204 ).delete( getSchemaConstraintLabelExistencePropertyUri( labelName, propertyKey ) ).entity();

        assertThat( getConstraints( graphdb(), label( labelName ) ), isEmpty() );
    }

    /**
     * Drop relationship property existence constraint.
     * Drop relationship property existence constraint for a relationship type and a property.
     */
    @Documented
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void drop_relationship_property_existence_constraint() throws Exception
    {
        data.get();

        String typeName = relationshipTypes.newInstance(), propertyKey = properties.newInstance();
        DynamicRelationshipType type = DynamicRelationshipType.withName( typeName );
        ConstraintDefinition constraintDefinition = createRelationshipTypePropertyExistenceConstraint( typeName,
                propertyKey );
        assertThat( getConstraints( graphdb(), type ), containsOnly( constraintDefinition ) );

        gen.get().noGraph().expectedStatus( 204 ).delete(
                getSchemaRelationshipConstraintTypeExistencePropertyUri( typeName, propertyKey ) ).entity();

        assertThat( getConstraints( graphdb(), label( typeName ) ), isEmpty() );
    }

    /**
     * Create an index for a label and property key which already exists.
     */
    @Test
    public void create_existing_constraint()
    {
        String labelName = labels.newInstance(), propertyKey = properties.newInstance();
        createLabelUniquenessPropertyConstraint( labelName, propertyKey );

        Map<String, Object> definition = map( "property_keys", singletonList( propertyKey ) );
        gen.get().noGraph().expectedStatus( 409 ).payload( createJsonFrom( definition ) )
                .post( getSchemaConstraintLabelUniquenessUri( labelName ) ).entity();
    }

    @Test
    public void drop_non_existent_constraint() throws Exception
    {
        String labelName = labels.newInstance(), propertyKey = properties.newInstance();

        gen.get().noGraph().expectedStatus( 404 )
                .delete( getSchemaConstraintLabelUniquenessPropertyUri( labelName, propertyKey ) );
    }

    /**
     * Creating a compound index should not yet be supported.
     */
    @Test
    public void create_compound_schema_index()
    {
        Map<String,Object> definition = map( "property_keys",
                asList( properties.newInstance(), properties.newInstance() ) );

        gen.get().noGraph().expectedStatus( 400 )
                .payload( createJsonFrom( definition ) ).post( getSchemaIndexLabelUri( labels.newInstance() ) );
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

    private ConstraintDefinition createLabelPropertyExistenceConstraint( String labelName, String propertyKey )
    {
        try ( Transaction tx = graphdb().beginTx() )
        {
            ConstraintDefinition constraintDefinition = graphdb().schema().constraintFor( label( labelName ) )
                    .assertPropertyExists( propertyKey ).create();
            tx.success();
            return constraintDefinition;
        }
    }

    private ConstraintDefinition createRelationshipTypePropertyExistenceConstraint( String typeName,
            String propertyKey )
    {
        try ( Transaction tx = graphdb().beginTx() )
        {
            DynamicRelationshipType type = DynamicRelationshipType.withName( typeName );
            ConstraintDefinition constraintDefinition = graphdb().schema().constraintFor( type )
                    .assertPropertyExists( propertyKey ).create();
            tx.success();
            return constraintDefinition;
        }
    }

    private final Factory<String> labels =  UniqueStrings.withPrefix( "label" );
    private final Factory<String> properties =  UniqueStrings.withPrefix( "property" );
    private final Factory<String> relationshipTypes =  UniqueStrings.withPrefix( "relationshipType" );
}
