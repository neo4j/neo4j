/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.rest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToList;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_CONSTRAINT;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_RELATIONSHIP_CONSTRAINT;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class PropertyExistenceConstraintsIT implements GraphHolder
{
    private final Factory<String> labels = UniqueStrings.withPrefix( "label" );
    private final Factory<String> properties = UniqueStrings.withPrefix( "property" );
    private final Factory<String> relationshipTypes = UniqueStrings.withPrefix( "relationshipType" );

    @Rule
    public TestData<Map<String,Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );
    @Rule
    public TestData<RESTRequestGenerator> gen = TestData.producedThrough( RESTRequestGenerator.PRODUCER );

    private static NeoServer server;

    @Override
    public GraphDatabaseService graphdb()
    {
        return server.getDatabase().getGraph();
    }

    @BeforeClass
    public static void initServer() throws Exception
    {
        suppressAll().call( (Callable<Void>) () ->
        {
            CommunityServerBuilder serverBuilder = EnterpriseServerBuilder.serverOnRandomPorts();

            PropertyExistenceConstraintsIT.server = ServerHelper.createNonPersistentServer( serverBuilder );
            return null;
        } );
    }

    @AfterClass
    public static void stopServer() throws Exception
    {
        if ( server != null )
        {
            suppressAll().call( (Callable<Void>) () ->
            {
                server.stop();
                return null;
            } );
        }
    }

    @Documented( "Get a specific node property existence constraint.\n" +
                 "Get a specific node property existence constraint for a label and a property." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelPropertyExistenceConstraint() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance();
        String propertyKey = properties.newInstance();
        createLabelPropertyExistenceConstraint( labelName, propertyKey );

        String result = gen.get().expectedStatus( 200 ).get(
                getSchemaConstraintLabelExistencePropertyUri( labelName, propertyKey ) ).entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> constraint = new HashMap<>();
        constraint.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint.put( "label", labelName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serializedList, hasItem( constraint ) );
    }

    @Documented( "Get a specific relationship property existence constraint.\n" +
                 "Get a specific relationship property existence constraint for a label and a property." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getRelationshipTypePropertyExistenceConstraint() throws JsonParseException
    {
        data.get();

        String typeName = relationshipTypes.newInstance();
        String propertyKey = properties.newInstance();
        createRelationshipTypePropertyExistenceConstraint( typeName, propertyKey );

        String result = gen.get().expectedStatus( 200 ).get(
                getSchemaRelationshipConstraintTypeExistencePropertyUri( typeName, propertyKey ) ).entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> constraint = new HashMap<>();
        constraint.put( "type", ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE.name() );
        constraint.put( "relationshipType", typeName );
        constraint.put( "property_keys", singletonList( propertyKey ) );

        assertThat( serializedList, hasItem( constraint ) );
    }

    @SuppressWarnings( "unchecked" )
    @Documented( "Get all node property existence constraints for a label." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelPropertyExistenceConstraints() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance();
        String propertyKey1 = properties.newInstance();
        String propertyKey2 = properties.newInstance();
        createLabelPropertyExistenceConstraint( labelName, propertyKey1 );
        createLabelPropertyExistenceConstraint( labelName, propertyKey2 );

        String result =
                gen.get().expectedStatus( 200 ).get( getSchemaConstraintLabelExistenceUri( labelName ) )
                        .entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> constraint1 = new HashMap<>();
        constraint1.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint1.put( "label", labelName );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String,Object> constraint2 = new HashMap<>();
        constraint2.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint2.put( "label", labelName );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Documented( "Get all relationship property existence constraints for a type." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getRelationshipTypePropertyExistenceConstraints() throws JsonParseException
    {
        data.get();

        String typeName = relationshipTypes.newInstance();
        String propertyKey1 = properties.newInstance();
        String propertyKey2 = properties.newInstance();
        createRelationshipTypePropertyExistenceConstraint( typeName, propertyKey1 );
        createRelationshipTypePropertyExistenceConstraint( typeName, propertyKey2 );

        String result = gen.get().expectedStatus( 200 )
                .get( getSchemaRelationshipConstraintTypeExistenceUri( typeName ) ).entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> constraint1 = new HashMap<>();
        constraint1.put( "type", ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE.name() );
        constraint1.put( "relationshipType", typeName );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String,Object> constraint2 = new HashMap<>();
        constraint2.put( "type", ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE.name() );
        constraint2.put( "relationshipType", typeName );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Documented( "Get all constraints for a label." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void getLabelPropertyConstraints() throws JsonParseException
    {
        data.get();

        String labelName = labels.newInstance();
        String propertyKey1 = properties.newInstance();
        String propertyKey2 = properties.newInstance();
        createLabelUniquenessPropertyConstraint( labelName, propertyKey1 );
        createLabelPropertyExistenceConstraint( labelName, propertyKey2 );

        String result =
                gen.get().expectedStatus( 200 ).get( getSchemaConstraintLabelUri( labelName ) ).entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> constraint1 = new HashMap<>();
        constraint1.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint1.put( "label", labelName );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String,Object> constraint2 = new HashMap<>();
        constraint2.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint2.put( "label", labelName );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Documented( "Get all constraints." )
    @Test
    @GraphDescription.Graph( nodes = {} )
    public void get_constraints() throws JsonParseException
    {
        data.get();

        String labelName1 = labels.newInstance();
        String propertyKey1 = properties.newInstance();
        String labelName2 = labels.newInstance();
        String propertyKey2 = properties.newInstance();
        createLabelUniquenessPropertyConstraint( labelName1, propertyKey1 );
        createLabelPropertyExistenceConstraint( labelName2, propertyKey2 );

        String result = gen.get().expectedStatus( 200 ).get( getSchemaConstraintUri() ).entity();

        List<Map<String,Object>> serializedList = jsonToList( result );

        Map<String,Object> constraint1 = new HashMap<>();
        constraint1.put( "type", ConstraintType.UNIQUENESS.name() );
        constraint1.put( "label", labelName1 );
        constraint1.put( "property_keys", singletonList( propertyKey1 ) );

        Map<String,Object> constraint2 = new HashMap<>();
        constraint2.put( "type", ConstraintType.NODE_PROPERTY_EXISTENCE.name() );
        constraint2.put( "label", labelName2 );
        constraint2.put( "property_keys", singletonList( propertyKey2 ) );

        assertThat( serializedList, hasItems( constraint1, constraint2 ) );
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

    private String getDataUri()
    {
        return server.baseUri() + "db/data/";
    }

    public String getSchemaConstraintUri()
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT;
    }

    public String getSchemaConstraintLabelUri( String label )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label;
    }

    private void createLabelPropertyExistenceConstraint( String labelName, String propertyKey )
    {
        String query = String.format( "CREATE CONSTRAINT ON (n:%s) ASSERT exists(n.%s)", labelName, propertyKey );
        graphdb().execute( query );
    }

    private void createRelationshipTypePropertyExistenceConstraint( String typeName, String propertyKey )
    {
        String query = String.format( "CREATE CONSTRAINT ON ()-[r:%s]-() ASSERT exists(r.%s)", typeName, propertyKey );
        graphdb().execute( query );
    }

    private String getSchemaConstraintLabelExistenceUri( String label )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label + "/existence/";
    }

    private String getSchemaRelationshipConstraintTypeExistenceUri( String type )
    {
        return getDataUri() + PATH_SCHEMA_RELATIONSHIP_CONSTRAINT + "/" + type + "/existence/";
    }

    private String getSchemaConstraintLabelExistencePropertyUri( String label, String property )
    {
        return getDataUri() + PATH_SCHEMA_CONSTRAINT + "/" + label + "/existence/" + property;
    }

    private String getSchemaRelationshipConstraintTypeExistencePropertyUri( String type, String property )
    {
        return getDataUri() + PATH_SCHEMA_RELATIONSHIP_CONSTRAINT + "/" + type + "/existence/" + property;
    }
}
