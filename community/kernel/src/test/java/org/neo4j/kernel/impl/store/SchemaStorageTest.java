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
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Functions;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.SchemaStorage.IndexRuleKind;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.MandatoryNodePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.MandatoryRelationshipPropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.NodePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.record.MandatoryNodePropertyConstraintRule.mandatoryNodePropertyConstraintRule;
import static org.neo4j.kernel.impl.store.record.MandatoryRelationshipPropertyConstraintRule.mandatoryRelPropertyConstraintRule;
import static org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule.uniquenessConstraintRule;

public class SchemaStorageTest
{
    private static final String LABEL1 = "Label1";
    private static final String LABEL2 = "Label2";
    private static final String TYPE1 = "Type1";
    private static final String TYPE2 = "Type2";
    private static final String PROP1 = "prop1";
    private static final String PROP2 = "prop2";

    @Rule
    public final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule();

    private SchemaStorage storage;

    @Before
    public void initStorage() throws Exception
    {
        storage = new SchemaStorage( dependencyResolver().resolveDependency( NeoStore.class ).getSchemaStore() );
    }

    @Test
    public void shouldReturnIndexRuleForLabelAndProperty()
    {
        // Given
        createIndex( LABEL1, PROP1 );
        createIndex( LABEL1, PROP2 );
        createIndex( LABEL2, PROP1 );

        // When
        IndexRule rule = storage.indexRule( labelId( LABEL1 ), propId( PROP1 ) );

        // Then
        assertNotNull( rule );
        assertEquals( labelId( LABEL1 ), rule.getLabel() );
        assertEquals( propId( PROP1 ), rule.getPropertyKey() );
        assertEquals( SchemaRule.Kind.INDEX_RULE, rule.getKind() );
    }

    @Test
    public void shouldReturnNullIfIndexRuleForLabelAndPropertyDoesNotExist()
    {
        // Given
        createIndex( LABEL1, PROP1 );

        // When
        IndexRule rule = storage.indexRule( labelId( LABEL1 ), propId( PROP2 ) );

        // Then
        assertNull( rule );
    }

    @Test
    public void shouldListIndexRulesForLabelPropertyAndKind()
    {
        // Given
        createUniquenessConstraint( LABEL1, PROP1 );
        createIndex( LABEL1, PROP2 );

        // When
        IndexRule rule = storage.indexRule( labelId( LABEL1 ), propId( PROP1 ), IndexRuleKind.CONSTRAINT );

        // Then
        assertNotNull( rule );
        assertEquals( labelId( LABEL1 ), rule.getLabel() );
        assertEquals( propId( PROP1 ), rule.getPropertyKey() );
        assertEquals( SchemaRule.Kind.CONSTRAINT_INDEX_RULE, rule.getKind() );
    }

    @Test
    public void shouldListAllIndexRules()
    {
        // Given
        createIndex( LABEL1, PROP1 );
        createIndex( LABEL1, PROP2 );
        createUniquenessConstraint( LABEL2, PROP1 );

        // When
        Set<IndexRule> listedRules = asSet( storage.allIndexRules() );

        // Then
        Set<IndexRule> expectedRules = new HashSet<>();
        expectedRules.add( new IndexRule( 0, labelId( LABEL1 ), propId( PROP1 ), PROVIDER_DESCRIPTOR, null ) );
        expectedRules.add( new IndexRule( 1, labelId( LABEL1 ), propId( PROP2 ), PROVIDER_DESCRIPTOR, null ) );
        expectedRules.add( new IndexRule( 2, labelId( LABEL2 ), propId( PROP1 ), PROVIDER_DESCRIPTOR, 0L ) );

        assertEquals( expectedRules, listedRules );
    }

    @Test
    public void shouldListAllSchemaRulesForNodes()
    {
        // Given
        createIndex( LABEL2, PROP1 );
        createUniquenessConstraint( LABEL1, PROP1 );
        createMandatoryNodePropertyConstraint( LABEL1, PROP1 );
        createMandatoryRelPropertyConstraint( LABEL1, PROP1 );

        // When
        Set<NodePropertyConstraintRule> listedRules = asSet( storage.schemaRulesForNodes(
                Functions.<NodePropertyConstraintRule>identity(), NodePropertyConstraintRule.class, labelId( LABEL1 ),
                Predicates.<NodePropertyConstraintRule>alwaysTrue() ) );

        // Then
        Set<NodePropertyConstraintRule> expectedRules = new HashSet<>();
        expectedRules.add( uniquenessConstraintRule( 1, labelId( LABEL1 ), propId( PROP1 ), 0 ) );
        expectedRules.add( mandatoryNodePropertyConstraintRule( 1, labelId( LABEL1 ), propId( PROP1 ) ) );

        assertEquals( expectedRules, listedRules );
    }

    @Test
    public void shouldListAllSchemaRulesForRelationships()
    {
        // Given
        createIndex( LABEL1, PROP1 );
        createMandatoryRelPropertyConstraint( TYPE1, PROP1 );
        createMandatoryRelPropertyConstraint( TYPE2, PROP1 );
        createMandatoryRelPropertyConstraint( TYPE1, PROP2 );

        // When
        Set<RelationshipPropertyConstraintRule> listedRules = asSet( storage.schemaRulesForRelationships(
                Functions.<RelationshipPropertyConstraintRule>identity(), RelationshipPropertyConstraintRule.class,
                labelId( LABEL1 ), Predicates.<RelationshipPropertyConstraintRule>alwaysTrue() ) );

        // Then
        Set<RelationshipPropertyConstraintRule> expectedRules = new HashSet<>();
        expectedRules.add( mandatoryRelPropertyConstraintRule( 0, typeId( TYPE1 ), propId( PROP1 ) ) );
        expectedRules.add( mandatoryRelPropertyConstraintRule( 1, typeId( TYPE1 ), propId( PROP2 ) ) );

        assertEquals( expectedRules, listedRules );
    }

    @Test
    public void shouldListSchemaRulesByClass()
    {
        // Given
        createUniquenessConstraint( LABEL1, PROP1 );
        createMandatoryNodePropertyConstraint( LABEL1, PROP1 );
        createMandatoryRelPropertyConstraint( TYPE1, PROP1 );

        // When
        Set<RelationshipPropertyConstraintRule> listedRules = asSet(
                storage.schemaRules( RelationshipPropertyConstraintRule.class ) );

        // Then
        Set<RelationshipPropertyConstraintRule> expectedRules = new HashSet<>();
        expectedRules.add( mandatoryRelPropertyConstraintRule( 0, typeId( TYPE1 ), propId( PROP1 ) ) );

        assertEquals( expectedRules, listedRules );
    }

    @Test
    public void shouldReturnCorrectUniquenessRuleForLabelAndProperty() throws SchemaRuleNotFoundException
    {
        // Given
        createUniquenessConstraint( LABEL1, PROP1 );
        createUniquenessConstraint( LABEL2, PROP1 );

        // When
        UniquePropertyConstraintRule rule = storage.uniquenessConstraint( labelId( LABEL1 ), propId( PROP1 ) );

        // Then
        assertNotNull( rule );
        assertEquals( labelId( LABEL1 ), rule.getLabel() );
        assertEquals( propId( PROP1 ), rule.getPropertyKey() );
        assertEquals( SchemaRule.Kind.UNIQUENESS_CONSTRAINT, rule.getKind() );
    }

    @Test
    public void shouldReturnCorrectMandatoryRuleForLabelAndProperty() throws SchemaRuleNotFoundException
    {
        // Given
        createMandatoryNodePropertyConstraint( LABEL1, PROP1 );
        createMandatoryNodePropertyConstraint( LABEL2, PROP1 );

        // When
        MandatoryNodePropertyConstraintRule rule =
                storage.mandatoryNodePropertyConstraint( labelId( LABEL1 ), propId( PROP1 ) );

        // Then
        assertNotNull( rule );
        assertEquals( labelId( LABEL1 ), rule.getLabel() );
        assertEquals( propId( PROP1 ), rule.getPropertyKey() );
        assertEquals( SchemaRule.Kind.MANDATORY_NODE_PROPERTY_CONSTRAINT, rule.getKind() );
    }

    @Test
    public void shouldReturnCorrectMandatoryRuleForRelTypeAndProperty() throws SchemaRuleNotFoundException
    {
        // Given
        createMandatoryRelPropertyConstraint( TYPE1, PROP1 );
        createMandatoryRelPropertyConstraint( TYPE2, PROP1 );

        // When
        MandatoryRelationshipPropertyConstraintRule rule =
                storage.mandatoryRelationshipPropertyConstraint( typeId( TYPE1 ), propId( PROP1 ) );

        // Then
        assertNotNull( rule );
        assertEquals( typeId( TYPE1 ), rule.getRelationshipType() );
        assertEquals( propId( PROP1 ), rule.getPropertyKey() );
        assertEquals( SchemaRule.Kind.MANDATORY_RELATIONSHIP_PROPERTY_CONSTRAINT, rule.getKind() );
    }

    private void createIndex( String labelName, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( labelName ) ).on( propertyName ).create();
            tx.success();
        }
        awaitIndexes();
    }

    private void createUniquenessConstraint( String labelName, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label( labelName ) ).assertPropertyIsUnique( propertyName ).create();
            tx.success();
        }
        awaitIndexes();
    }

    private void createMandatoryNodePropertyConstraint( String labelName, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label( labelName ) ).assertPropertyExists( propertyName ).create();
            tx.success();
        }
        awaitIndexes();
    }

    private void createMandatoryRelPropertyConstraint( String typeName, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( withName( typeName ) ).assertPropertyExists( propertyName ).create();
            tx.success();
        }
        awaitIndexes();
    }

    private void awaitIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private int labelId( String labelName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return readOps().labelGetForName( labelName );
        }
    }

    private int propId( String propName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return readOps().propertyKeyGetForName( propName );
        }
    }

    private int typeId( String typeName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return readOps().relationshipTypeGetForName( typeName );
        }
    }

    private ReadOperations readOps()
    {
        DependencyResolver dependencyResolver = dependencyResolver();
        Statement statement = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ).get();
        return statement.readOperations();
    }

    private DependencyResolver dependencyResolver()
    {
        return db.getGraphDatabaseAPI().getDependencyResolver();
    }
}
