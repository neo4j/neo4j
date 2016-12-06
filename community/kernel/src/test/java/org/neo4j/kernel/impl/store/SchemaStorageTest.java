/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.DuplicateEntitySchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.EntitySchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.SchemaStorage.IndexRuleKind;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.mockito.matcher.KernelExceptionUserMessageMatcher;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule.uniquenessConstraintRule;

public class SchemaStorageTest
{
    private static final String LABEL1 = "Label1";
    private static final String LABEL2 = "Label2";
    private static final String TYPE1 = "Type1";
    private static final String PROP1 = "prop1";
    private static final String PROP2 = "prop2";

    @ClassRule
    public static final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static SchemaStorage storage;

    @BeforeClass
    public static void initStorage() throws Exception
    {
        storage = new SchemaStorage( dependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getSchemaStore() );
    }

    @Before
    public void clearSchema()
    {
        GraphDatabaseServiceCleaner.cleanupSchema( db );
    }

    @Test
    public void shouldReturnIndexRuleForLabelAndProperty()
    {
        // Given
        createSchema(
                index( LABEL1, PROP1 ),
                index( LABEL1, PROP2 ),
                index( LABEL2, PROP1 ) );

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
        createSchema(
                index( LABEL1, PROP1 ) );

        // When
        IndexRule rule = storage.indexRule( labelId( LABEL1 ), propId( PROP2 ) );

        // Then
        assertNull( rule );
    }

    @Test
    public void shouldListIndexRulesForLabelPropertyAndKind()
    {
        // Given
        createSchema(
                uniquenessConstraint( LABEL1, PROP1 ),
                index( LABEL1, PROP2 ) );

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
        createSchema(
                index( LABEL1, PROP1 ),
                index( LABEL1, PROP2 ),
                uniquenessConstraint( LABEL2, PROP1 ) );

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
        createSchema(
                index( LABEL2, PROP1 ),
                uniquenessConstraint( LABEL1, PROP1 ) );

        // When
        Set<NodePropertyConstraintRule> listedRules = asSet( storage.schemaRulesForNodes( value -> value, NodePropertyConstraintRule.class, labelId( LABEL1 ),
                Predicates.<NodePropertyConstraintRule>alwaysTrue() ) );

        // Then
        Set<NodePropertyConstraintRule> expectedRules = new HashSet<>();
        expectedRules.add( uniquenessConstraintRule( 1, labelId( LABEL1 ), propId( PROP1 ), 0 ) );

        assertEquals( expectedRules, listedRules );
    }

    @Test
    public void shouldListSchemaRulesByClass()
    {
        // Given
        createSchema(
                index( LABEL1, PROP1 ),
                uniquenessConstraint( LABEL2, PROP1 ) );

        // When
        Set<UniquePropertyConstraintRule> listedRules = asSet(
                storage.schemaRules( UniquePropertyConstraintRule.class ) );

        // Then
        Set<UniquePropertyConstraintRule> expectedRules = new HashSet<>();
        expectedRules.add( uniquenessConstraintRule( 0, labelId( LABEL2 ), propId( PROP1 ), 0 ) );

        assertEquals( expectedRules, listedRules );
    }

    @Test
    public void shouldReturnCorrectUniquenessRuleForLabelAndProperty()
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        // Given
        createSchema(
                uniquenessConstraint( LABEL1, PROP1 ),
                uniquenessConstraint( LABEL2, PROP1 ) );

        // When
        UniquePropertyConstraintRule rule = storage.uniquenessConstraint( labelId( LABEL1 ), propId( PROP1 ) );

        // Then
        assertNotNull( rule );
        assertEquals( labelId( LABEL1 ), rule.getLabel() );
        assertEquals( propId( PROP1 ), rule.getPropertyKey() );
        assertEquals( SchemaRule.Kind.UNIQUENESS_CONSTRAINT, rule.getKind() );
    }

    @Test
    public void shouldThrowExceptionOnNodeRuleNotFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        // EXPECT
        expectedException.expect( EntitySchemaRuleNotFoundException.class );
        expectedException.expect(
                new KernelExceptionUserMessageMatcher( tokenNameLookup, "Constraint for label 'Label1' and property" +
                                                                        " 'prop1' not found." ) );

        // WHEN
        storage.nodePropertyExistenceConstraint( labelId( LABEL1 ), propId( PROP1 ) );

    }

    @Test
    public void shouldThrowExceptionOnNodeDuplicateRuleFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        Mockito.when( schemaStorageSpy.loadAllSchemaRules() ).thenReturn(
                Iterators.<SchemaRule>iterator(
                        getUniquePropertyConstraintRule( 1L, LABEL1, PROP1 ),
                        getUniquePropertyConstraintRule( 2L, LABEL1, PROP1 ) ) );

        //EXPECT
        expectedException.expect( DuplicateEntitySchemaRuleException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "Multiple constraints found for label 'Label1' and property 'prop1'." ) );

        // WHEN
        schemaStorageSpy.uniquenessConstraint( labelId( LABEL1 ), propId( PROP1 ) );
    }

    @Test
    public void shouldThrowExceptionOnRelationshipRuleNotFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        // EXPECT
        expectedException.expect( EntitySchemaRuleNotFoundException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher<>( tokenNameLookup,
                "Constraint for relationship type 'Type1' and property 'prop1' not found." ) );

        //WHEN
        storage.relationshipPropertyExistenceConstraint( typeId( TYPE1 ), propId( PROP1 ) );
    }

    @Test
    public void shouldThrowExceptionOnRelationshipDuplicateRuleFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        Mockito.when( schemaStorageSpy.loadAllSchemaRules() ).thenReturn(
                Iterators.<SchemaRule>iterator(
                        getRelationshipPropertyExistenceConstraintRule( 1L, TYPE1, PROP1 ),
                        getRelationshipPropertyExistenceConstraintRule( 2L, TYPE1, PROP1 ) ) );

        //EXPECT
        expectedException.expect( DuplicateEntitySchemaRuleException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "Multiple constraints found for relationship type 'Type1' and property 'prop1'." ) );

        // WHEN
        schemaStorageSpy.relationshipPropertyExistenceConstraint( typeId( TYPE1 ), propId( PROP1 ) );
    }

    private TokenNameLookup getDefaultTokenNameLookup()
    {
        TokenNameLookup tokenNameLookup = Mockito.mock( TokenNameLookup.class );
        Mockito.when( tokenNameLookup.labelGetName( labelId( LABEL1 ) ) ).thenReturn( LABEL1 );
        Mockito.when( tokenNameLookup.propertyKeyGetName( propId( PROP1 ) ) ).thenReturn( PROP1 );
        Mockito.when( tokenNameLookup.relationshipTypeGetName( typeId( TYPE1 ) ) ).thenReturn( TYPE1 );
        return tokenNameLookup;
    }

    private UniquePropertyConstraintRule getUniquePropertyConstraintRule( long id, String label,
            String property )
    {
        return UniquePropertyConstraintRule
                .uniquenessConstraintRule( id, labelId( label ), propId( property ), 0L );
    }

    private RelationshipPropertyExistenceConstraintRule getRelationshipPropertyExistenceConstraintRule( long id,
            String type,
            String property )
    {
        return RelationshipPropertyExistenceConstraintRule
                .relPropertyExistenceConstraintRule( id, typeId( type ), propId( property ) );
    }

    private static void awaitIndexes()
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

    private static DependencyResolver dependencyResolver()
    {
        return db.getGraphDatabaseAPI().getDependencyResolver();
    }

    private Consumer<GraphDatabaseService> index( String label, String prop )
    {
        return db -> db.schema().indexFor( Label.label( label ) ).on( prop ).create();
    }

    private Consumer<GraphDatabaseService> uniquenessConstraint( String label, String prop )
    {
        return db -> db.schema().constraintFor( Label.label( label ) ).assertPropertyIsUnique( prop ).create();
    }

    @SafeVarargs
    private static void createSchema( Consumer<GraphDatabaseService>... creators )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Consumer<GraphDatabaseService> rule : creators )
            {
                rule.accept( db );
            }
            tx.success();
        }
        awaitIndexes();
    }
}
