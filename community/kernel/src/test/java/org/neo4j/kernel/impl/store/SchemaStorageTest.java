/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorPredicates;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.mockito.matcher.KernelExceptionUserMessageMatcher;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR;

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
        IndexRule rule = storage.indexGetForSchema( schemaDescriptor( LABEL1, PROP2 ) );

        // Then
        assertNotNull( rule );
        assertRule( rule, LABEL1, PROP2, NewIndexDescriptor.Type.GENERAL );
    }

    @Test
    public void shouldReturnNullIfIndexRuleForLabelAndPropertyDoesNotExist()
    {
        // Given
        createSchema(
                index( LABEL1, PROP1 ) );

        // When
        IndexRule rule = storage.indexGetForSchema( schemaDescriptor( LABEL1, PROP2 ) );

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
        IndexRule rule = storage.indexGetForSchema( schemaDescriptor( LABEL1, PROP1 ), NewIndexDescriptor.Filter.UNIQUE );

        // Then
        assertNotNull( rule );
        assertRule( rule, LABEL1, PROP1, NewIndexDescriptor.Type.UNIQUE );
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
        Set<IndexRule> listedRules = asSet( storage.indexesGetAll() );

        // Then
        Set<IndexRule> expectedRules = new HashSet<>();
        expectedRules.add( makeIndexRule( 0, LABEL1, PROP1 ) );
        expectedRules.add( makeIndexRule( 1, LABEL1, PROP2 ) );
        expectedRules.add( makeIndexRuleForConstraint( 2, LABEL2, PROP1, 0L ) );

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
        ConstraintRule rule = storage.constraintsGetSingle(
                ConstraintDescriptorFactory.uniqueForLabel( labelId( LABEL1 ), propId( PROP1 ) ) );

        // Then
        assertNotNull( rule );
        assertRule( rule, LABEL1, PROP1, ConstraintDescriptor.Type.UNIQUE );
    }

    @Test
    public void shouldThrowExceptionOnNodeRuleNotFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        // EXPECT
        expectedException.expect( SchemaRuleNotFoundException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "No node property existence constraint was found for :Label1(prop1)." ) );

        // WHEN
        storage.constraintsGetSingle(
                ConstraintDescriptorFactory.existsForLabel( labelId( LABEL1 ), propId( PROP1 ) ) );
    }

    @Test
    public void shouldThrowExceptionOnNodeDuplicateRuleFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        Mockito.when( schemaStorageSpy.loadAllSchemaRules( any(), any(), anyBoolean() ) ).thenReturn(
                Iterators.iterator(
                        getUniquePropertyConstraintRule( 1L, LABEL1, PROP1 ),
                        getUniquePropertyConstraintRule( 2L, LABEL1, PROP1 ) ) );

        //EXPECT
        expectedException.expect( DuplicateSchemaRuleException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "Multiple uniqueness constraints found for :Label1(prop1)." ) );

        // WHEN
        schemaStorageSpy.constraintsGetSingle(
                ConstraintDescriptorFactory.uniqueForLabel( labelId( LABEL1 ), propId( PROP1 ) ) );
    }

    @Test
    public void shouldThrowExceptionOnRelationshipRuleNotFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        // EXPECT
        expectedException.expect( SchemaRuleNotFoundException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher<>( tokenNameLookup,
                "No relationship property existence constraint was found for -[:Type1(prop1)]-." ) );

        //WHEN
        storage.constraintsGetSingle(
                ConstraintDescriptorFactory.existsForRelType( typeId( TYPE1 ), propId( PROP1 ) ) );
    }

    @Test
    public void shouldThrowExceptionOnRelationshipDuplicateRuleFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        Mockito.when( schemaStorageSpy.loadAllSchemaRules( any(), any(), anyBoolean() ) ).thenReturn(
                Iterators.iterator(
                        getRelationshipPropertyExistenceConstraintRule( 1L, TYPE1, PROP1 ),
                        getRelationshipPropertyExistenceConstraintRule( 2L, TYPE1, PROP1 ) ) );

        //EXPECT
        expectedException.expect( DuplicateSchemaRuleException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "Multiple relationship property existence constraints found for -[:Type1(prop1)]-." ) );

        // WHEN
        schemaStorageSpy.constraintsGetSingle(
                ConstraintDescriptorFactory.existsForRelType( typeId( TYPE1 ), propId( PROP1 ) ) );
    }

    private TokenNameLookup getDefaultTokenNameLookup()
    {
        TokenNameLookup tokenNameLookup = Mockito.mock( TokenNameLookup.class );
        Mockito.when( tokenNameLookup.labelGetName( labelId( LABEL1 ) ) ).thenReturn( LABEL1 );
        Mockito.when( tokenNameLookup.propertyKeyGetName( propId( PROP1 ) ) ).thenReturn( PROP1 );
        Mockito.when( tokenNameLookup.relationshipTypeGetName( typeId( TYPE1 ) ) ).thenReturn( TYPE1 );
        return tokenNameLookup;
    }

    private void assertRule( IndexRule rule, String label, String propertyKey, NewIndexDescriptor.Type type )
    {
        assertTrue( SchemaDescriptorPredicates.hasLabel( rule, labelId( label ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( propertyKey ) ) );
        assertEquals( type, rule.getIndexDescriptor().type() );
    }

    private void assertRule( ConstraintRule rule, String label, String propertyKey, ConstraintDescriptor.Type type )
    {
        assertTrue( SchemaDescriptorPredicates.hasLabel( rule, labelId( label ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( propertyKey ) ) );
        assertEquals( type, rule.getConstraintDescriptor().type() );
    }

    private LabelSchemaDescriptor schemaDescriptor( String label, String property )
    {
        return SchemaDescriptorFactory.forLabel( labelId( label ), propId( property ) );
    }

    private IndexRule makeIndexRule( long ruleId, String label, String propertyKey )
    {
        return IndexRule.indexRule(
                ruleId,
                NewIndexDescriptorFactory.forLabel( labelId( label ), propId( propertyKey ) ),
                PROVIDER_DESCRIPTOR );
    }

    private IndexRule makeIndexRuleForConstraint( long ruleId, String label, String propertyKey, long constaintId )
    {
        return IndexRule.constraintIndexRule(
                ruleId,
                NewIndexDescriptorFactory.uniqueForLabel( labelId( label ), propId( propertyKey ) ),
                PROVIDER_DESCRIPTOR, constaintId );
    }

    private ConstraintRule getUniquePropertyConstraintRule( long id, String label, String property )
    {
        return ConstraintRule.constraintRule( id,
                ConstraintDescriptorFactory.uniqueForLabel( labelId( label ), propId( property ) ), 0L );
    }

    private ConstraintRule getRelationshipPropertyExistenceConstraintRule( long id, String type, String property )
    {
        return ConstraintRule.constraintRule( id,
                ConstraintDescriptorFactory.existsForRelType( labelId( type ), propId( property ) ) );
    }

    private static void awaitIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private NodePropertyDescriptor nodeDescriptor( String labelName, String propName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return new NodePropertyDescriptor(
                    readOps().labelGetForName( labelName ),
                    readOps().propertyKeyGetForName( propName ) );
        }
    }

    private RelationshipPropertyDescriptor relDescriptor( String typeName, String propName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return new RelationshipPropertyDescriptor(
                    readOps().relationshipTypeGetForName( typeName ),
                    readOps().propertyKeyGetForName( propName ) );
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
