/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaStorage;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.IndexDescriptor;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.storageengine.api.ConstraintRule;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.token.TokenHolders;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.ArrayUtil.single;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;
import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.forSchema;
import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.uniqueForSchema;

public class SchemaStorageIT
{
    private static final String LABEL1 = "Label1";
    private static final String LABEL2 = "Label2";
    private static final String TYPE1 = "Type1";
    private static final String PROP1 = "prop1";
    private static final String PROP2 = "prop2";

    @ClassRule
    public static final DbmsRule db = new ImpermanentDbmsRule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static SchemaStorage storage;

    @BeforeClass
    public static void initStorage() throws Exception
    {
        try ( Transaction transaction = db.beginTx() )
        {
            TokenWrite tokenWrite = getTransaction().tokenWrite();
            tokenWrite.propertyKeyGetOrCreateForName( PROP1 );
            tokenWrite.propertyKeyGetOrCreateForName( PROP2 );
            tokenWrite.labelGetOrCreateForName( LABEL1 );
            tokenWrite.labelGetOrCreateForName( LABEL2 );
            tokenWrite.relationshipTypeGetOrCreateForName( TYPE1 );
            transaction.success();
        }
        SchemaStore schemaStore = resolveDependency( RecordStorageEngine.class ).testAccessNeoStores().getSchemaStore();
        storage = new SchemaStorage( schemaStore, resolveDependency( TokenHolders.class ) );
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
        StorageIndexReference rule = single( storage.indexGetForSchema( indexDescriptor( LABEL1, PROP2 ) ) );

        // Then
        assertNotNull( rule );
        assertRule( rule, LABEL1, PROP2, false );
    }

    @Test
    public void shouldReturnIndexRuleForLabelAndPropertyComposite()
    {
        String a = "a";
        String b = "b";
        String c = "c";
        String d = "d";
        String e = "e";
        String f = "f";
        createSchema( db -> db.schema().indexFor( Label.label( LABEL1 ) )
          .on( a ).on( b ).on( c ).on( d ).on( e ).on( f ).create() );

        StorageIndexReference rule = single( storage.indexGetForSchema( TestIndexDescriptorFactory.forLabel(
                labelId( LABEL1 ), propId( a ), propId( b ), propId( c ), propId( d ), propId( e ), propId( f ) ) ) );

        assertNotNull( rule );
        assertTrue( SchemaDescriptorPredicates.hasLabel( rule, labelId( LABEL1 ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( a ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( b ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( c ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( d ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( e ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( f ) ) );
        assertFalse( rule.isUnique() );
    }

    @Test
    public void shouldReturnIndexRuleForLabelAndVeryManyPropertiesComposite()
    {
        String[] props = "abcdefghijklmnopqrstuvwxyzABCDEFGHJILKMNOPQRSTUVWXYZ".split( "\\B" );
        createSchema( db ->
        {
            IndexCreator indexCreator = db.schema().indexFor( Label.label( LABEL1 ) );
            for ( String prop : props )
            {
                indexCreator = indexCreator.on( prop );
            }
            indexCreator.create();
        } );

        StorageIndexReference rule = single( storage.indexGetForSchema( TestIndexDescriptorFactory.forLabel(
                labelId( LABEL1 ), Arrays.stream( props ).mapToInt( this::propId ).toArray() ) ) );

        assertNotNull( rule );
        assertTrue( SchemaDescriptorPredicates.hasLabel( rule, labelId( LABEL1 ) ) );
        for ( String prop : props )
        {
            assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( prop ) ) );
        }
        assertFalse( rule.isUnique() );
    }

    @Test
    public void shouldReturnEmptyArrayIfIndexRuleForLabelAndPropertyDoesNotExist()
    {
        // Given
        createSchema(
                index( LABEL1, PROP1 ) );

        // When
        StorageIndexReference[] rules = storage.indexGetForSchema( indexDescriptor( LABEL1, PROP2 ) );

        // Then
        assertThat( rules.length, is( 0 ) );
    }

    @Test
    public void shouldListIndexRulesForLabelPropertyAndKind()
    {
        // Given
        createSchema(
                uniquenessConstraint( LABEL1, PROP1 ),
                index( LABEL1, PROP2 ) );

        // When
        StorageIndexReference rule = single( storage.indexGetForSchema( uniqueIndexDescriptor( LABEL1, PROP1 ) ) );

        // Then
        assertNotNull( rule );
        assertRule( rule, LABEL1, PROP1, true );
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
        Set<StorageIndexReference> listedRules = asSet( storage.indexesGetAll() );

        // Then
        Set<StoreIndexDescriptor> expectedRules = new HashSet<>();
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

    private void assertRule( StorageIndexReference rule, String label, String propertyKey, boolean isUnique )
    {
        assertTrue( SchemaDescriptorPredicates.hasLabel( rule, labelId( label ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( rule, propId( propertyKey ) ) );
        assertEquals( isUnique, rule.isUnique() );
    }

    private void assertRule( ConstraintRule rule, String label, String propertyKey, ConstraintDescriptor.Type type )
    {
        ConstraintDescriptor constraint = rule.getConstraintDescriptor();
        assertTrue( SchemaDescriptorPredicates.hasLabel( constraint, labelId( label ) ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( constraint, propId( propertyKey ) ) );
        assertEquals( type, constraint.type() );
    }

    private IndexDescriptor indexDescriptor( String label, String property )
    {
        return TestIndexDescriptorFactory.forLabel( labelId( label ), propId( property ) );
    }

    private IndexDescriptor uniqueIndexDescriptor( String label, String property )
    {
        return TestIndexDescriptorFactory.uniqueForLabel( labelId( label ), propId( property ) );
    }

    private StoreIndexDescriptor makeIndexRule( long ruleId, String label, String propertyKey )
    {
        return forSchema( forLabel( labelId( label ), propId( propertyKey ) ), EMPTY.getProviderDescriptor() ).withId( ruleId );
    }

    private StoreIndexDescriptor makeIndexRuleForConstraint( long ruleId, String label, String propertyKey, long constraintId )
    {
        return uniqueForSchema( forLabel( labelId( label ), propId( propertyKey ) ), EMPTY.getProviderDescriptor() ).withIds( ruleId, constraintId );
    }

    private static int labelId( String labelName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return getTransaction().tokenRead().nodeLabel( labelName );
        }
    }

    private int propId( String propName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return getTransaction().tokenRead().propertyKey( propName );
        }
    }

    private static KernelTransaction getTransaction()
    {
        return resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
    }

    private static <T> T resolveDependency( Class<T> clazz )
    {
        return db.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( clazz );
    }

    private static Consumer<GraphDatabaseService> index( String label, String prop )
    {
        return db -> db.schema().indexFor( Label.label( label ) ).on( prop ).create();
    }

    private static Consumer<GraphDatabaseService> uniquenessConstraint( String label, String prop )
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

    private static void awaitIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }
}
