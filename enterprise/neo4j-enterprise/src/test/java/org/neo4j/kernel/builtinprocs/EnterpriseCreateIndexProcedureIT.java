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
package org.neo4j.kernel.builtinprocs;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.values.storable.TextValue;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE10;
import static org.neo4j.helpers.Exceptions.rootCause;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public class EnterpriseCreateIndexProcedureIT extends KernelIntegrationTest
{
    @Parameterized.Parameters( name = "{2}" )
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(
                new Object[]{false, false, "createIndex", "index created"},
                new Object[]{false, true, "createUniquePropertyConstraint", "uniqueness constraint online"},
                new Object[]{true, true, "createNodeKey", "node key constraint online"}
        );
    }

    @Parameterized.Parameter()
    public static boolean existenceConstraint;

    @Parameterized.Parameter( 1 )
    public static boolean uniquenessConstraint;

    @Parameterized.Parameter( 2 )
    public static String indexProcedureName;

    @Parameterized.Parameter( 3 )
    public static String expectedSuccessfulCreationStatus;

    @Test
    public void createIndexWithGivenProvider() throws KernelException
    {
        testCreateIndexWithGivenProvider( "Person", "name" );
    }

    @Test
    public void createIndexWithGivenProviderComposite() throws KernelException
    {
        testCreateIndexWithGivenProvider( "NinjaTurtle", "favoritePizza", "favoriteBrother" );
    }

    @Test
    public void shouldCreateNonExistingLabelAndPropertyToken() throws Exception
    {
        // given
        String label = "MyLabel";
        String propKey = "myKey";
        Transaction transaction = newTransaction( AnonymousContext.read() );
        assertEquals( "label token should not exist", TokenRead.NO_TOKEN, transaction.tokenRead().nodeLabel( label ) );
        assertEquals( "property token should not exist", TokenRead.NO_TOKEN, transaction.tokenRead().propertyKey( propKey ) );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        callIndexProcedure( indexPattern( label, propKey ), GraphDatabaseSettings.SchemaIndex.NATIVE20.providerName() );
        commit();

        // then
        transaction = newTransaction( AnonymousContext.read() );
        assertNotEquals( "label token should exist", TokenRead.NO_TOKEN, transaction.tokenRead().nodeLabel( label ) );
        assertNotEquals( "property token should exist", TokenRead.NO_TOKEN, transaction.tokenRead().propertyKey( propKey ) );
    }

    @Test
    public void throwIfNullProvider() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        createProperties( transaction, "name" );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( "Person", "name" );
        try
        {
            callIndexProcedure( pattern, null );
            fail( "Expected to fail" );
        }
        catch ( ProcedureException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Could not create index with specified index provider being null" ) );
        }
        commit();
    }

    @Test
    public void throwIfNonExistingProvider() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        createProperties( transaction, "name" );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( "Person", "name" );
        try
        {
            callIndexProcedure( pattern, "non+existing-1.0" );
            fail( "Expected to fail" );
        }
        catch ( ProcedureException e )
        {
            // then
            assertThat( e.getMessage(), Matchers.allOf(
                    containsString( "Failed to invoke procedure" ),
                    containsString( "Tried to get index provider" ),
                    containsString( "available providers in this session being" ),
                    containsString( "default being" )
            ) );
        }
    }

    @Test
    public void throwIfIndexAlreadyExists() throws Exception
    {
        // given
        String label = "Superhero";
        String propertyKey = "primaryPower";
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( Label.label( label ) ).on( propertyKey ).create();
            tx.success();
        }
        awaitIndexOnline();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( label, propertyKey );
        try
        {
            callIndexProcedure( pattern, GraphDatabaseSettings.SchemaIndex.NATIVE20.providerName() );
            fail( "Should have failed" );
        }
        catch ( ProcedureException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "There already exists an index for " ) );
        }
    }

    private int[] createProperties( Transaction transaction, String... properties ) throws IllegalTokenNameException
    {
        int[] propertyKeyIds = new int[properties.length];
        for ( int i = 0; i < properties.length; i++ )
        {
            propertyKeyIds[i] = transaction.tokenWrite().propertyKeyGetOrCreateForName( properties[i] );
        }
        return propertyKeyIds;
    }

    private long createNodeWithPropertiesAndLabel( Transaction transaction, int labelId, int[] propertyKeyIds, TextValue value ) throws KernelException
    {
        long node = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeAddLabel( node, labelId );
        for ( int propertyKeyId : propertyKeyIds )
        {
            transaction.dataWrite().nodeSetProperty( node, propertyKeyId, value );
        }
        return node;
    }

    private String indexPattern( String label, String... properties )
    {
        StringJoiner pattern = new StringJoiner( ",", ":" + label + "(", ")" );
        for ( String property : properties )
        {
            pattern.add( property );
        }
        return pattern.toString();
    }

    private RawIterator<Object[],ProcedureException> callIndexProcedure( String pattern, String specifiedProvider )
            throws ProcedureException, TransactionFailureException
    {
        return procsSchema().procedureCallSchema( ProcedureSignature.procedureName( "db", indexProcedureName ),
                new Object[]
                        {
                                pattern, // index
                                specifiedProvider // providerName
                        } );
    }

    private void awaitIndexOnline()
    {
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private void testCreateIndexWithGivenProvider( String label, String... properties ) throws KernelException
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        long node = createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( label, properties );
        String specifiedProvider = NATIVE10.providerName();
        RawIterator<Object[],ProcedureException> result = callIndexProcedure( pattern, specifiedProvider );
        // then
        assertThat( Arrays.asList( result.next() ), contains( pattern, specifiedProvider, expectedSuccessfulCreationStatus ) );
        commit();
        awaitIndexOnline();

        // and then
        transaction = newTransaction( AnonymousContext.read() );
        SchemaRead schemaRead = transaction.schemaRead();
        CapableIndexReference index = schemaRead.index( labelId, propertyKeyIds );
        assertCorrectIndex( labelId, propertyKeyIds, uniquenessConstraint, index );
        assertIndexData( transaction, propertyKeyIds, value, node, index );
        commit();
    }

    private void assertIndexData( Transaction transaction, int[] propertyKeyIds, TextValue value, long node, CapableIndexReference index )
            throws KernelException
    {
        try ( NodeValueIndexCursor indexCursor = transaction.cursors().allocateNodeValueIndexCursor() )
        {
            IndexQuery[] query = new IndexQuery[propertyKeyIds.length];
            for ( int i = 0; i < propertyKeyIds.length; i++ )
            {
                query[i] = IndexQuery.exact( propertyKeyIds[i], value );
            }
            transaction.dataRead().nodeIndexSeek( index, indexCursor, IndexOrder.NONE, query );
            assertTrue( indexCursor.next() );
            assertEquals( node, indexCursor.nodeReference() );
            assertFalse( indexCursor.next() );
        }
    }

    private void assertCorrectIndex( int labelId, int[] propertyKeyIds, boolean expectedUnique, CapableIndexReference index )
    {
        assertEquals( "provider key", "lucene+native", index.providerKey() );
        assertEquals( "provider version", "1.0", index.providerVersion() );
        assertEquals( expectedUnique, index.isUnique() );
        assertEquals( "label id", labelId, index.label() );
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            assertEquals( "property key id", propertyKeyIds[i], index.properties()[i] );
        }
    }

    @Test
    public void throwOnUniquenessViolation() throws Exception
    {
        testThrowOnUniquenessViolation( "MyLabel", "oneKey" );
    }

    @Test
    public void throwOnUniquenessViolationComposite() throws Exception
    {
        testThrowOnUniquenessViolation( "MyLabel", "oneKey", "anotherKey" );
    }

    @Test
    public void throwOnNonUniqueStore() throws Exception
    {
        assumeThat( "Only relevant for uniqueness constraints", uniquenessConstraint, is( true ) );

        // given
        String label = "SomeLabel";
        String[] properties = new String[]{"key1", "key2"};
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        // when
        try
        {
            createConstraint( label, properties );
            fail( "Should have failed" );
        }
        catch ( ProcedureException e )
        {
            // then
            // good
            assertThat( rootCause( e ), instanceOf( IndexEntryConflictException.class ) );
        }
    }

    @Test
    public void throwOnExistenceViolation() throws Exception
    {
        assumeThat( "Only relevant for existence constraints", existenceConstraint, is( true ) );

        // given
        String label = "label";
        String prop = "key";
        createConstraint( label, prop );

        // when
        try
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                db.createNode( Label.label( label ) );
                tx.success();
            }
            fail( "Should have failed" );
        }
        catch ( ConstraintViolationException e )
        {
            // then
            // good
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    private void testThrowOnUniquenessViolation( String label, String... properties ) throws Exception
    {
        assumeThat( "Only relevant for uniqueness constraints", uniquenessConstraint, is( true ) );

        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        createConstraint( label, properties );

        // when
        try
        {
            transaction = newTransaction( AnonymousContext.write() );
            createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
            fail( "Should have failed" );
        }
        catch ( UniquePropertyValueValidationException e )
        {
            // then
            // ok
        }
    }

    private void createConstraint( String label, String... properties ) throws TransactionFailureException, ProcedureException
    {
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( label, properties );
        String specifiedProvider = NATIVE10.providerName();
        callIndexProcedure( pattern, specifiedProvider );
        commit();
    }

    @Override
    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }
}
