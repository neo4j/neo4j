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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.getIndexes;
import static org.neo4j.graphdb.Neo4jMatchers.hasSize;
import static org.neo4j.graphdb.Neo4jMatchers.haveState;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;

public class IndexRecoveryIT
{
    private final Label myLabel = label( "MyLabel" );

    @Test
    public void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndex() throws Exception
    {
        // Given
        startDb();

        CountDownLatch latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        createIndex( myLabel );

        // And Given
        Future<Void> killFuture = killDbInSeparateThread();
        latch.countDown();
        killFuture.get();

        // When
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.POPULATING );
        latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.POPULATING ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong(), any( IndexConfiguration.class ) );
        verify( mockedIndexProvider, times( 0 ) ).getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) );
        latch.countDown();
    }

    @Test
    public void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndexWhereLogHasRotated() throws Exception
    {
        // Given
        startDb();

        CountDownLatch latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        createIndex( myLabel );
        rotateLogs();

        // And Given
        Future<Void> killFuture = killDbInSeparateThread();
        latch.countDown();
        killFuture.get();
        latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.POPULATING );

        // When
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.POPULATING ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong(), any( IndexConfiguration.class ) );
        verify( mockedIndexProvider, times( 0 ) ).getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) );
        latch.countDown();
    }

    @Test
    public void shouldBeAbleToRecoverAndUpdateOnlineIndex() throws Exception
    {
        // Given
        startDb();

        createIndex( myLabel );
        Set<NodePropertyUpdate> expectedUpdates = createSomeBananas( myLabel );

        // And Given
        killDb();
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.ONLINE );
        GatheringIndexWriter writer = new GatheringIndexWriter();
        when( mockedIndexProvider.getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( writer );

        // When
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.ONLINE ) ) );
        verify( mockedIndexProvider, times( 1 ) ).getPopulator( anyLong(), any( IndexConfiguration.class ) );
        verify( mockedIndexProvider, times( 1 ) ).getOnlineAccessor( anyLong(), any( IndexConfiguration.class ) );
        assertEquals( expectedUpdates, writer.updates );
    }

    @Test
    public void shouldKeepFailedIndexesAsFailedAfterRestart() throws Exception
    {
        // Given
        startDb();
        createIndex( myLabel );

        // And Given
        killDb();
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.FAILED );
        when( mockedIndexProvider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( mock(IndexPopulator.class) );

        // When
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 1 ) ) );
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.FAILED ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong(), any( IndexConfiguration.class ) );
    }

    private GraphDatabaseAPI db;
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final SchemaIndexProvider mockedIndexProvider = mock( SchemaIndexProvider.class );
    private final KernelExtensionFactory<?> mockedIndexProviderFactory =
            singleInstanceSchemaIndexProviderFactory( TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR.getKey(),
                    mockedIndexProvider );
    private final String key = "number_of_bananas_owned";

    @Before
    public void setUp()
    {
        when( mockedIndexProvider.getProviderDescriptor() ).thenReturn( TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR );
    }

    private void startDb()
    {
        if ( db != null )
            db.shutdown();

        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        factory.setKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( mockedIndexProviderFactory ) );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
    }

    private void killDb()
    {
        if ( db != null )
        {
            fs.snapshot( new Runnable()
            {
                @Override
                public void run()
                {
                    db.shutdown();
                    db = null;
                }
            } );
        }
    }

    private Future<Void> killDbInSeparateThread()
    {
        ExecutorService executor = newSingleThreadExecutor();
        Future<Void> result = executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                killDb();
                return null;
            }
        } );
        executor.shutdown();
        return result;
    }

    @After
    public void after()
    {
        if ( db != null )
            db.shutdown();
    }

    @SuppressWarnings("deprecation")
    private void rotateLogs()
    {
        db.getXaDataSourceManager().rotateLogicalLogs();
    }

    private void createIndex( Label label )
    {
        Transaction tx = db.beginTx();
        db.schema().indexFor( label ).on( key ).create();
        tx.success();
        tx.finish();
    }

    private Set<NodePropertyUpdate> createSomeBananas( Label label )
            throws PropertyKeyNotFoundException, LabelNotFoundKernelException
    {
        Set<NodePropertyUpdate> updates = new HashSet<NodePropertyUpdate>();
        Transaction tx = db.beginTx();
        try
        {
            ThreadToStatementContextBridge ctxProvider = db.getDependencyResolver().resolveDependency(
                    ThreadToStatementContextBridge.class );
            StatementOperationParts context = ctxProvider.getCtxForWriting();
            StatementState state = ctxProvider.statementForReading();
            for ( int number : new int[] {4, 10} )
            {
                Node node = db.createNode( label );
                node.setProperty( key, number );
                updates.add( NodePropertyUpdate.add( node.getId(), context.keyReadOperations().propertyKeyGetForName( state, key ), number,
                        new long[] {context.keyReadOperations().labelGetForName( state, label.name() )} ) );
            }
            state.close();
            tx.success();
            return updates;
        }
        finally
        {
            tx.finish();
        }
    }

    private static class GatheringIndexWriter extends IndexAccessor.Adapter
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<NodePropertyUpdate>();

        @Override
        public void updateAndCommit( Iterable<NodePropertyUpdate> updates )
        {
            this.updates.addAll( asCollection( updates ) );
        }

        @Override
        public void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
        {
            this.updates.addAll( asCollection( updates ) );
        }
    }

    private IndexPopulator indexPopulatorWithControlledCompletionTiming( final CountDownLatch latch )
    {
        return new IndexPopulator.Adapter()
        {
            @Override
            public void create()
            {
                try
                {
                    latch.await();
                }
                catch ( InterruptedException e )
                {
                    // fall through and return early
                }
                throw new RuntimeException();
            }
        };
    }
}
