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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class IndexRecoveryIT
{
    @Test
    public void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndex() throws Exception
    {
        // Given
        startDb();
        Label myLabel = label( "MyLabel" );

        CountDownLatch latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong() ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        createIndex( myLabel );

        // And Given
        killDb();
        latch.countDown();

        // When
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.POPULATING );
        latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong() ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        startDb();

        // Then
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );

        assertThat( indexes.size(), equalTo( 1 ) );

        IndexDefinition index = single( indexes );
        assertThat( db.schema().getIndexState( index), equalTo( Schema.IndexState.POPULATING ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong() );
        verify( mockedIndexProvider, times( 0 ) ).getWriter( anyLong() );
        latch.countDown();
    }

    @Test
    public void shouldBeAbleToRecoverInTheMiddleOfPopulatingAnIndexWhereLogHasRotated() throws Exception
    {
        // Given
        startDb();
        Label myLabel = label( "MyLabel" );

        CountDownLatch latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong() ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        createIndex( myLabel );
        rotateLogs();

        // And Given
        killDb();
        latch.countDown();
        latch = new CountDownLatch( 1 );
        when( mockedIndexProvider.getPopulator( anyLong() ) ).thenReturn( indexPopulatorWithControlledCompletionTiming( latch ) );
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.POPULATING );

        // When
        startDb();

        // Then
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );

        assertThat( indexes.size(), equalTo( 1 ) );

        IndexDefinition index = single( indexes );
        assertThat( db.schema().getIndexState( index), equalTo( Schema.IndexState.POPULATING ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong() );
        verify( mockedIndexProvider, times( 0 ) ).getWriter( anyLong() );
        latch.countDown();
    }
    
    @Test
    public void shouldBeAbleToRecoverAndUpdateOnlineIndex() throws Exception
    {
        // Given
        startDb();
        Label myLabel = label( "MyLabel" );

        createIndex( myLabel );
        Set<NodePropertyUpdate> expectedUpdates = createSomeBananas( myLabel );

        // And Given
        killDb();
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.ONLINE );
        GatheringIndexWriter writer = new GatheringIndexWriter();
        when( mockedIndexProvider.getWriter( anyLong() ) ).thenReturn( writer );

        // When
        startDb();

        // Then
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );

        assertThat( indexes.size(), equalTo( 1 ) );

        IndexDefinition index = single( indexes );
        assertThat( db.schema().getIndexState( index), equalTo( Schema.IndexState.ONLINE ) );
        verify( mockedIndexProvider, times( 1 ) ).getPopulator( anyLong() );
        verify( mockedIndexProvider, times( 1 ) ).getWriter( anyLong() );
        assertEquals( expectedUpdates, writer.updates ); 
    }
    
    @Test
    public void shouldKeepFailedIndexesAsFailedAfterRestart() throws Exception
    {
        // Given
        startDb();
        Label myLabel = label( "MyLabel" );
        createIndex( myLabel );

        // And Given
        killDb();
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.FAILED );
        when( mockedIndexProvider.getPopulator( anyLong() ) ).thenReturn( mock(IndexPopulator.class) );

        // When
        startDb();

        // Then
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );

        assertThat( indexes.size(), equalTo( 1 ) );

        IndexDefinition index = single( indexes );
        assertThat( db.schema().getIndexState( index ), equalTo( Schema.IndexState.FAILED ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong() );
    }
    
    private GraphDatabaseAPI db;
    private EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private final SchemaIndexProvider mockedIndexProvider = mock( SchemaIndexProvider.class);
    private final String key = "number_of_bananas_owned";

    private void startDb()
    {
        if ( db != null )
            db.shutdown();

        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs );
        factory.setSchemaIndexProviders( Arrays.asList( mockedIndexProvider ) );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
    }

    private void killDb()
    {
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        if ( db != null )
            db.shutdown();
        fs.shutdown();
        fs = snapshot;
    }
    
    @After
    public void after()
    {
        fs.shutdown();
    }

    private void rotateLogs()
    {
        db.getXaDataSourceManager().rotateLogicalLogs();
    }

    private void createIndex( Label label )
    {
        Transaction tx = db.beginTx();
        db.schema().indexCreator( label ).on( key ).create();
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
            StatementContext context = db.getDependencyResolver().resolveDependency(
                    ThreadToStatementContextBridge.class ).getCtxForWriting();
            for ( int number : new int[] {4, 10} )
            {
                Node node = db.createNode( label );
                node.setProperty( key, number );
                updates.add( NodePropertyUpdate.add( node.getId(), context.getPropertyKeyId( key ), number,
                        new long[] {context.getLabelId( label.name() )} ) );
            }
            tx.success();
            return updates;
        }
        finally
        {
            tx.finish();
        }
    }
    
    private static class GatheringIndexWriter extends IndexWriter.Adapter
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<NodePropertyUpdate>();
        
        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
            this.updates.addAll( asCollection( updates ) );
        }
    }

    private IndexPopulator indexPopulatorWithControlledCompletionTiming( final CountDownLatch latch )
    {
        return new IndexPopulator.Adapter()
        {
            @Override
            public void createIndex()
            {
                try
                {
                    latch.await();
                }
                catch ( InterruptedException e )
                {
                }
                throw new RuntimeException();
            }
        };
    }
}
