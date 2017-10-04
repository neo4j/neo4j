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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RecordStorageEngineRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.TimeUtil.parseTimeMillis;
import static org.neo4j.kernel.impl.transaction.command.Commands.createIndexRule;
import static org.neo4j.kernel.impl.transaction.command.Commands.transactionRepresentation;
import static org.neo4j.kernel.impl.transaction.log.Commitment.NO_COMMITMENT;

public class IndexWorkSyncTransactionApplicationStressIT
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final RecordStorageEngineRule storageEngineRule = new RecordStorageEngineRule();
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( directory )
                                          .around( fileSystemRule )
                                          .around( pageCacheRule )
                                          .around( storageEngineRule );

    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 0 );

    @Test
    public void shouldApplyIndexUpdatesInWorkSyncedBatches() throws Exception
    {
        // GIVEN
        long duration = parseTimeMillis.apply( System.getProperty( getClass().getName() + ".duration", "2s" ) );
        int numThreads = Integer.getInteger( getClass().getName() + ".numThreads",
                Runtime.getRuntime().availableProcessors() );
        RecordStorageEngine storageEngine = storageEngineRule
                .getWith( fileSystemRule.get(), pageCacheRule.getPageCache( fileSystemRule.get() ) )
                .storeDirectory( directory.directory() )
                .indexProvider( new InMemoryIndexProvider() )
                .build();
        storageEngine.apply( tx( singletonList( createIndexRule(
                InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR, 1, descriptor ) ) ),
                TransactionApplicationMode.EXTERNAL );
        Dependencies dependencies = new Dependencies();
        storageEngine.satisfyDependencies( dependencies );
        IndexProxy index = dependencies.resolveDependency( IndexingService.class )
                .getIndexProxy( descriptor );
        awaitOnline( index );

        // WHEN
        Workers<Worker> workers = new Workers<>( getClass().getSimpleName() );
        final AtomicBoolean end = new AtomicBoolean();
        for ( int i = 0; i < numThreads; i++ )
        {
            workers.start( new Worker( i, end, storageEngine, 10, index ) );
        }

        // let the threads hammer the storage engine for some time
        Thread.sleep( duration );
        end.set( true );

        // THEN (assertions as part of the workers applying transactions)
        workers.awaitAndThrowOnError( RuntimeException.class );
    }

    private void awaitOnline( IndexProxy index ) throws InterruptedException
    {
        while ( index.getState() == InternalIndexState.POPULATING )
        {
            Thread.sleep( 10 );
        }
    }

    private static Value propertyValue( int id, int progress )
    {
        return Values.of( id + "_" + progress );
    }

    private static TransactionToApply tx( Collection<StorageCommand> commands )
    {
        TransactionToApply tx = new TransactionToApply( transactionRepresentation( commands ) );
        tx.commitment( NO_COMMITMENT, 0 );
        return tx;
    }

    private class Worker implements Runnable
    {
        private final int id;
        private final AtomicBoolean end;
        private final RecordStorageEngine storageEngine;
        private final NodeStore nodeIds;
        private final int batchSize;
        private final IndexProxy index;
        private final CommandCreationContext commandCreationContext;
        private int i;
        private int base;

        Worker( int id, AtomicBoolean end, RecordStorageEngine storageEngine, int batchSize, IndexProxy index )
        {
            this.id = id;
            this.end = end;
            this.storageEngine = storageEngine;
            this.batchSize = batchSize;
            this.index = index;
            NeoStores neoStores = this.storageEngine.testAccessNeoStores();
            this.nodeIds = neoStores.getNodeStore();
            this.commandCreationContext = storageEngine.allocateCommandCreationContext();
        }

        @Override
        public void run()
        {
            try
            {
                TransactionQueue queue = new TransactionQueue( batchSize, ( tx, last ) ->
                {
                    // Apply
                    storageEngine.apply( tx, TransactionApplicationMode.EXTERNAL );

                    // And verify that all nodes are in the index
                    verifyIndex( tx );
                    base += batchSize;
                } );
                for ( ; !end.get(); i++ )
                {
                    queue.queue( createNodeAndProperty( i ) );
                }
                queue.empty();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                commandCreationContext.close();
            }
        }

        private TransactionToApply createNodeAndProperty( int progress ) throws Exception
        {
            TransactionState txState = new TxState();
            long nodeId = nodeIds.nextId();
            txState.nodeDoCreate( nodeId );
            txState.nodeDoAddLabel( descriptor.getLabelId(), nodeId );
            txState.nodeDoAddProperty( nodeId, descriptor.getPropertyId(), propertyValue( id, progress ) );
            Collection<StorageCommand> commands = new ArrayList<>();
            try ( StorageStatement statement = storageEngine.storeReadLayer().newStatement() )
            {
                storageEngine.createCommands( commands, txState, statement, null, 0 );
            }
            return tx( commands );
        }

        private void verifyIndex( TransactionToApply tx ) throws Exception
        {
            try ( IndexReader reader = index.newReader() )
            {
                NodeVisitor visitor = new NodeVisitor();
                for ( int i = 0; tx != null; i++ )
                {
                    tx.transactionRepresentation().accept( visitor.clear() );

                    Value propertyValue = propertyValue( id, base + i );
                    IndexQuery.ExactPredicate query = IndexQuery.exact( descriptor.getPropertyId(), propertyValue );
                    PrimitiveLongIterator hits = reader.query( query );
                    assertEquals( "Index doesn't contain " + visitor.nodeId + " " + propertyValue,
                            visitor.nodeId, PrimitiveLongCollections.single( hits, -1 ) );
                    tx = tx.next();
                }
            }
        }
    }

    private static class NodeVisitor implements Visitor<StorageCommand,IOException>
    {
        long nodeId;

        @Override
        public boolean visit( StorageCommand element ) throws IOException
        {
            if ( element instanceof NodeCommand )
            {
                nodeId = ((NodeCommand)element).getKey();
            }
            return false;
        }

        public NodeVisitor clear()
        {
            nodeId = -1;
            return this;
        }
    }
}
