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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.internal.batchimport.cache.idmapping.string.Workers;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Commands;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RecordStorageEngineRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.helpers.TimeUtil.parseTimeMillis;
import static org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider.DESCRIPTOR;
import static org.neo4j.kernel.impl.transaction.log.Commitment.NO_COMMITMENT;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;

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

    private final LabelSchemaDescriptor descriptor = SchemaDescriptor.forLabel( 0, 0 );

    @Test
    public void shouldApplyIndexUpdatesInWorkSyncedBatches() throws Exception
    {
        // GIVEN
        long duration = parseTimeMillis.apply( System.getProperty( getClass().getName() + ".duration", "2s" ) );
        int numThreads = Integer.getInteger( getClass().getName() + ".numThreads",
                Runtime.getRuntime().availableProcessors() );
        DefaultFileSystemAbstraction fs = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        CollectingIndexUpdateListener index = new CollectingIndexUpdateListener();
        RecordStorageEngine storageEngine = storageEngineRule
                .getWith( fs, pageCache, directory.databaseLayout() )
                .indexUpdateListener( index )
                .build();
        storageEngine.apply( tx( singletonList( Commands.createIndexRule( DESCRIPTOR, 1, descriptor ) ) ), EXTERNAL );

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
        workers.awaitAndThrowOnError();
    }

    private static Value propertyValue( int id, int progress )
    {
        return Values.of( id + "_" + progress );
    }

    private static TransactionToApply tx( Collection<StorageCommand> commands )
    {
        PhysicalTransactionRepresentation txRepresentation = new PhysicalTransactionRepresentation( commands, new byte[0], -1, -1, -1, -1, -1, -1 );
        TransactionToApply tx = new TransactionToApply( txRepresentation );
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
        private final CollectingIndexUpdateListener index;
        private final CommandCreationContext commandCreationContext;
        private int i;
        private int base;

        Worker( int id, AtomicBoolean end, RecordStorageEngine storageEngine, int batchSize, CollectingIndexUpdateListener index )
        {
            this.id = id;
            this.end = end;
            this.storageEngine = storageEngine;
            this.batchSize = batchSize;
            this.index = index;
            NeoStores neoStores = this.storageEngine.testAccessNeoStores();
            this.nodeIds = neoStores.getNodeStore();
            this.commandCreationContext = storageEngine.newCommandCreationContext();
        }

        @Override
        public void run()
        {
            try ( StorageReader reader = storageEngine.newReader();
                  CommandCreationContext creationContext = storageEngine.newCommandCreationContext() )
            {
                TransactionQueue queue = new TransactionQueue( batchSize, ( tx, last ) ->
                {
                    // Apply
                    storageEngine.apply( tx, EXTERNAL );

                    // And verify that all nodes are in the index
                    verifyIndex( tx );
                    base += batchSize;
                } );
                for ( ; !end.get(); i++ )
                {
                    queue.queue( createNodeAndProperty( i, reader, creationContext ) );
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

        private TransactionToApply createNodeAndProperty( int progress, StorageReader reader, CommandCreationContext creationContext ) throws Exception
        {
            TransactionState txState = new TxState();
            long nodeId = nodeIds.nextId();
            txState.nodeDoCreate( nodeId );
            txState.nodeDoAddLabel( descriptor.getLabelId(), nodeId );
            txState.nodeDoAddProperty( nodeId, descriptor.getPropertyId(), propertyValue( id, progress ) );
            Collection<StorageCommand> commands = new ArrayList<>();
            storageEngine.createCommands( commands, txState, reader, creationContext, null, 0, NO_DECORATION );
            return tx( commands );
        }

        private void verifyIndex( TransactionToApply tx ) throws Exception
        {
            NodeVisitor visitor = new NodeVisitor();
            for ( int i = 0; tx != null; i++ )
            {
                tx.transactionRepresentation().accept( visitor.clear() );

                Value propertyValue = propertyValue( id, base + i );
                index.assertHasIndexEntry( propertyValue, visitor.nodeId );
                tx = tx.next();
            }
        }
    }

    private static class NodeVisitor implements Visitor<StorageCommand,IOException>
    {
        long nodeId;

        @Override
        public boolean visit( StorageCommand element )
        {
            if ( element instanceof NodeCommand )
            {
                nodeId = ((NodeCommand) element).getKey();
            }
            return false;
        }

        public NodeVisitor clear()
        {
            nodeId = -1;
            return this;
        }
    }

    private static class CollectingIndexUpdateListener extends IndexUpdateListener.Adapter
    {
        // Only one index assumed
        private final ConcurrentMap<Value,Set<Long>> index = new ConcurrentHashMap<>();

        @Override
        public void applyUpdates( Iterable<IndexEntryUpdate<SchemaDescriptor>> updates )
        {
            updates.forEach( update ->
            {
                // Only additions assumed
                assert update.updateMode() == UpdateMode.ADDED;
                index.computeIfAbsent( update.values()[0], value -> ConcurrentHashMap.newKeySet() ).add( update.getEntityId() );
            } );
        }

        void assertHasIndexEntry( Value value, long entityId )
        {
            assertTrue( index.getOrDefault( value, emptySet() ).contains( entityId ) );
        }
    }
}
