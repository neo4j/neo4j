/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

/**
 * Migrates a neo4j database from one version to the next. Instantiated with a {@link LegacyStore}
 * representing the old version and a {@link NeoStore} representing the new version.
 *
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 */
public class StoreMigrator
{
    // Developers: There is a benchmark, storemigrate-benchmark, that generates large stores and benchmarks
    // the upgrade process. Please utilize that when writing upgrade code to ensure the code is fast enough to
    // complete upgrades in a reasonable time period.

    private final MigrationProgressMonitor progressMonitor;

    public StoreMigrator( MigrationProgressMonitor progressMonitor )
    {
        this.progressMonitor = progressMonitor;
    }

    public void migrate( LegacyStore legacyStore, NeoStore neoStore ) throws IOException
    {
        progressMonitor.started();
        new Migration( legacyStore, neoStore ).migrate();
        progressMonitor.finished();
    }

    protected class Migration
    {
        private final LegacyStore legacyStore;
        private final NeoStore neoStore;
        private final long totalEntities;
        private int percentComplete;

        public Migration( LegacyStore legacyStore, NeoStore neoStore )
        {
            this.legacyStore = legacyStore;
            this.neoStore = neoStore;
            totalEntities = legacyStore.getRelStoreReader().getMaxId();
        }

        private void migrate() throws IOException
        {
            // Migrate
            migrateNodesAndRelationships();

            // Close
            neoStore.close();
            legacyStore.close();

            // Just copy unchanged stores that doesn't need migration
            legacyStore.copyNeoStore( neoStore );
            legacyStore.copyRelationshipTypeTokenStore( neoStore );
            legacyStore.copyRelationshipTypeTokenNameStore( neoStore );
            legacyStore.copyDynamicStringPropertyStore( neoStore );
            legacyStore.copyDynamicArrayPropertyStore( neoStore );
            legacyStore.copyPropertyStore( neoStore );
            legacyStore.copyPropertyKeyTokenStore( neoStore );
            legacyStore.copyPropertyKeyTokenNameStore( neoStore );
            legacyStore.copyLabelTokenStore( neoStore );
            legacyStore.copyLabelTokenNameStore( neoStore );
            legacyStore.copyNodeLabelStore( neoStore );
            legacyStore.copySchemaStore( neoStore );
        }

        private void migrateNodesAndRelationships() throws IOException
        {
            /* For each node
             *   load the full relationship chain into memory
             *   if ( more than THRESHOLD relationships )
             *      store in dense node way
             *   else
             *      store in normal way
             *
             * Keep ids */

            final NodeStore nodeStore = neoStore.getNodeStore();
            final RelationshipStore relationshipStore = neoStore.getRelationshipStore();
            final RelationshipGroupStore relGroupStore = neoStore.getRelationshipGroupStore();
            final LegacyNodeStoreReader nodeReader = legacyStore.getNodeStoreReader();
            final LegacyRelationshipStoreReader relReader = legacyStore.getRelStoreReader();
            nodeStore.setHighId( nodeReader.getMaxId() );
            relationshipStore.setHighId( relReader.getMaxId() );

            final ArrayBlockingQueue<RelChainBuilder> chainsToWrite = new ArrayBlockingQueue<>( 24 );
            final AtomicReference<Throwable> writerException = new AtomicReference<>();

            Thread writerThread = new RelationshipWriter( chainsToWrite, neoStore.getDenseNodeThreshold(), nodeStore,
                    relationshipStore, relGroupStore, nodeReader, writerException );
            writerThread.start();

            try
            {

                final Map<Long, RelChainBuilder> relChains = new HashMap<>();
                relReader.accept( new LegacyRelationshipStoreReader.Visitor()
                {
                    @Override
                    public void visit( long id, RelationshipRecord record )
                    {
                        reportProgress( id );
                        if ( record.inUse() )
                        {
                            appendToRelChain( record.getFirstNode(), record.getFirstPrevRel(),
                                    record.getFirstNextRel(), record );
                            appendToRelChain( record.getSecondNode(), record.getSecondPrevRel(),
                                    record.getSecondNextRel(), record );
                        }
                    }

                    private void appendToRelChain( long nodeId, long prevRel, long nextRel, RelationshipRecord record )
                    {
                        RelChainBuilder chain = relChains.get( nodeId );
                        if ( chain == null )
                        {
                            chain = new RelChainBuilder( nodeId );
                            relChains.put( nodeId, chain );
                        }

                        chain.append( record, prevRel, nextRel );

                        if ( chain.isComplete() )
                        {
                            assertNoWriterException( writerException );
                            try
                            {
                                chainsToWrite.put( relChains.remove( nodeId ) );
                            }
                            catch ( InterruptedException e )
                            {
                                Thread.interrupted();
                                throw new RuntimeException( "Interrupted while reading relationships.", e );
                            }
                        }
                    }
                } );

                try
                {
                    chainsToWrite.put( new RelChainBuilder( -1 ) );
                    writerThread.join();
                    assertNoWriterException( writerException );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( "Interrupted.", e);
                }

                legacyStore.copyNodeStoreIdFile( neoStore );
                legacyStore.copyRelationshipStoreIdFile( neoStore );

                // Migrate nodes with no relationships
                nodeReader.accept(new LegacyNodeStoreReader.Visitor()
                {
                    @Override
                    public void visit( NodeRecord record )
                    {
                        if(record.inUse() && record.getNextRel() == Record.NO_NEXT_RELATIONSHIP.intValue())
                        {
                            nodeStore.forceUpdateRecord( record );
                        }
                    }
                });
            }
            finally
            {
                nodeReader.close();
                relReader.close();
            }
        }

        private void assertNoWriterException( AtomicReference<Throwable> writerException )
        {
            if(writerException.get() != null)
            {
                throw new RuntimeException( writerException.get() );
            }
        }

        private void reportProgress( long id )
        {
            int newPercent = totalEntities == 0 ? 100 : (int) ((id+1) * 100 / totalEntities);
            if ( newPercent > percentComplete )
            {
                percentComplete = newPercent;
                progressMonitor.percentComplete( percentComplete );
            }
        }
    }
}
