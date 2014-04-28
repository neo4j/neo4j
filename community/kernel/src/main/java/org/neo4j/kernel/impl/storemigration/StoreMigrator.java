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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader.ReusableRelationship;

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
            legacyStore.copyLegacyIndexStoreFile( neoStore.getStorageFileName().getParentFile() );
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

            final ArrayBlockingQueue<RelChainBuilder> chainsToWrite = new ArrayBlockingQueue<>( 1024 );
            final AtomicReference<Throwable> writerException = new AtomicReference<>();

            Thread writerThread = new RelationshipWriter( chainsToWrite, neoStore.getDenseNodeThreshold(), nodeStore,
                    relationshipStore, relGroupStore, nodeReader, writerException );
            writerThread.start();

            try
            {
                // Determined through testing to be a reasonable weigh-off between risk/benefit
                final int maxSimultaneousNodes = (int) (120 * (Runtime.getRuntime().totalMemory() / (1024 * 1024)));
                final AtomicBoolean morePassesRequired = new AtomicBoolean(false);
                final AtomicLong firstRelationshipRequiringANewPass = new AtomicLong(0l);
                final PrimitiveLongObjectMap<RelChainBuilder> relChains = Primitive.longObjectMap();
                long numberOfPasses = 1;
                do
                {
                    percentComplete = 0;
                    if(morePassesRequired.get())
                    {
                        if(numberOfPasses == 1)
                        {
                            System.out.println("\nNote: Was not able to do single-pass upgrade due to highly " +
                                    "dispersed relationships across the store. Will need to perform multi-pass upgrade.\n" +
                                    "Note: Dotted line shows progress for each pass, the X in the dotted line shows total progress.\n");
                        }
                        else
                        {
                            System.out.println( " [MultiPass Upgrade] Finished pass #" + (numberOfPasses-1) );
                        }
                        numberOfPasses++;
                    }

                    relReader.accept( firstRelationshipRequiringANewPass.get(),
                            new Visitor<ReusableRelationship, RuntimeException>()
                    {
                        private final boolean isMultiPass = morePassesRequired.getAndSet( false );

                        @Override
                        public boolean visit( ReusableRelationship rel )
                        {
                            reportProgress( rel.id() );
                            if ( rel.inUse() )
                            {
                                if(appendToRelChain( rel.getFirstNode(), rel.getFirstPrevRel(),
                                        rel.getFirstNextRel(), rel ))
                                {
                                    return true;
                                }

                                if(appendToRelChain( rel.getSecondNode(), rel.getSecondPrevRel(),
                                        rel.getSecondNextRel(), rel ))
                                {
                                    return true;
                                }
                            }
                            return false;
                        }

                        private boolean appendToRelChain( long nodeId, long prevRel, long nextRel,
                                                       ReusableRelationship rel )
                        {
                            RelChainBuilder chain = relChains.get( nodeId );

                            if ( chain == null )
                            {
                                if ( morePassesRequired.get() || (isMultiPass && nodeStore.inUse( nodeId )) )
                                {
                                    // Handled in a previous pass, ignore.
                                    return false;
                                }

                                if ( relChains.size() >= maxSimultaneousNodes )
                                {
                                    morePassesRequired.set( true );
                                    firstRelationshipRequiringANewPass.set( rel.id() );
                                    System.out.print( "X" );
                                    return false;
                                }

                                chain = new RelChainBuilder( nodeId );
                                relChains.put( nodeId, chain );
                            }

                            chain.append( rel.createRecord(), prevRel, nextRel );

                            if ( chain.isComplete() )
                            {
                                assertNoWriterException( writerException );
                                try
                                {
                                    RelChainBuilder remove = relChains.remove( nodeId );
                                    chainsToWrite.put( remove );
                                }
                                catch ( InterruptedException e )
                                {
                                    Thread.interrupted();
                                    throw new RuntimeException( "Interrupted while reading relationships.", e );
                                }
                            }

                            return false;
                        }
                    } );

                } while(morePassesRequired.get());

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
            int newPercent = totalEntities == 0 ? 100 : (int) (id * 100 / (totalEntities));
            while( newPercent > percentComplete )
            {
                percentComplete++;
                progressMonitor.percentComplete( percentComplete );
            }
        }
    }
}
