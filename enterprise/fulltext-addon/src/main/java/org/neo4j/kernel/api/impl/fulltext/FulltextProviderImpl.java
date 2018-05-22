/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.impl.util.MultiResource;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing.getSnapshotFilesMetadata;

/**
 * Provider class that manages and provides fulltext indices. This is the main entry point for the fulltext addon.
 */
public class FulltextProviderImpl implements FulltextProvider
{
    private final static int ASIDE = 0;
    private final static int BSIDE = 1;

    private final GraphDatabaseService db;
    private final Log log;
    private final TransactionIdStore transactionIdStore;
    private final Map<String,WritableFulltext>[] writableNodeIndices;
    private final Map<String,WritableFulltext>[] writableRelationshipIndices;
    private final FulltextUpdateApplier applier;
    private final FulltextFactory factory;
    private final ReadWriteLock configurationLock;
    private static final JobScheduler.Group FLIPPER = new JobScheduler.Group( "FulltextIndexSideFlipper" );
    private final JobScheduler.JobHandle flipperJob;
    private volatile boolean flipper;
    private volatile int side;

    /**
     * Creates a provider of fulltext indices for the given database. This is the entry point for all fulltext index
     * operations.
     * @param db Database that this provider should work with.
     * @param log For logging errors.
     * @param availabilityGuard Used for waiting with populating the index until the database is available.
     * @param scheduler For background work.
     * @param transactionIdStore Used for checking if the store has had transactions applied to it, while the fulltext
     * @param fileSystem The filesystem to use.
     * @param storeDir Store directory of the database.
     * @param analyzerClassName The Lucene analyzer to use for the {@link LuceneFulltext} created by this factory.
     */
    public FulltextProviderImpl( GraphDatabaseService db, Log log, AvailabilityGuard availabilityGuard, JobScheduler scheduler,
            TransactionIdStore transactionIdStore, FileSystemAbstraction fileSystem, File storeDir, String analyzerClassName )
    {
        this.db = db;
        this.log = log;
        this.transactionIdStore = transactionIdStore;
        JobScheduler jobScheduler = scheduler;
        applier = new FulltextUpdateApplier( log, availabilityGuard, scheduler );
        applier.start();
        factory = new FulltextFactory( fileSystem, storeDir, analyzerClassName );
        writableNodeIndices = new Map[]{new ConcurrentHashMap<>(), new ConcurrentHashMap<>()};
        writableRelationshipIndices = new Map[]{new ConcurrentHashMap<>(), new ConcurrentHashMap<>()};
        configurationLock = new ReentrantReadWriteLock( true );
        flipper = true;
        flipperJob = jobScheduler.schedule( FLIPPER, () ->
        {
            boolean isAvailable;
            do
            {
                isAvailable = availabilityGuard.isAvailable( 100 );
            }
            while ( !isAvailable && !availabilityGuard.isShutdown() );
            while ( flipper )
            {
                try
                {
                    flip();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        } );
    }

    private boolean matchesConfiguration( WritableFulltext index ) throws IOException
    {
        long txId = transactionIdStore.getLastCommittedTransactionId();
        FulltextIndexConfiguration currentConfig = new FulltextIndexConfiguration( index.getAnalyzerName(), index.getProperties(), txId );

        FulltextIndexConfiguration storedConfig;
        try ( ReadOnlyFulltext indexReader = index.getIndexReader() )
        {
            storedConfig = indexReader.getConfigurationDocument();
        }
        return storedConfig == null && index.getProperties().isEmpty() || storedConfig != null && storedConfig.equals( currentConfig );
    }

    @Override
    public void awaitPopulation()
    {
        try
        {
            applier.writeBarrier().awaitCompletion();
        }
        catch ( ExecutionException e )
        {
            throw new AssertionError( "The writeBarrier operation should never throw an exception", e );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void openIndex( String identifier, FulltextIndexType type ) throws IOException
    {
        LuceneFulltext a = factory.openFulltextIndex( identifier, ASIDE, type );
        register( a, ASIDE );
        LuceneFulltext b = factory.openFulltextIndex( identifier, BSIDE, type );
        register( b, BSIDE );
    }

    @Override
    public void createIndex( String identifier, FulltextIndexType type, List<String> properties ) throws IOException
    {
        LuceneFulltext a = factory.createFulltextIndex( identifier, ASIDE, type, properties );
        register( a, ASIDE );
        LuceneFulltext b = factory.createFulltextIndex( identifier, BSIDE, type, properties );
        register( b, BSIDE );
    }

    private void register( LuceneFulltext fulltextIndex, int side ) throws IOException
    {
        configurationLock.writeLock().lock();
        try
        {
            WritableFulltext writableFulltext = new WritableFulltext( fulltextIndex );
            writableFulltext.open();
            if ( fulltextIndex.getType() == FulltextIndexType.NODES )
            {
                if ( !matchesConfiguration( writableFulltext ) )
                {
                    writableFulltext.drop();
                    writableFulltext.open();
                    if ( !writableFulltext.getProperties().isEmpty() )
                    {
                        applier.populateNodes( writableFulltext, db );
                    }
                }
                writableNodeIndices[side].put( fulltextIndex.getIdentifier(), writableFulltext );
            }
            else
            {
                if ( !matchesConfiguration( writableFulltext ) )
                {
                    writableFulltext.drop();
                    writableFulltext.open();
                    if ( !writableFulltext.getProperties().isEmpty() )
                    {
                        applier.populateRelationships( writableFulltext, db );
                    }
                }
                writableRelationshipIndices[side].put( fulltextIndex.getIdentifier(), writableFulltext );
            }
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    @Override
    public ReadOnlyFulltext getReader( String identifier, FulltextIndexType type ) throws IOException
    {
        //Lock to protect from flips before we register the reader.
        Lock lock = configurationLock.readLock();
        lock.lock();
        try
        {
            WritableFulltext writableFulltext = getIndexMap( type ).get( identifier );
            if ( writableFulltext == null )
            {
                throw new IllegalArgumentException( "No such " + type + " index '" + identifier + "'." );
            }
            return writableFulltext.getIndexReader();
        }
        finally
        {
            lock.unlock();
        }
    }

    private Map<String,WritableFulltext> getIndexMap( FulltextIndexType type )
    {
        switch ( type )
        {
        case NODES:
            return writableNodeIndices[side];
        case RELATIONSHIPS:
            return writableRelationshipIndices[side];
        default:
            throw new IllegalArgumentException( "No such fulltext index type: " + type );
        }
    }

    @Override
    public Set<String> getProperties( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, WritableFulltext::getProperties );
    }

    private <E> E applyToMatchingIndex( String identifier, FulltextIndexType type, Function<WritableFulltext,E> function )
    {
        if ( type == FulltextIndexType.NODES )
        {
            return function.apply( writableNodeIndices[side].get( identifier ) );
        }
        else
        {
            return function.apply( writableRelationshipIndices[side].get( identifier ) );
        }
    }

    @Override
    public InternalIndexState getState( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, WritableFulltext::getState );
    }

    void drop( String identifier, FulltextIndexType type ) throws IOException
    {
        configurationLock.writeLock().lock();
        try
        {
            // Wait for the queue of updates to drain, before deleting an index.
            awaitPopulation();
            if ( type == FulltextIndexType.NODES )
            {
                writableNodeIndices[ASIDE].remove( identifier ).drop();
                writableNodeIndices[BSIDE].remove( identifier ).drop();
            }
            else
            {
                writableRelationshipIndices[ASIDE].remove( identifier ).drop();
                writableRelationshipIndices[BSIDE].remove( identifier ).drop();
            }
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    @Override
    public void changeIndexedProperties( String identifier, FulltextIndexType type, List<String> propertyKeys ) throws IOException, InvalidArgumentsException
    {
        configurationLock.writeLock().lock();
        try
        {
            if ( propertyKeys.stream().anyMatch( s -> s.startsWith( FulltextProvider.LUCENE_FULLTEXT_ADDON_PREFIX ) ) )
            {
                throw new InvalidArgumentsException(
                        "It is not possible to index property keys starting with " + FulltextProvider.LUCENE_FULLTEXT_ADDON_PREFIX );
            }
            Set<String> currentProperties = getProperties( identifier, type );
            if ( !currentProperties.containsAll( propertyKeys ) || !propertyKeys.containsAll( currentProperties ) )
            {
                drop( identifier, type );
                createIndex( identifier, type, propertyKeys );
            }
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    @Override
    public void registerFileListing( NeoStoreFileListing fileListing )
    {
        fileListing.registerStoreFileProvider( this::snapshotStoreFiles );
    }

    @Override
    public void awaitFlip()
    {
        int startSide = side;
        while ( startSide == side )
        {
            synchronized ( this )
            {
                try
                {
                    wait();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void flip() throws IOException
    {
        configurationLock.writeLock().lock();
        try
        {
            applier.writeBarrier().awaitCompletion();
            int currentSide = side;
            List<AsyncFulltextIndexOperation> populations = new ArrayList<>();
            int newSide = (currentSide + 1) % 2;
            for ( WritableFulltext writableFulltext : writableNodeIndices[newSide].values() )
            {
                writableFulltext.awaitNoReaders();
                writableFulltext.drop();
                writableFulltext.open();
                populations.add( applier.populateNodes( writableFulltext, db ) );
            }
            for ( WritableFulltext writableFulltext : writableRelationshipIndices[newSide].values() )
            {
                writableFulltext.awaitNoReaders();
                writableFulltext.drop();
                writableFulltext.open();
                populations.add( applier.populateRelationships( writableFulltext, db ) );
            }

            for ( AsyncFulltextIndexOperation population : populations )
            {
                population.awaitCompletion();
            }
            side = newSide;
            synchronized ( this )
            {
                notifyAll();
            }
        }
        catch ( ExecutionException | InterruptedException e )
        {
            throw new IOException( "Unable to flip fulltext indexes", e );
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    private Resource snapshotStoreFiles( Collection<StoreFileMetadata> files ) throws IOException
    {
        final Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        Lock lock = configurationLock.readLock();
        lock.lock();
        try
        {
            for ( WritableFulltext index : Iterables.concat( writableNodeIndices[ASIDE].values(), writableNodeIndices[BSIDE].values(),
                    writableRelationshipIndices[ASIDE].values(), writableRelationshipIndices[BSIDE].values() ) )
            {
                // Save the last committed transaction, then drain the update queue to make sure that we have applied _at least_ the commits the config claims.
                index.saveConfiguration( transactionIdStore.getLastCommittedTransactionId() );
                try
                {
                    applier.writeBarrier().awaitCompletion();
                }
                catch ( ExecutionException e )
                {
                    throw new IOException( "Unable to prepare index for snapshot." );
                }
                index.flush();
                ResourceIterator<File> snapshot = index.snapshot();
                snapshots.add( snapshot );
                files.addAll( getSnapshotFilesMetadata( snapshot ) );
            }
        }
        finally
        {
            lock.unlock();
        }
        // Intentionally don't close the snapshots here, return them for closing by the consumer of the targetFiles list.
        return new MultiResource( snapshots );
    }

    /**
     * Closes the provider and all associated resources.
     */
    @Override
    public void close()
    {
        flipper = false;
        try
        {
            flipperJob.waitTermination();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            e.printStackTrace();
        }
        applier.stop();
        Consumer<WritableFulltext> fulltextCloser = luceneFulltextIndex ->
        {
            try
            {
                luceneFulltextIndex.saveConfiguration( transactionIdStore.getLastCommittedTransactionId() );
                luceneFulltextIndex.close();
            }
            catch ( IOException e )
            {
                log.error( "Unable to close fulltext index.", e );
            }
        };
        writableNodeIndices[ASIDE].values().forEach( fulltextCloser );
        writableNodeIndices[BSIDE].values().forEach( fulltextCloser );
        writableRelationshipIndices[ASIDE].values().forEach( fulltextCloser );
        writableRelationshipIndices[BSIDE].values().forEach( fulltextCloser );
    }
}
