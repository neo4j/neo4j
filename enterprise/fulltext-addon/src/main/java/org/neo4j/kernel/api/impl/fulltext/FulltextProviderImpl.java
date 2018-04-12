/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    private final GraphDatabaseService db;
    private final Log log;
    private final TransactionIdStore transactionIdStore;
    private final FulltextTransactionEventUpdater fulltextTransactionEventUpdater;
    private final Set<String> nodeProperties;
    private final Set<String> relationshipProperties;
    private final Map<String,WritableFulltext> writableNodeIndices;
    private final Map<String,WritableFulltext> writableRelationshipIndices;
    private final FulltextUpdateApplier applier;
    private final FulltextFactory factory;
    private final ReadWriteLock configurationLock;

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
        applier = new FulltextUpdateApplier( log, availabilityGuard, scheduler );
        applier.start();
        factory = new FulltextFactory( fileSystem, storeDir, analyzerClassName );
        fulltextTransactionEventUpdater = new FulltextTransactionEventUpdater( this, applier );
        nodeProperties = ConcurrentHashMap.newKeySet();
        relationshipProperties = ConcurrentHashMap.newKeySet();
        writableNodeIndices = new ConcurrentHashMap<>();
        writableRelationshipIndices = new ConcurrentHashMap<>();
        configurationLock = new ReentrantReadWriteLock( true );
    }

    @Override
    public void registerTransactionEventHandler()
    {
        db.registerTransactionEventHandler( fulltextTransactionEventUpdater );
    }

    private boolean matchesConfiguration( WritableFulltext index ) throws IOException
    {
        long txId = transactionIdStore.getLastCommittedTransactionId();
        FulltextIndexConfiguration currentConfig =
                new FulltextIndexConfiguration( index.getAnalyzerName(), index.getProperties(), txId );

        FulltextIndexConfiguration storedConfig;
        try ( ReadOnlyFulltext indexReader = index.getIndexReader() )
        {
            storedConfig = indexReader.getConfigurationDocument();
        }
        return storedConfig == null && index.getProperties().isEmpty() ||
               storedConfig != null && storedConfig.equals( currentConfig );
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
        LuceneFulltext index = factory.openFulltextIndex( identifier, type );
        register( index );
    }

    @Override
    public void createIndex( String identifier, FulltextIndexType type, List<String> properties )
            throws IOException
    {
        LuceneFulltext index = factory.createFulltextIndex( identifier, type, properties );
        register( index );
    }

    private void register( LuceneFulltext fulltextIndex ) throws IOException
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
                writableNodeIndices.put( fulltextIndex.getIdentifier(), writableFulltext );
                nodeProperties.addAll( fulltextIndex.getProperties() );
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
                writableRelationshipIndices.put( fulltextIndex.getIdentifier(), writableFulltext );
                relationshipProperties.addAll( fulltextIndex.getProperties() );
            }
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    String[] getNodeProperties()
    {
        return nodeProperties.toArray( new String[0] );
    }

    String[] getRelationshipProperties()
    {
        return relationshipProperties.toArray( new String[0] );
    }

    Collection<WritableFulltext> writableNodeIndices()
    {
        return Collections.unmodifiableCollection( writableNodeIndices.values() );
    }

    Collection<WritableFulltext> writableRelationshipIndices()
    {
        return Collections.unmodifiableCollection( writableRelationshipIndices.values() );
    }

    @Override
    public ReadOnlyFulltext getReader( String identifier, FulltextIndexType type ) throws IOException
    {
        WritableFulltext writableFulltext = getIndexMap( type ).get( identifier );
        if ( writableFulltext == null )
        {
            throw new IllegalArgumentException( "No such " + type + " index '" + identifier + "'." );
        }
        return writableFulltext.getIndexReader();
    }

    private Map<String,WritableFulltext> getIndexMap( FulltextIndexType type )
    {
        switch ( type )
        {
        case NODES:
            return writableNodeIndices;
        case RELATIONSHIPS:
            return writableRelationshipIndices;
        default:
            throw new IllegalArgumentException( "No such fulltext index type: " + type );
        }
    }

    @Override
    public Set<String> getProperties( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, WritableFulltext::getProperties );
    }

    private <E> E applyToMatchingIndex(
            String identifier, FulltextIndexType type, Function<WritableFulltext,E> function )
    {
        if ( type == FulltextIndexType.NODES )
        {
            return function.apply( writableNodeIndices.get( identifier ) );
        }
        else
        {
            return function.apply( writableRelationshipIndices.get( identifier ) );
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
                writableNodeIndices.remove( identifier ).drop();
            }
            else
            {
                writableRelationshipIndices.remove( identifier ).drop();
            }
            rebuildProperties();
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    private void rebuildProperties()
    {
        nodeProperties.clear();
        relationshipProperties.clear();
        writableNodeIndices.forEach( ( s, index ) -> nodeProperties.addAll( index.getProperties() ) );
        writableRelationshipIndices.forEach( ( s, index ) -> relationshipProperties.addAll( index.getProperties() ) );
    }

    Lock readLockIndexConfiguration()
    {
        Lock lock = configurationLock.readLock();
        lock.lock();
        return lock;
    }

    @Override
    public void changeIndexedProperties( String identifier, FulltextIndexType type, List<String> propertyKeys )
            throws IOException, InvalidArgumentsException
    {
        configurationLock.writeLock().lock();
        try
        {
            if ( propertyKeys.stream().anyMatch( s -> s.startsWith( FulltextProvider.LUCENE_FULLTEXT_ADDON_PREFIX ) ) )
            {
                throw new InvalidArgumentsException(
                        "It is not possible to index property keys starting with " +
                        FulltextProvider.LUCENE_FULLTEXT_ADDON_PREFIX );
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

    private Resource snapshotStoreFiles( Collection<StoreFileMetadata> files ) throws IOException
    {
        final Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        for ( WritableFulltext index : Iterables.concat( writableNodeIndices.values(), writableRelationshipIndices.values() ) )
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
        // Intentionally don't close the snapshots here, return them for closing by the consumer of the targetFiles list.
        return new MultiResource( snapshots );
    }

    /**
     * Closes the provider and all associated resources.
     */
    @Override
    public void close()
    {
        db.unregisterTransactionEventHandler( fulltextTransactionEventUpdater );
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
        writableNodeIndices.values().forEach( fulltextCloser );
        writableRelationshipIndices.values().forEach( fulltextCloser );
    }

}
