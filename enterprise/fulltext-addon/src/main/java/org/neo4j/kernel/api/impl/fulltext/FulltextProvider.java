/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
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
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

/**
 * Provider class that manages and provides fulltext indices. This is the main entry point for the fulltext addon.
 */
public class FulltextProvider implements AutoCloseable
{
    public static final String LUCENE_FULLTEXT_ADDON_PREFIX = "__lucene__fulltext__addon__";
    public static final String FIELD_ENTITY_ID = LUCENE_FULLTEXT_ADDON_PREFIX + "internal__id__";

    private final GraphDatabaseService db;
    private final Log log;
    private final TransactionIdStore transactionIdStore;
    private final FulltextTransactionEventUpdater fulltextTransactionEventUpdater;
    private final Set<String> nodeProperties;
    private final Set<String> relationshipProperties;
    private final Map<String,WritableFulltext> writableNodeIndices;
    private final Map<String,WritableFulltext> writableRelationshipIndices;
    private final FulltextUpdateApplier applier;
    private final ReadWriteLock configurationLock;

    /**
     * Creates a provider of fulltext indices for the given database. This is the entry point for all fulltext index operations.
     * @param db Database that this provider should work with.
     * @param log For logging errors.
     * @param availabilityGuard Used for waiting with populating the index until the database is available.
     * @param scheduler For background work.
     * @param transactionIdStore Used for checking if the store has had transactions applied to it, while the fulltext
     * indexes have been disabled. If so, then the indexes will be rebuilt.
     */
    public FulltextProvider( GraphDatabaseService db, Log log, AvailabilityGuard availabilityGuard,
                             JobScheduler scheduler, TransactionIdStore transactionIdStore )
    {
        this.db = db;
        this.log = log;
        this.transactionIdStore = transactionIdStore;
        applier = new FulltextUpdateApplier( log, availabilityGuard, scheduler );
        applier.start();
        fulltextTransactionEventUpdater = new FulltextTransactionEventUpdater( this, log, applier );
        nodeProperties = ConcurrentHashMap.newKeySet();
        relationshipProperties = ConcurrentHashMap.newKeySet();
        writableNodeIndices = new ConcurrentHashMap<>();
        writableRelationshipIndices = new ConcurrentHashMap<>();
        configurationLock = new ReentrantReadWriteLock( true );
    }

    public void init() throws IOException
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
        return storedConfig == null && index.getProperties().isEmpty() || storedConfig != null && storedConfig.equals( currentConfig );
    }

    /**
     * Wait for the asynchronous background population, if one is on-going, to complete.
     *
     * Such population, where the entire store is scanned for data to write to the index, will be started if the index
     * needs to recover after an unclean shut-down, or a configuration change.
     * @throws IOException If it was not possible to wait for the population to finish, for some reason.
     */
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

    void register( LuceneFulltext fulltextIndex ) throws IOException
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

    /**
     * Returns a reader for the specified index.
     *
     * @param identifier Identifier for the index.
     * @param type Type of the index.
     * @return A {@link ReadOnlyFulltext} for the index, or null if no such index is found.
     * @throws IOException
     */
    public ReadOnlyFulltext getReader( String identifier, FulltextIndexType type ) throws IOException
    {
        if ( type == FulltextIndexType.NODES )
        {
            return writableNodeIndices.get( identifier ).getIndexReader();
        }
        else
        {
            return writableRelationshipIndices.get( identifier ).getIndexReader();
        }
    }

    public Set<String> getProperties( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, WritableFulltext::getProperties );
    }

    private <E> E applyToMatchingIndex( String identifier, FulltextIndexType type, Function<WritableFulltext,E> function )
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

    /**
     * Fulltext index type.
     */
    public enum FulltextIndexType
    {
        NODES
                {
                    @Override
                    public String toString()
                    {
                        return "Nodes";
                    }
                },
        RELATIONSHIPS
                {
                    @Override
                    public String toString()
                    {
                        return "Relationships";
                    }
                }
    }
}
