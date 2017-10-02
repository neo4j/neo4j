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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private final Map<String,LuceneFulltext> nodeIndices;
    private final Map<String,LuceneFulltext> relationshipIndices;
    private final FulltextUpdateApplier applier;

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
        nodeProperties = new HashSet<>();
        relationshipProperties = new HashSet<>();
        writableNodeIndices = new HashMap<>();
        writableRelationshipIndices = new HashMap<>();
        nodeIndices = new HashMap<>();
        relationshipIndices = new HashMap<>();
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
    public void awaitPopulation() throws Exception
    {
        applier.writeBarrier().awaitCompletion();
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
            nodeIndices.put( fulltextIndex.getIdentifier(), fulltextIndex );
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
            relationshipIndices.put( fulltextIndex.getIdentifier(), fulltextIndex );
            writableRelationshipIndices.put( fulltextIndex.getIdentifier(), writableFulltext );
            relationshipProperties.addAll( fulltextIndex.getProperties() );
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
            return nodeIndices.get( identifier ).getIndexReader();
        }
        else
        {
            return relationshipIndices.get( identifier ).getIndexReader();
        }
    }

    public Set<String> getProperties( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, LuceneFulltext::getProperties );
    }

    private <E> E applyToMatchingIndex( String identifier, FulltextIndexType type, Function<LuceneFulltext,E> function )
    {
        if ( type == FulltextIndexType.NODES )
        {
            return function.apply( nodeIndices.get( identifier ) );
        }
        else
        {
            return function.apply( relationshipIndices.get( identifier ) );
        }
    }

    public InternalIndexState getState( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, LuceneFulltext::getState );
    }

    public void drop( String identifier, FulltextIndexType type ) throws IOException
    {
        if ( type == FulltextIndexType.NODES )
        {
            nodeIndices.remove( identifier ).drop();
            writableNodeIndices.remove( identifier ).close();
        }
        else
        {
            relationshipIndices.remove( identifier ).drop();
            writableRelationshipIndices.remove( identifier ).close();
        }
        rebuildProperties();
    }

    private void rebuildProperties()
    {
        nodeProperties.clear();
        relationshipProperties.clear();
        nodeIndices.forEach( ( s, index ) -> nodeProperties.addAll( index.getProperties() ) );
        relationshipIndices.forEach( ( s, index ) -> relationshipProperties.addAll( index.getProperties() ) );
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
