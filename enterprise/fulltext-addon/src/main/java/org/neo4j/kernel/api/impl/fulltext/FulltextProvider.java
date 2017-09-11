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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

/**
 * Provider class that manages and provides fulltext indices. This is the main entry point for the fulltext addon.
 */
public class FulltextProvider implements AutoCloseable
{
    private static FulltextProvider instance;
    private final GraphDatabaseService db;
    private Log log;
    private final FulltextTransactionEventUpdater fulltextTransactionEventUpdater;
    private boolean closed;
    private Set<String> nodeProperties;
    private Set<String> relationshipProperties;
    private Map<String,LuceneFulltext> nodeIndices;
    private Map<String,LuceneFulltext> relationshipIndices;

    private FulltextProvider( GraphDatabaseService db, Log log )
    {
        this.db = db;
        this.log = log;
        closed = false;
        fulltextTransactionEventUpdater = new FulltextTransactionEventUpdater( this );
        db.registerTransactionEventHandler( fulltextTransactionEventUpdater );
        nodeProperties = new HashSet<>();
        relationshipProperties = new HashSet<>();
        nodeIndices = new HashMap<>();
        relationshipIndices = new HashMap<>();
    }

    /**
     * Fetch the current instance of the provider. If there is none, create one associated with the given database.
     * @param db Database used for eventual creation of the provider.
     * @param logService
     * @return The instance for the given database.
     */
    public static synchronized FulltextProvider instance( GraphDatabaseService db, LogService logService )
    {
        if ( instance == null || instance.closed )
        {
            instance = new FulltextProvider( db, logService.getInternalLog( FulltextProvider.class ) );
        }
        return instance;
    }

    /**
     * Closes the provider and all associated resources.
     */
    @Override
    public synchronized void close()
    {
        if ( !closed )
        {
            closed = true;
            db.unregisterTransactionEventHandler( fulltextTransactionEventUpdater );
            nodeIndices.values().forEach( luceneFulltextIndex ->
            {
                try
                {
                    luceneFulltextIndex.close();
                }
                catch ( IOException e )
                {
                    log.error( "Unable to close fulltext node index.", e );
                }
            } );
            relationshipIndices.values().forEach( luceneFulltextIndex ->
            {
                try
                {
                    luceneFulltextIndex.close();
                }
                catch ( IOException e )
                {
                    log.error( "Unable to close fulltext relationship index.", e );
                }
            } );
        }
    }

    synchronized void register( LuceneFulltext fulltextIndex ) throws IOException
    {
        fulltextIndex.open();
        if ( fulltextIndex.getType() == FulltextIndexType.NODES )
        {
            nodeIndices.put( fulltextIndex.getIdentifier(), fulltextIndex );
            nodeProperties.addAll( fulltextIndex.getProperties() );
        }
        else
        {
            relationshipIndices.put( fulltextIndex.getIdentifier(), fulltextIndex );
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

    Set<WritableFulltext> writableNodeIndices()
    {
        return nodeIndices.values().stream().map( WritableFulltext::new ).collect( Collectors.toSet() );
    }

    Set<WritableFulltext> writableRelationshipIndices()
    {
        return relationshipIndices.values().stream().map( WritableFulltext::new ).collect( Collectors.toSet() );
    }

    /**
     * Returns a reader for the specified index.
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
