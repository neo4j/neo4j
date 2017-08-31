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

public class FulltextProvider implements AutoCloseable
{
    private static FulltextProvider instance;
    private final GraphDatabaseService db;
    private final FulltextTransactionEventUpdater fulltextTransactionEventUpdater;
    private boolean closed;
    private Set<String> nodeProperties;
    private Set<String> relationshipProperties;
    private Map<String,LuceneFulltext> nodeIndices;
    private Map<String,LuceneFulltext> relationshipIndices;

    private FulltextProvider( GraphDatabaseService db )
    {
        this.db = db;
        closed = false;
        fulltextTransactionEventUpdater = new FulltextTransactionEventUpdater( this );
        db.registerTransactionEventHandler( fulltextTransactionEventUpdater );
        nodeProperties = new HashSet<>();
        relationshipProperties = new HashSet<>();
        nodeIndices = new HashMap<>();
        relationshipIndices = new HashMap<>();
    }

    public static synchronized FulltextProvider instance( GraphDatabaseService db )
    {
        if ( instance == null || instance.closed )
        {
            instance = new FulltextProvider( db );
        }
        return instance;
    }

    @Override
    public synchronized void close()
    {
        if ( !closed )
        {
            closed = true;
            db.unregisterTransactionEventHandler( fulltextTransactionEventUpdater );
            nodeIndices.values().forEach( luceneFulltextHelper ->
            {
                try
                {
                    luceneFulltextHelper.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            } );
            relationshipIndices.values().forEach( luceneFulltextHelper ->
            {
                try
                {
                    luceneFulltextHelper.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            } );
        }
    }

    public synchronized void register( LuceneFulltext fulltextHelper ) throws IOException
    {
        fulltextHelper.open();
        if ( fulltextHelper.getType() == FulltextFactory.FULLTEXT_HELPER_TYPE.NODES )
        {
            nodeIndices.put( fulltextHelper.getIdentifier(), fulltextHelper );
            nodeProperties.addAll( fulltextHelper.getProperties() );
        }
        else
        {
            relationshipIndices.put( fulltextHelper.getIdentifier(), fulltextHelper );
            relationshipProperties.addAll( fulltextHelper.getProperties() );
        }
    }

    public String[] getNodeProperties()
    {
        return nodeProperties.toArray( new String[0] );
    }

    public String[] getRelationshipProperties()
    {
        return relationshipProperties.toArray( new String[0] );
    }

    public Set<WritableDatabaseFulltext> nodeIndices()
    {
        return nodeIndices.values().stream().map( WritableDatabaseFulltext::new ).collect( Collectors.toSet() );
    }

    public Set<WritableDatabaseFulltext> relationshipIndices()
    {
        return relationshipIndices.values().stream().map( WritableDatabaseFulltext::new ).collect( Collectors.toSet() );
    }

    public FulltextReader getReader( String identifier, FulltextFactory.FULLTEXT_HELPER_TYPE type ) throws IOException
    {
        if ( type == FulltextFactory.FULLTEXT_HELPER_TYPE.NODES )
        {
            return nodeIndices.get( identifier ).getIndexReader();
        }
        else
        {
            return relationshipIndices.get( identifier ).getIndexReader();
        }
    }
}
