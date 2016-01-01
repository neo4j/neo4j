/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;
import org.neo4j.kernel.impl.nioneo.store.TokenStore;
import org.neo4j.kernel.impl.nioneo.xa.TokenCreator;

/**
 * Batching version of a {@link TokenStore} where tokens can be created and retrieved, but only persisted
 * to storage as part of {@link #close() closing}.
 */
public abstract class BatchingTokenRepository<T extends TokenRecord>
{
    // TODO more efficient data structure
    private final Map<String,Integer> existing = new HashMap<>();
    private final Map<String,Integer> created = new HashMap<>();
    private final TokenStore<T> store;
    private int highId;

    public BatchingTokenRepository( TokenStore<T> store )
    {
        this.store = store;
        // TODO read the store into the repository?
        this.highId = (int) store.getHighId();
    }

    public int getOrCreateId( String name )
    {
        assert name != null;
        Integer id = existing.get( name );
        if ( id == null )
        {
            synchronized ( created )
            {
                id = created.get( name );
                if ( id == null )
                {
                    id = highId++;
                    created.put( name, id );
                }
            }
        }
        return id;
    }

    protected abstract T createRecord( int key );

    public void close()
    {
        // Batch-friendly record access
        BatchingRecordAccess<Integer, T, Void> recordAccess = new BatchingRecordAccess<Integer, T, Void>()
        {
            @Override
            protected T createRecord( Integer key, Void additionalData )
            {
                return BatchingTokenRepository.this.createRecord( key );
            }
        };

        // Create the tokens
        TokenCreator<T> creator = new TokenCreator<>( store );
        int highest = 1;
        for ( Map.Entry<Integer,String> tokenToCreate : sortCreatedTokensById() )
        {
            creator.createToken( tokenToCreate.getValue(), tokenToCreate.getKey(), recordAccess );
            highest = Math.max( highest, tokenToCreate.getKey() );
        }

        // Store them
        store.setRecovered();
        try
        {
            for ( T record : recordAccess.records() )
            {
                store.updateRecord( record );
            }
            store.updateIdGenerators();
        }
        finally
        {
            store.unsetRecovered();
        }
    }

    private Iterable<Map.Entry<Integer,String>> sortCreatedTokensById()
    {
        Map<Integer,String> sorted = new TreeMap<>();
        for ( Map.Entry<String,Integer> entry : created.entrySet() )
        {
            sorted.put( entry.getValue(), entry.getKey() );
        }
        return sorted.entrySet();
    }

    public static class BatchingPropertyKeyTokenRepository extends BatchingTokenRepository<PropertyKeyTokenRecord>
    {
        public BatchingPropertyKeyTokenRepository( TokenStore<PropertyKeyTokenRecord> store )
        {
            super( store );
        }

        @Override
        protected PropertyKeyTokenRecord createRecord( int key )
        {
            return new PropertyKeyTokenRecord( key );
        }
    }

    public static class BatchingLabelTokenRepository extends BatchingTokenRepository<LabelTokenRecord>
    {
        public BatchingLabelTokenRepository( TokenStore<LabelTokenRecord> store )
        {
            super( store );
        }

        @Override
        protected LabelTokenRecord createRecord( int key )
        {
            return new LabelTokenRecord( key );
        }
    }

    public static class BatchingRelationshipTypeTokenRepository extends BatchingTokenRepository<RelationshipTypeTokenRecord>
    {
        public BatchingRelationshipTypeTokenRepository( TokenStore<RelationshipTypeTokenRecord> store )
        {
            super( store );
        }

        @Override
        protected RelationshipTypeTokenRecord createRecord( int key )
        {
            return new RelationshipTypeTokenRecord( key );
        }
    }
}
