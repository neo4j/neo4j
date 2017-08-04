/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.ToIntFunction;

import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.transaction.state.TokenCreator;
import org.neo4j.storageengine.api.Token;
import org.neo4j.values.storable.Values;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;

/**
 * Batching version of a {@link TokenStore} where tokens can be created and retrieved, but only persisted
 * to storage as part of {@link #close() closing}. Instances of this class are thread safe
 * to call {@link #getOrCreateId(String)} methods on.
 */
public abstract class BatchingTokenRepository<RECORD extends TokenRecord, TOKEN extends Token>
        implements ToIntFunction<Object>
{
    private final Map<String,Integer> tokens = new HashMap<>();
    private final TokenStore<RECORD, TOKEN> store;
    private int highId;

    public BatchingTokenRepository( TokenStore<RECORD,TOKEN> store )
    {
        this.store = store;
        this.highId = (int)store.getHighId();
    }

    /**
     * Returns the id for token with the specified {@code name}, potentially creating that token and
     * assigning a new id as part of this call.
     *
     * @param name token name.
     * @return the id (created or existing) for the token by this name.
     */
    public int getOrCreateId( String name )
    {
        assert name != null;
        Integer id = tokens.get( name );
        if ( id == null )
        {
            synchronized ( tokens )
            {
                id = tokens.get( name );
                if ( id == null )
                {
                    id = highId++;
                    tokens.put( name, id );
                }
            }
        }
        return id;
    }

    /**
     * Returns the id for token with the specified {@code key}, which can be a {@link String} if representing
     * a user-defined name or an {@link Integer} if representing an existing type from an external source,
     * which wants to preserve its name --> id tokens. Also see {@link #getOrCreateId(String)} for more details.
     *
     * @param key name or id of this token.
     * @return the id (created or existing) for the token key.
     */
    public int getOrCreateId( Object key )
    {
        if ( key instanceof String )
        {
            // A name was supplied, get or create a token id for it
            return getOrCreateId( (String) key );
        }
        else if ( key instanceof Integer )
        {
            // A raw token id was supplied, just use it
            return (Integer) key;
        }
        throw new IllegalArgumentException( "Expected either a String or Integer for property key, but was '" +
                key + "'" + ", " + key.getClass() );
    }

    @Override
    public int applyAsInt( Object key )
    {
        return getOrCreateId( key );
    }

    /**
     * Returns or creates multiple tokens for given token names.
     *
     * @param names token names to lookup or create token ids for.
     * @return {@code long[]} containing the label ids.
     */
    public long[] getOrCreateIds( String[] names )
    {
        long[] result = new long[names.length];
        int from;
        int to;
        for ( from = 0, to = 0; from < names.length; from++ )
        {
            int id = getOrCreateId( names[from] );
            if ( !contains( result, id, to ) )
            {
                result[to++] = id;
            }
        }
        if ( to < from )
        {
            result = Arrays.copyOf( result, to );
        }
        Arrays.sort( result );
        return result;
    }

    private boolean contains( long[] array, long id, int arrayLength )
    {
        for ( int i = 0; i < arrayLength; i++ )
        {
            if ( array[i] == id )
            {
                return true;
            }
        }
        return false;
    }

    public int getHighId()
    {
        return highId;
    }

    protected abstract RECORD createRecord( int key );

    /**
     * Closes this repository and writes all created tokens to the underlying store.
     */
    public void close()
    {
        // Batch-friendly record access
        BatchingRecordAccess<RECORD, Void> recordAccess = new BatchingRecordAccess<RECORD, Void>()
        {
            @Override
            protected RECORD createRecord( long key, Void additionalData )
            {
                return BatchingTokenRepository.this.createRecord( toIntExact( key ) );
            }
        };

        // Create the tokens
        TokenCreator<RECORD, TOKEN> creator = new TokenCreator<>( store );
        int highest = 1;
        for ( Map.Entry<Integer,String> tokenToCreate : sortCreatedTokensById() )
        {
            creator.createToken( tokenToCreate.getValue(), tokenToCreate.getKey(), recordAccess );
            highest = Math.max( highest, tokenToCreate.getKey() );
        }

        // Store them
        int highestId = (int) store.getHighestPossibleIdInUse();
        for ( RECORD record : recordAccess.records() )
        {
            store.updateRecord( record );
            highestId = max( highestId, record.getIntId() );
        }
        store.setHighestPossibleIdInUse( highestId );
    }

    private Iterable<Map.Entry<Integer,String>> sortCreatedTokensById()
    {
        Map<Integer,String> sorted = new TreeMap<>();
        for ( Map.Entry<String,Integer> entry : tokens.entrySet() )
        {
            sorted.put( entry.getValue(), entry.getKey() );
        }
        return sorted.entrySet();
    }

    public static class BatchingPropertyKeyTokenRepository extends BatchingTokenRepository<PropertyKeyTokenRecord, Token>
    {
        public BatchingPropertyKeyTokenRepository( TokenStore<PropertyKeyTokenRecord, Token> store )
        {
            super( store );
        }

        @Override
        protected PropertyKeyTokenRecord createRecord( int key )
        {
            return new PropertyKeyTokenRecord( key );
        }

        public void propertyKeysAndValues( PropertyBlock[] target, int offset, Object[] properties,
                PropertyCreator creator )
        {
            int count = properties.length >> 1;
            for ( int i = 0, cursor = 0; i < count; i++ )
            {
                int key = getOrCreateId( properties[cursor++] );
                Object value = properties[cursor++];
                target[offset + i] = creator.encodeValue( new PropertyBlock(), key, Values.of( value ) );
            }
        }
    }

    public static class BatchingLabelTokenRepository extends BatchingTokenRepository<LabelTokenRecord, Token>
    {
        public BatchingLabelTokenRepository( TokenStore<LabelTokenRecord, Token> store )
        {
            super( store );
        }

        @Override
        protected LabelTokenRecord createRecord( int key )
        {
            return new LabelTokenRecord( key );
        }
    }

    public static class BatchingRelationshipTypeTokenRepository
            extends BatchingTokenRepository<RelationshipTypeTokenRecord,RelationshipTypeToken>
    {
        public BatchingRelationshipTypeTokenRepository( TokenStore<RelationshipTypeTokenRecord,
                RelationshipTypeToken> store )
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
