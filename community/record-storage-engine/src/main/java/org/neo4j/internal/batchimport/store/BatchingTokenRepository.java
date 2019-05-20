/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.batchimport.store;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;

/**
 * Batching version of a {@link TokenStore} where tokens can be created and retrieved, but only persisted
 * to storage as part of {@link #close() closing}. Instances of this class are thread safe
 * to call {@link #getOrCreateId(String)} methods on.
 */
public abstract class BatchingTokenRepository<RECORD extends TokenRecord>
        implements ToIntFunction<Object>, Closeable
{
    private final Map<String,Integer> tokens = new HashMap<>();
    private final TokenStore<RECORD> store;
    private final IntFunction<RECORD> recordInstantiator;
    private int highId;
    private int highestCreatedId;

    BatchingTokenRepository( TokenStore<RECORD> store, IntFunction<RECORD> recordInstantiator )
    {
        this.store = store;
        this.recordInstantiator = recordInstantiator;
        this.highId = (int)store.getHighId();
        this.highestCreatedId = highId - 1;
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
                id = tokens.computeIfAbsent( name, k -> highId++ );
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

    public long[] getOrCreateIds( String[] names )
    {
        return getOrCreateIds( names, names.length );
    }

    /**
     * Returns or creates multiple tokens for given token names.
     *
     * @param names token names to lookup or create token ids for.
     * @param length length of the names array to consider, the array itself may be longer.
     * @return {@code long[]} containing the label ids.
     */
    public long[] getOrCreateIds( String[] names, int length )
    {
        long[] result = new long[length];
        int from;
        int to;
        for ( from = 0, to = 0; from < length; from++ )
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

    /**
     * Closes this repository and writes all created tokens to the underlying store.
     */
    @Override
    public void close()
    {
        flush();
    }

    public void flush()
    {
        int highest = highestCreatedId;
        for ( Map.Entry<Integer,String> tokenToCreate : sortCreatedTokensById() )
        {
            if ( tokenToCreate.getKey() > highestCreatedId )
            {
                createToken( tokenToCreate.getValue(), tokenToCreate.getKey() );
                highest = Math.max( highest, tokenToCreate.getKey() );
            }
        }
        // Store them
        int highestId = max( toIntExact( store.getHighestPossibleIdInUse() ), highest );
        store.setHighestPossibleIdInUse( highestId );
        highestCreatedId = highestId;
    }

    private void createToken( String name, int tokenId )
    {
        RECORD record = recordInstantiator.apply( tokenId );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> nameRecords = store.allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) Iterables.first( nameRecords ).getId() );
        record.addNameRecords( nameRecords );
        store.updateRecord( record );
        nameRecords.forEach( store.getNameStore()::updateRecord );
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

    public static class BatchingPropertyKeyTokenRepository
            extends BatchingTokenRepository<PropertyKeyTokenRecord>
    {
        BatchingPropertyKeyTokenRepository( TokenStore<PropertyKeyTokenRecord> store )
        {
            super( store, PropertyKeyTokenRecord::new );
        }
    }

    public static class BatchingLabelTokenRepository extends BatchingTokenRepository<LabelTokenRecord>
    {
        BatchingLabelTokenRepository( TokenStore<LabelTokenRecord> store )
        {
            super( store, LabelTokenRecord::new );
        }
    }

    public static class BatchingRelationshipTypeTokenRepository
            extends BatchingTokenRepository<RelationshipTypeTokenRecord>
    {
        BatchingRelationshipTypeTokenRepository( TokenStore<RelationshipTypeTokenRecord> store )
        {
            super( store, RelationshipTypeTokenRecord::new );
        }
    }
}
