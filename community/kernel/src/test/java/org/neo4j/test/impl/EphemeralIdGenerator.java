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
package org.neo4j.test.impl;

import java.io.File;
import java.util.EnumMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;

import static java.lang.Integer.min;

public class EphemeralIdGenerator implements IdGenerator
{
    public static class Factory implements IdGeneratorFactory
    {
        protected final Map<IdType, IdGenerator> generators = new EnumMap<>( IdType.class );
        private final IdTypeConfigurationProvider
                idTypeConfigurationProvider = new CommunityIdTypeConfigurationProvider();

        @Override
        public IdGenerator open( File filename, IdType idType, Supplier<Long> highId, long maxId )
        {
            return open( filename, 0, idType, highId, maxId );
        }

        @Override
        public IdGenerator open( File fileName, int grabSize, IdType idType, Supplier<Long> highId, long maxId )
        {
            IdGenerator generator = generators.get( idType );
            if ( generator == null )
            {
                IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
                generator = new EphemeralIdGenerator( idType, idTypeConfiguration );
                generators.put( idType, generator );
            }
            return generator;
        }

        @Override
        public void create( File fileName, long highId, boolean throwIfFileExists )
        {
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }
    }

    private final AtomicLong nextId = new AtomicLong();
    private final IdType idType;
    private final Queue<Long> freeList;
    private final AtomicInteger freedButNotReturnableIdCount = new AtomicInteger();

    public EphemeralIdGenerator( IdType idType, IdTypeConfiguration idTypeConfiguration )
    {
        this.idType = idType;
        this.freeList = idType != null && idTypeConfiguration.allowAggressiveReuse() ? new ConcurrentLinkedQueue<>() : null;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + idType + "]";
    }

    @Override
    public synchronized long nextId()
    {
        if ( freeList != null )
        {
            Long id = freeList.poll();
            if ( id != null )
            {
                return id;
            }
        }
        return nextId.getAndIncrement();
    }

    @Override
    public synchronized IdRange nextIdBatch( int size )
    {
        long[] defragIds = PrimitiveLongCollections.EMPTY_LONG_ARRAY;
        if ( freeList != null && !freeList.isEmpty() )
        {
            defragIds = new long[min( size, freeList.size() )];
            for ( int i = 0; i < defragIds.length; i++ )
            {
                defragIds[i] = freeList.poll();
            }
            size -= defragIds.length;
        }
        return new IdRange( defragIds, nextId.getAndAdd( size ), size );
    }

    @Override
    public void setHighId( long id )
    {
        nextId.set( id );
    }

    @Override
    public long getHighId()
    {
        return nextId.get();
    }

    @Override
    public void freeId( long id )
    {
        if ( freeList != null )
        {
            freeList.add( id );
        }
        else
        {
            freedButNotReturnableIdCount.getAndIncrement();
        }
    }

    @Override
    public void close()
    {
    }

    @Override
    public long getNumberOfIdsInUse()
    {
        long result = freeList == null ? nextId.get() : nextId.get() - freeList.size();
        return result - freedButNotReturnableIdCount.get();
    }

    @Override
    public long getDefragCount()
    {
        return 0;
    }

    @Override
    public void delete()
    {
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return nextId.get();
    }
}
