/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;

public class EphemeralIdGenerator implements IdGenerator
{
    public static class Factory implements IdGeneratorFactory
    {
        protected final Map<IdType, IdGenerator> generators = new EnumMap<IdType, IdGenerator>( IdType.class );

        @Override
        public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType, long highId )
        {
            IdGenerator generator = generators.get( idType );
            if ( generator == null )
            {
                generator = new EphemeralIdGenerator( idType );
                generators.put( idType, generator );
            }
            return generator;
        }

        @Override
        public void create( FileSystemAbstraction fs, File fileName, long highId )
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

    public EphemeralIdGenerator( IdType idType )
    {
        this.idType = idType;
        this.freeList = idType != null && idType.allowAggressiveReuse() ? new ConcurrentLinkedQueue<Long>() : null;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + idType + "]";
    }

    @Override
    public long nextId()
    {
        if ( freeList != null )
        {
            Long id = freeList.poll();
            if ( id != null )
            {
                return id.longValue();
            }
        }
        return nextId.getAndIncrement();
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        throw new UnsupportedOperationException();
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
        if (freeList != null)
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
        return result-freedButNotReturnableIdCount.get();
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
