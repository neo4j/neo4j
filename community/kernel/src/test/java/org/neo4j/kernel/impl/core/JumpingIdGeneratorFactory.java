/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.IdRange;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class JumpingIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType, IdGenerator> generators = new EnumMap<IdType, IdGenerator>( IdType.class );
    private IdGenerator forTheRest = new InMemoryIdGenerator();

    private final int sizePerJump;
    
    public JumpingIdGeneratorFactory( int sizePerJump )
    {
        this.sizePerJump = sizePerJump;
    }
    
    public IdGenerator open( String fileName, int grabSize, IdType idType,
            long highestIdInUse )
    {
        return get( idType );
    }
    
    public IdGenerator get( IdType idType )
    {
        if ( idType == IdType.NODE || idType == IdType.RELATIONSHIP || idType == IdType.PROPERTY ||
                idType == IdType.STRING_BLOCK || idType == IdType.ARRAY_BLOCK )
        {
            IdGenerator generator = generators.get( idType );
            if ( generator == null )
            {
                generator = new JumpingIdGenerator();
                generators.put( idType, generator );
            }
            return generator;
        }
        return forTheRest;
    }
    
    public void create( String fileName )
    {
    }
    
    public void updateIdGenerators( NeoStore neoStore )
    {
    }
    
    private class InMemoryIdGenerator implements IdGenerator
    {
        private final AtomicLong nextId = new AtomicLong();

        @Override
        public long nextId()
        {
            return nextId.incrementAndGet();
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
        }

        @Override
        public void close()
        {
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return nextId.get();
        }

        @Override
        public long getDefragCount()
        {
            return 0;
        }
        
        @Override
        public void clearFreeIds()
        {
        }
    }
    
    private class JumpingIdGenerator implements IdGenerator
    {
        private AtomicLong nextId = new AtomicLong();
        private int leftToNextJump = sizePerJump/2;
        private long highBits = 0;
        
        @Override
        public long nextId()
        {
            long result = tryNextId();
            if ( --leftToNextJump == 0 )
            {
                leftToNextJump = sizePerJump;
                nextId.set( (0xFFFFFFFFL | (highBits++ << 32)) - sizePerJump/2 + 1 );
            }
            return result;
        }

        private long tryNextId()
        {
            long result = nextId.getAndIncrement();
            if ( result == IdGeneratorImpl.INTEGER_MINUS_ONE )
            {
                result = nextId.getAndIncrement();
                leftToNextJump--;
            }
            return result;
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
        }

        @Override
        public void close()
        {
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return nextId.get();
        }

        @Override
        public long getDefragCount()
        {
            return 0;
        }
        
        @Override
        public void clearFreeIds()
        {
        }
    }
}
