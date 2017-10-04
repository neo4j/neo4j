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
package org.neo4j.kernel.impl.store.id;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@link IdGeneratorFactory} managing read-only {@link IdGenerator} instances which basically only can access
 * {@link IdGenerator#getHighId()} and {@link IdGenerator#getHighestPossibleIdInUse()}.
 */
public class ReadOnlyIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType,IdGenerator> idGenerators = new HashMap<>();

    @Override
    public IdGenerator open( File filename, IdType idType, Supplier<Long> highId, long maxId )
    {
        return open( filename, 0, idType, highId, maxId );
    }

    @Override
    public IdGenerator open( File filename, int grabSize, IdType idType, Supplier<Long> highId, long maxId )
    {
        IdGenerator generator = new ReadOnlyIdGenerator( highId );
        idGenerators.put( idType, generator );
        return generator;
    }

    @Override
    public void create( File filename, long highId, boolean throwIfFileExists )
    {
        // Don't
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return idGenerators.get( idType );
    }

    class ReadOnlyIdGenerator implements IdGenerator
    {
        private final long highId;

        ReadOnlyIdGenerator( Supplier<Long> highId )
        {
            this.highId = highId.get();
        }

        @Override
        public long nextId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setHighId( long id )
        {   // OK
        }

        @Override
        public long getHighId()
        {
            return highId;
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return highId - 1;
        }

        @Override
        public void freeId( long id )
        {   // Don't
        }

        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return 0;
        }

        @Override
        public long getDefragCount()
        {
            return 0;
        }

        @Override
        public void delete()
        {   // Nothing to delete
        }
    }
}
