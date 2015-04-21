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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.util.EnumMap;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;

import static org.neo4j.kernel.impl.store.id.IdGeneratorImpl.createGenerator;

/**
 * Batching version of an {@link IdGeneratorFactory} where all {@link IdGenerator id generators} are
 * batching as well, in that there is no synchronization and new id assignments is trivially incrementing a variable.
 */
public class BatchingIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType, IdGenerator> idGenerators = new EnumMap<>( IdType.class );

    @Override
    public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType, long highId )
    {
        IdGenerator generator = idGenerators.get( idType );
        if ( generator == null )
        {
            idGenerators.put( idType, generator = new BatchingIdGenerator( fs, fileName, highId ) );
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
        return idGenerators.get( idType );
    }

    private static class BatchingIdGenerator implements IdGenerator
    {
        private long highId;
        private final FileSystemAbstraction fs;
        private final File fileName;

        public BatchingIdGenerator( FileSystemAbstraction fs, File fileName, long highId )
        {
            this.fs = fs;
            this.fileName = fileName;
            this.highId = highId;
        }

        @Override
        public long nextId()
        {
            try
            {
                return highId;
            }
            finally
            {
                highId++;
            }
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setHighId( long id )
        {
            highId = id;
        }

        @Override
        public long getHighId()
        {
            return highId;
        }

        @Override
        public void freeId( long id )
        {   // No freeing of ids
        }

        @Override
        public void close()
        {
            fs.deleteFile( fileName );
            createGenerator( fs, fileName, highId );
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return highId;
        }

        @Override
        public long getDefragCount()
        {
            return 0;
        }

        @Override
        public void delete()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return getHighId()-1;
        }
    }
}
