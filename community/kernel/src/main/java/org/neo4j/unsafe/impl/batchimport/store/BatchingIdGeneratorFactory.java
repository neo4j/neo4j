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

import java.io.File;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.neo4j.kernel.impl.store.id.IdGeneratorImpl.createGenerator;

/**
 * Batching version of an {@link IdGeneratorFactory} where all {@link IdGenerator id generators} are
 * batching as well, in that there is no synchronization and new id assignments is trivially incrementing a variable.
 */
public class BatchingIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType, IdGenerator> idGenerators = new EnumMap<>( IdType.class );
    private final FileSystemAbstraction fs;

    public BatchingIdGeneratorFactory( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, Supplier<Long> highId, long maxId )
    {
        return open( filename, 0, idType, highId, maxId );
    }

    @Override
    public IdGenerator open( File fileName, int grabSize, IdType idType, Supplier<Long> highId, long maxId )
    {
        return idGenerators.computeIfAbsent( idType, k -> new BatchingIdGenerator( fs, fileName, highId ) );
    }

    @Override
    public void create( File fileName, long highId, boolean throwIfFileExists )
    {
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return idGenerators.get( idType );
    }

    private static class BatchingIdGenerator implements IdGenerator
    {
        private final BatchingIdSequence idSequence;
        private final FileSystemAbstraction fs;
        private final File fileName;

        BatchingIdGenerator( FileSystemAbstraction fs, File fileName, Supplier<Long> highId )
        {
            this.fs = fs;
            this.fileName = fileName;
            this.idSequence = new BatchingIdSequence();
            this.idSequence.set( highId.get() );
        }

        @Override
        public long nextId()
        {
            return idSequence.nextId();
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            return idSequence.nextIdBatch( size );
        }

        @Override
        public void setHighId( long id )
        {
            idSequence.set( id );
        }

        @Override
        public long getHighId()
        {
            return idSequence.peek();
        }

        @Override
        public void freeId( long id )
        {   // No freeing of ids
        }

        @Override
        public void close()
        {
            createGenerator( fs, fileName, idSequence.peek(), false );
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return idSequence.peek();
        }

        @Override
        public long getDefragCount()
        {
            return 0;
        }

        @Override
        public void delete()
        {
            // This would be equivalent of not doing anything because close() will create the file
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return getHighId() - 1;
        }
    }
}
