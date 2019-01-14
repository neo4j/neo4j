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
package org.neo4j.kernel.impl.store.id;

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;
import java.util.function.LongSupplier;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

/**
 * {@link IdGeneratorFactory} managing read-only {@link IdGenerator} instances which basically only can access
 * {@link IdGenerator#getHighId()} and {@link IdGenerator#getHighestPossibleIdInUse()}.
 */
public class ReadOnlyIdGeneratorFactory implements IdGeneratorFactory
{
    private final EnumMap<IdType,IdGenerator> idGenerators = new EnumMap<>( IdType.class );
    private final FileSystemAbstraction fileSystemAbstraction;

    public ReadOnlyIdGeneratorFactory()
    {
        this.fileSystemAbstraction = new DefaultFileSystemAbstraction();
    }

    public ReadOnlyIdGeneratorFactory( FileSystemAbstraction fileSystemAbstraction )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, LongSupplier highId, long maxId )
    {
        return open( filename, 0, idType, highId, maxId );
    }

    @Override
    public IdGenerator open( File filename, int grabSize, IdType idType, LongSupplier highId, long maxId )
    {
        IdGenerator generator = new ReadOnlyIdGenerator( highId, fileSystemAbstraction, filename );
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

    static class ReadOnlyIdGenerator implements IdGenerator
    {
        private final long highId;
        private final long defragCount;

        ReadOnlyIdGenerator( LongSupplier highId, FileSystemAbstraction fs, File filename )
        {
            if ( fs != null && fs.fileExists( filename ) )
            {
                try
                {
                    this.highId = IdGeneratorImpl.readHighId( fs, filename );
                    defragCount = IdGeneratorImpl.readDefragCount( fs, filename );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException(
                            "Failed to read id counts of the id file: " + filename, e );
                }
            }
            else
            {
                this.highId = highId.getAsLong();
                defragCount = 0;
            }
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
            return highId - defragCount;
        }

        @Override
        public long getDefragCount()
        {
            return defragCount;
        }

        @Override
        public void delete()
        {   // Nothing to delete
        }
    }
}
