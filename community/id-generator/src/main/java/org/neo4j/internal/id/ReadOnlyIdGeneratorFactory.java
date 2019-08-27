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
package org.neo4j.internal.id;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.EnumMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;

/**
 * {@link IdGeneratorFactory} managing read-only {@link IdGenerator} instances which basically only can access
 * {@link IdGenerator#getHighId()} and {@link IdGenerator#getHighestPossibleIdInUse()}.
 */
public class ReadOnlyIdGeneratorFactory implements IdGeneratorFactory
{
    private final EnumMap<IdType,IdGenerator> idGenerators = new EnumMap<>( IdType.class );
    private final IdGeneratorFactory actual;

    public ReadOnlyIdGeneratorFactory( IdGeneratorFactory actual )
    {
        this.actual = actual;
    }

    @Override
    public IdGenerator open( PageCache pageCache, File filename, IdType idType, LongSupplier highIdScanner, long maxId, OpenOption... openOptions )
    {
        IdGenerator actualGenerator = actual.open( pageCache, filename, idType, highIdScanner, maxId );
        ReadOnlyIdGenerator readOnlyIdGenerator = new ReadOnlyIdGenerator( actualGenerator );
        idGenerators.put( idType, readOnlyIdGenerator );
        return readOnlyIdGenerator;
    }

    @Override
    public IdGenerator create( PageCache pageCache, File filename, IdType idType, long highId, boolean throwIfFileExists, long maxId,
            OpenOption... openOptions )
    {
        throw new UnsupportedOperationException( "Not supported on read-only id generator factory" );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return idGenerators.get( idType );
    }

    @Override
    public void visit( Consumer<IdGenerator> visitor )
    {
        idGenerators.values().forEach( visitor );
    }

    static class ReadOnlyIdGenerator implements IdGenerator
    {
        private final IdGenerator actual;

        ReadOnlyIdGenerator( IdGenerator actual )
        {
            this.actual = actual;
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
        public void markHighestWrittenAtHighId()
        {   // OK
        }

        @Override
        public long getHighId()
        {
            return actual.getHighId();
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return actual.getHighestPossibleIdInUse();
        }

        @Override
        public ReuseMarker reuseMarker()
        {
            return NOOP_REUSE_MARKER;
        }

        @Override
        public CommitMarker commitMarker()
        {
            return NOOP_COMMIT_MARKER;
        }

        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            // TODO
            return getHighId();
        }

        @Override
        public long getDefragCount()
        {
            // TODO
            return 0;
        }

        @Override
        public void start( FreeIds freeIdsForRebuild )
        {
            // Don't
        }

        @Override
        public void checkpoint( IOLimiter ioLimiter )
        {
            // Don't
        }

        @Override
        public void maintenance()
        {
            // Nothing to do
        }

        @Override
        public void clearCache()
        {
            // Nothing to do
        }
    }
}
