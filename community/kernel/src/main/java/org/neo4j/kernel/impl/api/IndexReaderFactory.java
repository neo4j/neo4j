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
package org.neo4j.kernel.impl.api;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;

public interface IndexReaderFactory
{
    IndexReader newReader( long indexId ) throws IndexNotFoundKernelException;

    IndexReader newUnCachedReader( long indexId ) throws IndexNotFoundKernelException;

    void close();

    class Caching implements IndexReaderFactory
    {
        private Map<Long,IndexReader> indexReaders = null;
        private final IndexingService indexingService;

        public Caching( IndexingService indexingService )
        {
            this.indexingService = indexingService;
        }

        @Override
        public IndexReader newReader( long indexId ) throws IndexNotFoundKernelException
        {
            if( indexReaders == null )
            {
                indexReaders = new HashMap<>();
            }

            IndexReader reader = indexReaders.get( indexId );
            if ( reader == null )
            {
                reader = newUnCachedReader( indexId );
                indexReaders.put( indexId, reader );
            }
            return reader;
        }

        public IndexReader newUnCachedReader( long indexId ) throws IndexNotFoundKernelException
        {
            IndexProxy index = indexingService.getIndexProxy( indexId );
            return index.newReader();
        }

        @Override
        public void close()
        {
            if ( indexReaders != null )
            {
                for ( IndexReader indexReader : indexReaders.values() )
                {
                    indexReader.close();
                }
            }
        }
    }
}
