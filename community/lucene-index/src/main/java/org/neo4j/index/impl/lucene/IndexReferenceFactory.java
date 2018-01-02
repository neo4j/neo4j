/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;

/**
 * Factory that build appropriate (read only or writable) {@link IndexReference} for provided {@link IndexIdentifier}
 * or refresh previously constructed instance.
 */
abstract class IndexReferenceFactory
{
    private final File baseStorePath;
    private final IndexTypeCache typeCache;
    private final LuceneDataSource.LuceneFilesystemFacade filesystemFacade;

    IndexReferenceFactory( LuceneDataSource.LuceneFilesystemFacade filesystemFacade, File baseStorePath,
            IndexTypeCache typeCache )
    {
        this.filesystemFacade = filesystemFacade;
        this.baseStorePath = baseStorePath;
        this.typeCache = typeCache;
    }

    /**
     * Create new {@link IndexReference} for provided {@link IndexIdentifier}.
     * @param indexIdentifier index identifier to build index for.
     * @return newly create {@link IndexReference}
     *
     * @throws IOException in case of exception during accessing lucene reader/writer.
     */
    abstract IndexReference createIndexReference( IndexIdentifier indexIdentifier ) throws IOException;

    /**
     * Refresh previously constructed indexReference.
     * @param indexReference index reference to refresh
     * @return refreshed index reference
     */
    abstract IndexReference refresh( IndexReference indexReference );

    Directory getIndexDirectory( IndexIdentifier identifier ) throws IOException
    {
        return filesystemFacade.getDirectory( baseStorePath, identifier );
    }

    IndexSearcher newIndexSearcher( IndexIdentifier identifier, IndexReader reader )
    {
        IndexSearcher searcher = new IndexSearcher( reader );
        IndexType type = getType( identifier );
        if ( type.getSimilarity() != null )
        {
            searcher.setSimilarity( type.getSimilarity() );
        }
        return searcher;
    }

    IndexType getType( IndexIdentifier identifier )
    {
        return typeCache.getIndexType( identifier, false );
    }
}
