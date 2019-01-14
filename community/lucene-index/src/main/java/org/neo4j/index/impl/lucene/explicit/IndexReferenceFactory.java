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
package org.neo4j.index.impl.lucene.explicit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;

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
     * @throws ExplicitIndexNotFoundKernelException if the index is dropped prior to, or concurrently with, this
     * operation.
     */
    abstract IndexReference createIndexReference( IndexIdentifier indexIdentifier )
            throws IOException, ExplicitIndexNotFoundKernelException;

    /**
     * If nothing has changed underneath (since the searcher was last created or refreshed) {@code searcher} is
     * returned. But if something has changed a refreshed searcher is returned. It makes use if the
     * {@link DirectoryReader#openIfChanged(DirectoryReader, IndexWriter, boolean)} which faster than opening an index
     * from scratch.
     *
     * @param indexReference the {@link IndexReference} to refresh.
     * @return a refreshed version of the searcher or, if nothing has changed,
     *         {@code null}.
     * @throws RuntimeException if there's a problem with the index.
     * @throws ExplicitIndexNotFoundKernelException if the index is dropped prior to, or concurrently with, this
     * operation.
     */
    abstract IndexReference refresh( IndexReference indexReference ) throws ExplicitIndexNotFoundKernelException;

    Directory getIndexDirectory( IndexIdentifier identifier ) throws IOException
    {
        return filesystemFacade.getDirectory( baseStorePath, identifier );
    }

    IndexSearcher newIndexSearcher( IndexIdentifier identifier, IndexReader reader )
            throws ExplicitIndexNotFoundKernelException
    {
        IndexSearcher searcher = new IndexSearcher( reader );
        IndexType type = getType( identifier );
        if ( type.getSimilarity() != null )
        {
            searcher.setSimilarity( type.getSimilarity() );
        }
        return searcher;
    }

    IndexType getType( IndexIdentifier identifier ) throws ExplicitIndexNotFoundKernelException
    {
        return typeCache.getIndexType( identifier, false );
    }
}

