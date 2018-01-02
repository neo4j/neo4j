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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.util.FailureStorage;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.System.currentTimeMillis;

public abstract class LuceneIndexPopulator implements IndexPopulator
{
    protected final LuceneDocumentStructure documentStructure;
    private final IndexWriterFactory<LuceneIndexWriter> indexWriterFactory;
    private final DirectoryFactory dirFactory;
    private final File dirFile;
    private final boolean archiveOld;
    private final FailureStorage failureStorage;
    private final long indexId;
    private final Log log;
    protected LuceneIndexWriter writer;
    private Directory directory;

    LuceneIndexPopulator(
            LuceneDocumentStructure documentStructure, LogProvider logging,
            IndexWriterFactory<LuceneIndexWriter> indexWriterFactory, DirectoryFactory dirFactory, File dirFile,
            boolean archiveOld, FailureStorage failureStorage, long indexId )
    {
        this.documentStructure = documentStructure;
        this.indexWriterFactory = indexWriterFactory;
        this.dirFactory = dirFactory;
        this.dirFile = dirFile;
        this.archiveOld = archiveOld;
        this.failureStorage = failureStorage;
        this.indexId = indexId;
        this.log = logging.getLog( getClass() );
    }

    @Override
    public void create() throws IOException
    {
        this.directory = dirFactory.open( dirFile );
        if ( archiveOld )
        {
            String archiveName = "archive-" + currentTimeMillis() + ".zip";
            DirectorySupport.archiveAndDeleteDirectoryContents( directory, archiveName );
            if ( directory.fileExists( archiveName ) )
            {
                log.info( "Created archive of previous (failed) index %s", new File( dirFile, archiveName ).getPath() );
            }
        }
        else
        {
            DirectorySupport.deleteDirectoryContents( directory );
        }
        failureStorage.reserveForIndex( indexId );
        writer = indexWriterFactory.create( directory );
    }

    @Override
    public void drop() throws IOException
    {
        if ( writer != null )
        {
            writer.close();
        }

        try
        {
            DirectorySupport.deleteDirectoryContents( directory = directory == null ? dirFactory.open( dirFile ) : directory );
        }
        catch ( AlreadyClosedException e )
        {   // It was closed, open again just to be able to delete the files
            DirectorySupport.deleteDirectoryContents( directory = dirFactory.open( dirFile ) );
        }
        finally
        {
            if ( directory != null )
            {
                directory.close();
            }
        }
        failureStorage.clearForIndex( indexId );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException, IndexCapacityExceededException
    {
        try
        {
            if ( populationCompletedSuccessfully )
            {
                flush();
                writer.commitAsOnline();
            }
        }
        finally
        {
            if ( writer != null )
            {
                writer.close();
            }
            if ( directory != null )
            {
                directory.close();
            }
        }
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        failureStorage.storeIndexFailure( indexId, failure );
    }

    protected abstract void flush() throws IOException, IndexCapacityExceededException;
}
