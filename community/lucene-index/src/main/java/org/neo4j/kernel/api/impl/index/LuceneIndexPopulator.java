/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

import org.neo4j.kernel.api.index.IndexPopulator;

public abstract class LuceneIndexPopulator implements IndexPopulator
{
    protected final LuceneDocumentStructure documentStructure;
    private final LuceneIndexWriterFactory indexWriterFactory;
    private final IndexWriterStatus writerStatus;
    private final DirectoryFactory dirFactory;
    private final File dirFile;

    protected IndexWriter writer;
    private Directory directory;

    LuceneIndexPopulator(
            LuceneDocumentStructure documentStructure, LuceneIndexWriterFactory indexWriterFactory,
            IndexWriterStatus writerStatus, DirectoryFactory dirFactory, File dirFile )
    {
        this.documentStructure = documentStructure;
        this.indexWriterFactory = indexWriterFactory;
        this.writerStatus = writerStatus;
        this.dirFactory = dirFactory;
        this.dirFile = dirFile;
    }

    public void create() throws IOException
    {
        this.directory = dirFactory.open( dirFile );
        DirectorySupport.deleteDirectoryContents( directory );
        writer = indexWriterFactory.create( directory );
    }

    public void drop() throws IOException
    {
        writerStatus.close( writer );
        DirectorySupport.deleteDirectoryContents( directory );
    }

    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        try
        {
            if ( populationCompletedSuccessfully )
            {
                flush();
                writerStatus.commitAsOnline( writer );
            }
        }
        finally
        {
            writerStatus.close( writer );
            directory.close();
        }
    }

    protected abstract void flush() throws IOException;
}
