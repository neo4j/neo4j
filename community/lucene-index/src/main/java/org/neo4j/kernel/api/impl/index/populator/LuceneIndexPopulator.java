/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.populator;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.IndexWriterFactory;
import org.neo4j.kernel.api.impl.index.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.index.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.storage.IndexStorage;
import org.neo4j.kernel.api.index.IndexPopulator;

public abstract class LuceneIndexPopulator implements IndexPopulator
{
    protected final LuceneDocumentStructure documentStructure;
    private final IndexWriterFactory<LuceneIndexWriter> indexWriterFactory;
    private final IndexStorage indexStorage;

    protected LuceneIndexWriter writer;


    LuceneIndexPopulator( LuceneDocumentStructure documentStructure,
            IndexWriterFactory<LuceneIndexWriter> indexWriterFactory, IndexStorage indexStorage )
    {
        this.documentStructure = documentStructure;
        this.indexWriterFactory = indexWriterFactory;
        this.indexStorage = indexStorage;
    }

    @Override
    public void create() throws IOException
    {
        indexStorage.prepareIndexStorage();
        writer = indexWriterFactory.create( indexStorage.getDirectory() );
    }

    @Override
    public void drop() throws IOException
    {
        if ( writer != null )
        {
            writer.close();
        }

        indexStorage.cleanupStorage();
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
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
            indexStorage.close();
        }
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        indexStorage.storeIndexFailure( failure );
    }

    protected abstract void flush() throws IOException;
}
