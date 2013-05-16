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

import org.apache.lucene.store.Directory;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

public class LuceneSchemaIndexProvider extends SchemaIndexProvider
{
    private final DirectoryFactory directoryFactory;
    private final LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
    private final IndexWriterStatus writerStatus = new IndexWriterStatus();
    private final File rootDirectory;

    public LuceneSchemaIndexProvider( DirectoryFactory directoryFactory, Config config )
    {
        super( LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR, 1 );
        this.directoryFactory = directoryFactory;
        this.rootDirectory = getRootDirectory( config, LuceneSchemaIndexProviderFactory.KEY );
    }

    private File dirFile( long indexId )
    {
        return new File( rootDirectory, "" + indexId );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
    {
        if ( config.isUnique() )
        {
            return new UniqueLuceneIndexPopulator(
                    UniqueLuceneIndexPopulator.DEFAULT_BATCH_SIZE, documentStructure, standard(), writerStatus,
                    directoryFactory, dirFile( indexId ) );
        }
        else
        {
            return new NonUniqueLuceneIndexPopulator(
                    NonUniqueLuceneIndexPopulator.DEFAULT_QUEUE_THRESHOLD, documentStructure, standard(), writerStatus,
                    directoryFactory, dirFile( indexId ) );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config ) throws IOException
    {
        if ( config.isUnique() )
        {
            return new UniqueLuceneIndexAccessor( documentStructure, standard(), writerStatus, directoryFactory,
                                                  dirFile( indexId ) );
        }
        else
        {
            return new NonUniqueLuceneIndexAccessor( documentStructure, standard(), writerStatus, directoryFactory,
                                                     dirFile( indexId ) );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        try
        {
            Directory directory = directoryFactory.open( dirFile( indexId ) );
            try
            {
                boolean status = writerStatus.isOnline( directory );
                return status ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
            }
            finally
            {
                directory.close();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
