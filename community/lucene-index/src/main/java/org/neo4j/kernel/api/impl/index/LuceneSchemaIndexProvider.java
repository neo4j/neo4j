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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.util.FailureStorage;
import org.neo4j.kernel.api.index.util.FolderLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

public class LuceneSchemaIndexProvider extends SchemaIndexProvider
{
    private final DirectoryFactory directoryFactory;
    private final LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
    private final FailureStorage failureStorage;
    private final FolderLayout folderLayout;
    private final boolean readOnly;

    public LuceneSchemaIndexProvider( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory,
            File storeDir, Config config, OperationalMode operationalMode )
    {
        super( LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR, 1 );
        this.directoryFactory = directoryFactory;
        File rootDirectory = getRootDirectory( storeDir, LuceneSchemaIndexProviderFactory.KEY );
        this.folderLayout = new FolderLayout( rootDirectory );
        this.failureStorage = new FailureStorage( fileSystem, folderLayout );
        this.readOnly = isReadOnly( config, operationalMode );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor,
                                        IndexConfiguration config, IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Index population is not supported in read only mode." );
        }
        if ( config.isUnique() )
        {
            return new DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                    documentStructure, IndexWriterFactories.tracking(), SearcherManagerFactories.standard() ,
                    directoryFactory, folderLayout.getFolder( indexId ), failureStorage, indexId, descriptor );
        }
        else
        {
            return new NonUniqueLuceneIndexPopulator(
                    NonUniqueLuceneIndexPopulator.DEFAULT_QUEUE_THRESHOLD, documentStructure,
                    IndexWriterFactories.tracking(), directoryFactory, folderLayout.getFolder( indexId ),
                    failureStorage, indexId, samplingConfig );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config,
                                            IndexSamplingConfig samplingConfig ) throws IOException
    {
        if ( config.isUnique() )
        {
            return new UniqueLuceneIndexAccessor( documentStructure, readOnly, IndexWriterFactories.reserving(),
                    directoryFactory, folderLayout.getFolder( indexId ) );
        }
        else
        {
            return new NonUniqueLuceneIndexAccessor( documentStructure, readOnly, IndexWriterFactories.reserving(),
                    directoryFactory, folderLayout.getFolder( indexId ), samplingConfig.bufferSize() );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {   // Nothing to shut down
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        try
        {
            String failure = failureStorage.loadIndexFailure( indexId );
            if ( failure != null )
            {
                return InternalIndexState.FAILED;
            }

            try ( Directory directory = directoryFactory.open( folderLayout.getFolder( indexId ) ) )
            {
                boolean status = LuceneIndexWriter.isOnline( directory );
                return status ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
            }
        }
        catch( CorruptIndexException | FileNotFoundException | EOFException e )
        {
            recordFailure( indexId, e );
            return InternalIndexState.FAILED;
        }
        catch ( IOException e )
        {
            recordFailure( indexId, e );
            throw new RuntimeException( e );
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( final FileSystemAbstraction fs, PageCache pageCache )
    {
        return new SchemaIndexMigrator( fs, pageCache, new StoreFactory() );
    }

    @Override
    public String getIndexFailure( long indexId ) throws IllegalStateException
    {
        String failure = failureStorage.loadIndexFailure( indexId );
        if ( failure == null )
        {
            throw new IllegalStateException( "Index " + indexId + " isn't failed" );
        }
        return failure;
    }

    @Override
    public void storeIndexFailure( long indexId, String failure ) throws IOException
    {
        failureStorage.storeIndexFailure( indexId, failure );
    }

    private void recordFailure( long indexId, Exception e )
    {
        try
        {
            failureStorage.storeIndexFailure( indexId, failure( e ).asString() );
        }
        catch ( Exception ignored )
        {
            RuntimeException abort = new RuntimeException( "Failed to record index failure.", ignored );
            abort.addSuppressed( e );
            throw abort;
        }
    }

    private static boolean isReadOnly( Config config, OperationalMode operationalMode )
    {
        return config.get( GraphDatabaseSettings.read_only ) &&
               (OperationalMode.ha != operationalMode);
    }
}
