/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.ProviderMeta;
import org.neo4j.kernel.api.index.ProviderMeta.Record;
import org.neo4j.kernel.api.index.ProviderMeta.Snapshot;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.util.FailureStorage;
import org.neo4j.kernel.api.index.util.FolderLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.lifecycle.LifeSupport;

import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.resourceIterator;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

public class LuceneSchemaIndexProvider extends SchemaIndexProvider
{
    static final long CURRENT_VERSION = 1;

    private final DirectoryFactory directoryFactory;
    private final LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
    private final IndexWriterStatus writerStatus = new IndexWriterStatus();
    private final File rootDirectory;
    private final FailureStorage failureStorage;
    private final FolderLayout folderLayout;
    private final Map<Long, String> failures = new HashMap<>();
    private final LuceneIndexWriterFactory writerFactory = standard();
    private final LuceneIndexMigrator migrator;
    private final FileSystemAbstraction fileSystem;

    private final LifeSupport life = new LifeSupport();
    private final ProviderMeta meta;

    public LuceneSchemaIndexProvider( DirectoryFactory directoryFactory, Config config,
            FileSystemAbstraction fileSystem )
    {
        super( LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR, 1 );
        this.directoryFactory = directoryFactory;
        this.fileSystem = fileSystem;
        this.rootDirectory = getRootDirectory( config, LuceneSchemaIndexProviderFactory.KEY );
        this.folderLayout = new FolderLayout( rootDirectory );
        this.failureStorage = new FailureStorage( folderLayout );

        this.meta = life.add( new ProviderMeta( fileSystem, new File( rootDirectory, "meta" ) ) );
        this.migrator = new LuceneIndexMigrator( rootDirectory, folderLayout, directoryFactory, writerFactory,
                documentStructure, LuceneIndexMigrator.NO_MONITOR, meta );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexConfiguration config )
    {
        if ( config.isUnique() )
        {
            return new DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                    documentStructure, writerFactory, writerStatus,
                    directoryFactory, folderLayout.getFolder( indexId ), failureStorage,
                    indexId, descriptor );
        }
        else
        {
            return new NonUniqueLuceneIndexPopulator(
                    NonUniqueLuceneIndexPopulator.DEFAULT_QUEUE_THRESHOLD, documentStructure, writerFactory,
                    writerStatus, directoryFactory, folderLayout.getFolder( indexId ), failureStorage, indexId );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config ) throws IOException
    {
        if ( config.isUnique() )
        {
            return new UniqueLuceneIndexAccessor( documentStructure, writerFactory, writerStatus, directoryFactory,
                    folderLayout.getFolder( indexId ) );
        }
        else
        {
            return new NonUniqueLuceneIndexAccessor( documentStructure, writerFactory, writerStatus, directoryFactory,
                    folderLayout.getFolder( indexId ) );
        }
    }

    @Override
    public void init() throws Throwable
    {
        // Simulate a "create", triggered if the root directory doesn't even exist
        boolean firstTime = !fileSystem.fileExists( rootDirectory );
        if ( firstTime )
        {
            fileSystem.mkdirs( rootDirectory );
        }

        /* Why do life.start here in init? It's because in the life of InternalAbstractGraphDatabase the data sources
         * are enlisted before extensions. SchemaIndexProviders comes from extensions. So the order goes:
         *  - dataSource.init
         *  - extensions.init
         *  - dataSource.start
         *  - extensions.start
         *
         * so at the time when the neostore data source starts and requires things from a started
         * schema index provider it's not yet started. Luckily the part we need for now is only this "meta"
         * file and so we can go ahead and start it as part of init instead. This is a general problem
         * in the startup order and dependency tree between neostore data source and schema index providers
         * and so should be fixed on that level instead. */
        life.start();
        if ( firstTime )
        {
            meta.updateRecord( new Record( ProviderMeta.ID_VERSION, CURRENT_VERSION ) );
            meta.force();
        }
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        try
        {
            String failure = failureStorage.loadIndexFailure( indexId );
            if ( failure != null )
            {
                failures.put( indexId, failure );
                return InternalIndexState.FAILED;
            }

            try ( Directory directory = directoryFactory.open( folderLayout.getFolder( indexId ) ) )
            {
                boolean status = writerStatus.isOnline( directory );
                return status ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
            }
        }
        catch( CorruptIndexException e )
        {
            return InternalIndexState.FAILED;
        }
        catch( FileNotFoundException e )
        {
            failures.put( indexId, "File not found: " + e.getMessage() );
            return InternalIndexState.FAILED;
        }
        catch( EOFException e )
        {
            failures.put( indexId, "EOF encountered: " + e.getMessage() );
            return InternalIndexState.FAILED;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        String failure = failureStorage.loadIndexFailure( indexId );
        if ( failure == null )
        {
            failure = failures.get( indexId );
        }
        if ( failure == null )
        {
            throw new IllegalStateException( "Index " + indexId + " isn't failed" );
        }
        return failure;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant()
    {
        return migrator;
    }

    @Override
    public ResourceIterator<File> snapshotMetaFiles()
    {
        Snapshot metaSnapshot = meta.snapshot();
        return resourceIterator( iterator( metaSnapshot.getFile() ), metaSnapshot );
    }
}
