/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.util.VisibleForTesting;

public abstract class AbstractLuceneIndexProvider extends IndexProvider
{
    private final IndexStorageFactory indexStorageFactory;
    protected final Config config;
    protected final OperationalMode operationalMode;
    protected final FileSystemAbstraction fileSystem;

    protected AbstractLuceneIndexProvider( IndexProviderDescriptor descriptor, int priority, IndexDirectoryStructure.Factory directoryStructureFactory,
            Config config, OperationalMode operationalMode, FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
    {
        super( descriptor, priority, directoryStructureFactory );
        this.config = config;
        this.operationalMode = operationalMode;
        this.fileSystem = fileSystem;
        this.indexStorageFactory = buildIndexStorageFactory( fileSystem, directoryFactory );
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( final FileSystemAbstraction fs, PageCache pageCache )
    {
        return new SchemaIndexMigrator( fs, this );
    }

    @Override
    public String getPopulationFailure( StoreIndexDescriptor descriptor ) throws IllegalStateException
    {
        String failure = getIndexStorage( descriptor.getId() ).getStoredIndexFailure();
        if ( failure == null )
        {
            throw new IllegalStateException( "Index " + descriptor.getId() + " isn't failed" );
        }
        return failure;
    }

    @Override
    public IndexCapability getCapability()
    {
        return IndexCapability.NO_CAPABILITY;
    }

    protected PartitionedIndexStorage getIndexStorage( long indexId )
    {
        return indexStorageFactory.indexStorageOf( indexId );
    }

    protected boolean indexIsOnline( PartitionedIndexStorage indexStorage, StoreIndexDescriptor descriptor ) throws IOException
    {
        try ( SchemaIndex index = LuceneSchemaIndexBuilder.create( descriptor, config ).withIndexStorage( indexStorage ).build() )
        {
            if ( index.exists() )
            {
                index.open();
                return index.isOnline();
            }
            return false;
        }
    }

    /**
     * Visible <b>only</b> for testing.
     */
    @VisibleForTesting
    protected IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
    {
        return new IndexStorageFactory( directoryFactory, fileSystem, directoryStructure() );
    }
}
