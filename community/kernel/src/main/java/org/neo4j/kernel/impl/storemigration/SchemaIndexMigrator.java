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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.kernel.api.index.SchemaIndexProvider.getRootDirectory;
import static org.neo4j.kernel.impl.store.record.SchemaRule.Kind.UNIQUENESS_CONSTRAINT;

public class SchemaIndexMigrator implements StoreMigrationParticipant
{
    private final FileSystemAbstraction fileSystem;
    private final StoreFactory storeFactory;

    public SchemaIndexMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, StoreFactory storeFactory )
    {
        this.fileSystem = fileSystem;
        this.storeFactory = storeFactory;

        storeFactory.setConfig( new Config() );
        storeFactory.setFileSystemAbstraction( fileSystem );
        storeFactory.setIdGeneratorFactory( new DefaultIdGeneratorFactory( fileSystem ) );
        storeFactory.setLogProvider( NullLogProvider.getInstance() );
        storeFactory.setPageCache( pageCache );
    }

    @Override
    public void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider,
            String versionToMigrateFrom ) throws IOException
    {
        switch ( versionToMigrateFrom )
        {
        case Legacy19Store.LEGACY_VERSION:
            break;
        case Legacy20Store.LEGACY_VERSION:
        case Legacy21Store.LEGACY_VERSION:
            deleteIndexesContainingArrayValues( storeDir, schemaIndexProvider );
            break;
        case Legacy22Store.LEGACY_VERSION:
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToMigrateFrom );
        }
    }

    private void deleteIndexesContainingArrayValues( File storeDir,
                                                     SchemaIndexProvider schemaIndexProvider ) throws IOException
    {
        File indexRoot = getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( new Config() );
        List<File> indexesToBeDeleted = new ArrayList<>();
        storeFactory.setStoreDir( storeDir );
        try ( NeoStores neoStores = storeFactory.openNeoStores( NeoStores.StoreType.SCHEMA ) )
        {
            SchemaStore schema = neoStores.getSchemaStore();
            Iterator<SchemaRule> rules = schema.loadAllSchemaRules();
            while ( rules.hasNext() )
            {
                SchemaRule rule = rules.next();
                IndexConfiguration indexConfig = new IndexConfiguration( rule.getKind() == UNIQUENESS_CONSTRAINT );
                try ( IndexAccessor accessor =
                              schemaIndexProvider.getOnlineAccessor( rule.getId(), indexConfig, samplingConfig ) )
                {
                    try ( IndexReader reader = accessor.newReader() )
                    {
                        if ( reader.valueTypesInIndex().contains( Array.class ) )
                        {
                            indexesToBeDeleted.add( new File( indexRoot, "" + rule.getId() ) );
                        }
                    }
                }
            }
        }

        for ( File index : indexesToBeDeleted )
        {
            fileSystem.deleteRecursively( index );
        }

    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom ) throws IOException
    { // nothing to do
    }

    @Override
    public void rebuildCounts( File storeDir, String versionToMigrateFrom ) throws IOException
    { // nothing to do
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    { // nothing to do
    }
}
