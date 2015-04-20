/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;

import static org.neo4j.kernel.api.index.SchemaIndexProvider.getRootDirectory;
import static org.neo4j.kernel.impl.store.record.SchemaRule.Kind.UNIQUENESS_CONSTRAINT;

public class SchemaIndexMigrator implements StoreMigrationParticipant
{
    public interface SchemaStoreProvider
    {
        SchemaStore provide( File storeDir, PageCache pageCache );
    }

    private final FileSystemAbstraction fileSystem;
    private final UpgradableDatabase upgradableDatabase;
    private final SchemaStoreProvider schemaStoreProvider;
    private String versionToUpgradeFrom;

    public SchemaIndexMigrator( FileSystemAbstraction fileSystem, UpgradableDatabase upgradableDatabase,
                                SchemaStoreProvider schemaStoreProvider )
    {
        this.fileSystem = fileSystem;
        this.upgradableDatabase = upgradableDatabase;
        this.schemaStoreProvider = schemaStoreProvider;
    }

    @Override
    public boolean needsMigration( File storeDir ) throws IOException
    {
        if ( upgradableDatabase.hasCurrentVersion( fileSystem, storeDir ) )
        {
            return false;
        }

        switch ( versionToUpgradeFrom( storeDir ) )
        {
        case Legacy19Store.LEGACY_VERSION:
            return false;
        case Legacy20Store.LEGACY_VERSION:
        case Legacy21Store.LEGACY_VERSION:
            return true;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom( storeDir ) );
        }
    }

    @Override
    public void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider,
                         PageCache pageCache ) throws IOException
    {
        switch ( versionToUpgradeFrom( storeDir ) )
        {
        case Legacy20Store.LEGACY_VERSION:
        case Legacy21Store.LEGACY_VERSION:
            deleteIndexesContainingArrayValues( storeDir, pageCache, schemaIndexProvider );
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom( storeDir ) );
        }
    }

    private void deleteIndexesContainingArrayValues( File storeDir, PageCache pageCache,
                                                     SchemaIndexProvider schemaIndexProvider ) throws IOException
    {
        File indexRoot = getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( new Config() );
        List<File> indexesToBeDeleted = new ArrayList<>();
        try ( SchemaStore schema = schemaStoreProvider.provide( storeDir, pageCache ) )
        {
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
    public void moveMigratedFiles( File migrationDir, File storeDir ) throws IOException
    { // nothing to do
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    { // nothing to do
    }

    @Override
    public void close()
    { // nothing to do
    }

    /**
     * Will detect which version we're upgrading from.
     * Doing that initialization here is good because we do this check when
     * {@link #moveMigratedFiles(java.io.File, java.io.File) moving migrated files}, which might be done
     * as part of a resumed migration, i.e. run even if
     * {@link org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant#migrate(java.io.File, java.io.File,
     * SchemaIndexProvider, org.neo4j.io.pagecache.PageCache)}
     * hasn't been run.
     */
    private String versionToUpgradeFrom( File storeDir )
    {
        if ( versionToUpgradeFrom == null )
        {
            versionToUpgradeFrom = upgradableDatabase.checkUpgradeable( storeDir );
        }
        return versionToUpgradeFrom;
    }
}
