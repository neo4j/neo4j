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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;

import static org.neo4j.io.fs.FileUtils.path;

public class IndexProviderMigrator extends AbstractStoreMigrationParticipant
{
    private final FileSystemAbstraction fs;
    private final Config config;
    private final PageCache pageCache;
    private final LogService logService;
    private final StorageEngineFactory storageEngineFactory;

    IndexProviderMigrator( FileSystemAbstraction fs, Config config, PageCache pageCache, LogService logService, StorageEngineFactory storageEngineFactory )
    {
        super( "Index providers" );
        this.fs = fs;
        this.config = config;
        this.pageCache = pageCache;
        this.logService = logService;
        this.storageEngineFactory = storageEngineFactory;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progress, String versionToMigrateFrom,
            String versionToMigrateTo ) throws KernelException, IOException
    {
        try ( SchemaRuleMigrationAccess ruleAccess = storageEngineFactory
                .schemaRuleMigrationAccess( fs, pageCache, config, migrationLayout, logService, versionToMigrateTo ) )
        {
            for ( SchemaRule rule : ruleAccess.getAll() )
            {
                SchemaRule upgraded = upgradeIndexProvider( rule );

                if ( upgraded != rule )
                {
                    ruleAccess.writeSchemaRule( upgraded );
                }
            }
        }
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToMigrateFrom, String versionToMigrateTo )
            throws IOException
    {
        for ( RetiredIndexProvider retiredIndexProvider : RetiredIndexProvider.values() )
        {
            fs.deleteRecursively( retiredIndexProvider.providerRootDirectory( directoryLayout ) );
        }
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout )
    {
        // Nothing to clean up.
    }

    private SchemaRule upgradeIndexProvider( SchemaRule rule )
    {
        if ( rule instanceof DefaultStorageIndexReference )
        {
            DefaultStorageIndexReference oldIndexReference = (DefaultStorageIndexReference) rule;

            String currentKey = oldIndexReference.providerKey();
            String currentVersion = oldIndexReference.providerVersion();

            for ( RetiredIndexProvider retired : RetiredIndexProvider.values() )
            {
                if ( currentKey.equals( retired.providerKey ) && currentVersion.equals( retired.providerVersion ) )
                {
                    SchemaIndex replacement = retired.desiredAlternativeProvider;
                    String replacementKey = replacement.providerKey();
                    String replacementVersion = replacement.providerVersion();
                    return oldIndexReference.withIndexProvider( new IndexProviderDescriptor( replacementKey, replacementVersion ) );
                }
            }
        }
        return rule;
    }

    enum RetiredIndexProvider
    {
        LUCENE( "lucene", "1.0", SchemaIndex.NATIVE30 )
                {
                    @Override
                    File providerRootDirectory( DatabaseLayout layout )
                    {
                        return directoryRootByProviderKey( layout.databaseDirectory(), providerKey );
                    }
                },
        NATIVE10( "lucene+native", "1.0", SchemaIndex.NATIVE30 )
                {
                    @Override
                    File providerRootDirectory( DatabaseLayout layout )
                    {
                        return directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    }
                },
        NATIVE20( "lucene+native", "2.0", SchemaIndex.NATIVE_BTREE10 )
                {
                    @Override
                    File providerRootDirectory( DatabaseLayout layout )
                    {
                        return directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    }
                };

        final String providerKey;
        final String providerVersion;
        final SchemaIndex desiredAlternativeProvider;

        RetiredIndexProvider( String providerKey, String providerVersion, SchemaIndex desiredAlternativeProvider )
        {
            this.providerKey = providerKey;
            this.providerVersion = providerVersion;
            this.desiredAlternativeProvider = desiredAlternativeProvider;
        }

        abstract File providerRootDirectory( DatabaseLayout layout );
    }

    /**
     * Returns the base schema index directory, i.e.
     *
     * <pre>
     * &lt;db&gt;/schema/index/
     * </pre>
     *
     * @param databaseStoreDir database store directory, i.e. {@code db} in the example above, where e.g. {@code nodestore} lives.
     * @return the base directory of schema indexing.
     */
    private static File baseSchemaIndexFolder( File databaseStoreDir )
    {
        return path( databaseStoreDir, "schema", "index" );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static File directoryRootByProviderKey( File databaseStoreDir, String providerKey )
    {
        return path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( providerKey ) );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static File directoryRootByProviderKeyAndVersion( File databaseStoreDir, String providerKey, String providerVersion )
    {
        return path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( providerKey + "-" + providerVersion ) );
    }

    private static String fileNameFriendly( String name )
    {
        return name.replaceAll( "\\+", "_" );
    }
}
