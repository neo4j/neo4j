/*
 * Copyright (c) "Neo4j"
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class RecordStoreVersionCheck implements StoreVersionCheck
{
    private final PageCache pageCache;
    private final Path metaDataFile;
    private final Config config;
    private final String databaseName;

    public RecordStoreVersionCheck( PageCache pageCache, RecordDatabaseLayout databaseLayout, Config config )
    {
        this.pageCache = pageCache;
        this.metaDataFile = databaseLayout.metadataStore();
        this.databaseName = databaseLayout.getDatabaseName();
        this.config = config;
    }

    @Override
    public Optional<String> storeVersion( CursorContext cursorContext )
    {
        try
        {
            String version = readVersion( cursorContext );
            return Optional.of( version );
        }
        catch ( IOException e )
        {
            return Optional.empty();
        }
    }

    @Override
    public String storeVersionToString( long storeVersion )
    {
        return StoreVersion.versionLongToString( storeVersion );
    }

    private String readVersion( CursorContext cursorContext ) throws IOException
    {
        long version = MetaDataStore.getRecord( pageCache, metaDataFile, STORE_VERSION, databaseName, cursorContext );
        if ( version == MetaDataRecordFormat.FIELD_NOT_PRESENT )
        {
            throw new IllegalStateException( "Uninitialized version field in " + metaDataFile );
        }
        return StoreVersion.versionLongToString( version );
    }

    @Override
    public MigrationCheckResult getAndCheckMigrationTargetVersion( String formatFamily, CursorContext cursorContext )
    {
        RecordFormats formatToMigrateFrom;
        try
        {
            String currentVersion = readVersion( cursorContext );
            formatToMigrateFrom = RecordFormatSelector.selectForVersion( currentVersion );
        }
        catch ( Exception e )
        {
            return new MigrationCheckResult( MigrationOutcome.STORE_VERSION_RETRIEVAL_FAILURE, null, null, e );
        }

        if ( formatFamily == null )
        {
            formatFamily = formatToMigrateFrom.getFormatFamily().name();
        }

        RecordFormats formatToMigrateTo = RecordFormatSelector.findLatestFormatInFamily( formatFamily, config );

        if ( formatToMigrateTo.onlyForMigration() )
        {
            return new MigrationCheckResult( MigrationOutcome.UNSUPPORTED_TARGET_VERSION, formatToMigrateFrom.storeVersion(), formatToMigrateTo.storeVersion(),
                    null );
        }

        if ( formatToMigrateFrom.equals( formatToMigrateTo ) )
        {
            return new MigrationCheckResult( MigrationOutcome.NO_OP, formatToMigrateFrom.storeVersion(), formatToMigrateTo.storeVersion(), null );
        }

        if ( formatToMigrateFrom.getFormatFamily().isHigherThan( formatToMigrateTo.getFormatFamily() ) )
        {
            return new MigrationCheckResult( MigrationOutcome.UNSUPPORTED_MIGRATION_PATH, formatToMigrateFrom.storeVersion(), formatToMigrateTo.storeVersion(),
                    null );
        }

        return new MigrationCheckResult( MigrationOutcome.MIGRATION_POSSIBLE, formatToMigrateFrom.storeVersion(), formatToMigrateTo.storeVersion(), null );
    }

    @Override
    public UpgradeCheckResult getAndCheckUpgradeTargetVersion( CursorContext cursorContext )
    {
        RecordFormats formatToUpgradeFrom;
        try
        {
            String currentVersion = readVersion( cursorContext );
            formatToUpgradeFrom = RecordFormatSelector.selectForVersion( currentVersion );
        }
        catch ( Exception e )
        {
            return new UpgradeCheckResult( UpgradeOutcome.STORE_VERSION_RETRIEVAL_FAILURE, null, null, e );
        }

        RecordFormats formatToUpgradeTo = RecordFormatSelector.findLatestMinorVersion( formatToUpgradeFrom, config );

        if ( formatToUpgradeTo.onlyForMigration() )
        {
            return new UpgradeCheckResult( UpgradeOutcome.UNSUPPORTED_TARGET_VERSION, formatToUpgradeFrom.storeVersion(), formatToUpgradeTo.storeVersion(),
                    null );
        }

        if ( formatToUpgradeFrom.equals( formatToUpgradeTo ) )
        {
            return new UpgradeCheckResult( UpgradeOutcome.NO_OP, formatToUpgradeFrom.storeVersion(), formatToUpgradeTo.storeVersion(), null );
        }

        return new UpgradeCheckResult( UpgradeOutcome.UPGRADE_POSSIBLE, formatToUpgradeFrom.storeVersion(), formatToUpgradeTo.storeVersion(), null );
    }
}
