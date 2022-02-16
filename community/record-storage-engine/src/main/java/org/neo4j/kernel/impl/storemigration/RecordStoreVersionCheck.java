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

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator.configureRecordFormat;

public class RecordStoreVersionCheck implements StoreVersionCheck
{
    private final PageCache pageCache;
    private final Path metaDataFile;
    private final RecordFormats configuredFormat;
    private final Config config;
    private final String databaseName;

    public RecordStoreVersionCheck( FileSystemAbstraction fs, PageCache pageCache, RecordDatabaseLayout databaseLayout, InternalLogProvider logProvider,
            Config config, CursorContextFactory contextFactory )
    {
        this( pageCache, databaseLayout, configuredVersion( config, databaseLayout, fs, pageCache, logProvider, contextFactory ), config );
    }

    RecordStoreVersionCheck( PageCache pageCache, RecordDatabaseLayout databaseLayout, RecordFormats configuredFormat, Config config )
    {
        this.pageCache = pageCache;
        this.metaDataFile = databaseLayout.metadataStore();
        this.databaseName = databaseLayout.getDatabaseName();
        this.configuredFormat = configuredFormat;
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
        long record = MetaDataStore.getRecord( pageCache, metaDataFile, STORE_VERSION, databaseName, cursorContext );
        if ( record == MetaDataRecordFormat.FIELD_NOT_PRESENT )
        {
            throw new IllegalStateException( "Uninitialized version field in " + metaDataFile );
        }
        return StoreVersion.versionLongToString( record );
    }

    @Override
    public String configuredVersion()
    {
        configureRecordFormat( configuredFormat, config );
        return configuredFormat.storeVersion();
    }

    @Override
    public boolean isVersionConfigured()
    {
        return StringUtils.isNotEmpty( config.get( GraphDatabaseSettings.record_format ) );
    }

    @Override
    public String getLatestAvailableVersion( String formatFamily, CursorContext cursorContext )
    {
        if ( formatFamily == null )
        {
            try
            {
                String currentVersion = readVersion( cursorContext );
                RecordFormats recordFormats = RecordFormatSelector.selectForVersion( currentVersion );
                formatFamily = recordFormats.getFormatFamily().name();
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "Failed to read the current store version", e );
            }
        }
        return RecordFormatSelector.findLatestFormatInFamily( formatFamily, config ).storeVersion();
    }

    @Override
    public Result checkUpgrade( String desiredVersion, CursorContext cursorContext )
    {
        String version;
        try
        {
            version = readVersion( cursorContext );
        }
        catch ( IllegalStateException e )
        {
            // somehow a corrupt neostore file
            return new Result( Outcome.storeVersionNotFound, null, metaDataFile.getFileName().toString() );
        }
        catch ( IOException e )
        {
            // since we cannot read let's assume the file is not there
            return new Result( Outcome.missingStoreFile, null, metaDataFile.getFileName().toString() );
        }

        if ( desiredVersion.equals( version ) )
        {
            return new Result( Outcome.ok, version, metaDataFile.getFileName().toString() );
        }
        else
        {
            RecordFormats fromFormat;
            try
            {
                RecordFormats toFormat = RecordFormatSelector.selectForVersion( desiredVersion );
                fromFormat = RecordFormatSelector.selectForVersion( version );

                // If we are trying to open an enterprise store when configured to use community format, then inform the user
                // of the config setting to change since downgrades aren't possible but the store can still be opened.
                if ( fromFormat.getFormatFamily().isHigherThan( toFormat.getFormatFamily() ) )
                {
                    return new Result( Outcome.unexpectedUpgradingVersion, version, metaDataFile.toAbsolutePath().toString() );
                }

                if ( fromFormat.getFormatFamily() == toFormat.getFormatFamily() &&
                     ( fromFormat.majorVersion() > toFormat.majorVersion() ||
                       ( fromFormat.majorVersion() == toFormat.majorVersion() && fromFormat.minorVersion() > toFormat.minorVersion() ) ) )
                {
                    // Tried to downgrade, that isn't supported
                    return new Result( Outcome.attemptedStoreDowngrade, fromFormat.storeVersion(), metaDataFile.toAbsolutePath().toString() );
                }
                else
                {
                    return new Result( Outcome.ok, version, metaDataFile.toAbsolutePath().toString() );
                }
            }
            catch ( IllegalArgumentException e )
            {
                return new Result( Outcome.unexpectedStoreVersion, version, metaDataFile.toAbsolutePath().toString() );
            }
        }
    }

    private static RecordFormats configuredVersion( Config config, RecordDatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache,
            InternalLogProvider logProvider, CursorContextFactory contextFactory )
    {
        return RecordFormatSelector.selectNewestFormat( config, databaseLayout, fs, pageCache, logProvider, contextFactory );
    }
}
