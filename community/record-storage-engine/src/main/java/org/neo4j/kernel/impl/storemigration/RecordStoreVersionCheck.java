/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.logging.LogProvider;
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

    public RecordStoreVersionCheck( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout, LogProvider logProvider, Config config,
            PageCacheTracer pageCacheTracer )
    {
        this( pageCache, databaseLayout, configuredVersion( config, databaseLayout, fs, pageCache, logProvider, pageCacheTracer ), config );
    }

    RecordStoreVersionCheck( PageCache pageCache, DatabaseLayout databaseLayout, RecordFormats configuredFormat, Config config )
    {
        this.pageCache = pageCache;
        this.metaDataFile = databaseLayout.metadataStore();
        this.configuredFormat = configuredFormat;
        this.config = config;
    }

    @Override
    public Optional<String> storeVersion( PageCursorTracer cursorTracer )
    {
        try
        {
            String version = readVersion( cursorTracer );
            return Optional.of( version );
        }
        catch ( IOException e )
        {
            return Optional.empty();
        }
    }

    private String readVersion( PageCursorTracer cursorTracer ) throws IOException
    {
        long record = MetaDataStore.getRecord( pageCache, metaDataFile, STORE_VERSION, cursorTracer );
        if ( record == MetaDataRecordFormat.FIELD_NOT_PRESENT )
        {
            throw new IllegalStateException( "Uninitialized version field in " + metaDataFile );
        }
        return MetaDataStore.versionLongToString( record );
    }

    @Override
    public StoreVersion versionInformation( String storeVersion )
    {
        return new RecordStoreVersion( RecordFormatSelector.selectForVersion( storeVersion ) );
    }

    @Override
    public String configuredVersion()
    {
        configureRecordFormat( configuredFormat, config );
        return configuredFormat.storeVersion();
    }

    @Override
    public Result checkUpgrade( String desiredVersion, PageCursorTracer cursorTracer )
    {
        String version;
        try
        {
            version = readVersion( cursorTracer );
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
                RecordFormats format = RecordFormatSelector.selectForVersion( desiredVersion );
                fromFormat = RecordFormatSelector.selectForVersion( version );

                // If we are trying to open an enterprise store when configured to use community format, then inform the user
                // of the config setting to change since downgrades aren't possible but the store can still be opened.
                if ( FormatFamily.isLowerFamilyFormat( format, fromFormat ) )
                {
                    return new Result( Outcome.unexpectedUpgradingVersion, version, metaDataFile.toAbsolutePath().toString() );
                }

                if ( FormatFamily.isSameFamily( fromFormat, format ) && (fromFormat.generation() > format.generation()) )
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

    private static RecordFormats configuredVersion( Config config, DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache,
            LogProvider logProvider, PageCacheTracer pageCacheTracer )
    {
        return RecordFormatSelector.selectNewestFormat( config, databaseLayout, fs, pageCache, logProvider, pageCacheTracer );
    }
}
