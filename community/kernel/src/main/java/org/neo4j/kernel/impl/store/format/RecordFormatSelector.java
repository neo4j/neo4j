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
package org.neo4j.kernel.impl.store.format;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.logging.LogProvider;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.concat;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

/**
 * Selects record format that will be used in a database.
 * Supports selection based on the existing store and given configuration.
 * <p>
 * Automatic selection is used by various tools and tests that should pretend being format independent (for
 * example backup).
 */
public class RecordFormatSelector
{
    private static final RecordFormats DEFAULT_FORMAT = Standard.LATEST_RECORD_FORMATS;

    private static final List<RecordFormats> KNOWN_FORMATS = asList(
            StandardV2_3.RECORD_FORMATS,
            StandardV3_0.RECORD_FORMATS,
            StandardV3_2.RECORD_FORMATS,
            StandardV3_4.RECORD_FORMATS
    );

    private RecordFormatSelector()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    /**
     * Select {@link #DEFAULT_FORMAT} record format.
     *
     * @return default record format.
     */
    @Nonnull
    public static RecordFormats defaultFormat()
    {
        return DEFAULT_FORMAT;
    }

    /**
     * Select record formats for provided store version.
     *
     * @param storeVersion store version to find format for
     * @return record formats
     * @throws IllegalArgumentException if format for specified store version not found
     */
    @Nonnull
    public static RecordFormats selectForVersion( String storeVersion )
    {
        for ( RecordFormats format : allFormats() )
        {
            if ( format.storeVersion().equals( storeVersion ) )
            {
                return format;
            }
        }
        throw new IllegalArgumentException( "Unknown store version '" + storeVersion + "'" );
    }

    /**
     * Select configured record format based on available services in class path.
     * Specific format can be specified by {@link GraphDatabaseSettings#record_format} property.
     * <p>
     * If format is not specified {@link #DEFAULT_FORMAT} will be used.
     *
     * @param config configuration parameters
     * @param logProvider logging provider
     * @return selected record format
     * @throws IllegalArgumentException if requested format not found
     */
    @Nonnull
    public static RecordFormats selectForConfig( Config config, LogProvider logProvider )
    {
        String recordFormat = configuredRecordFormat( config );
        if ( StringUtils.isEmpty( recordFormat ) )
        {
            info( logProvider, "Record format not configured, selected default: " + defaultFormat() );
            return defaultFormat();
        }
        RecordFormats format = selectSpecificFormat( recordFormat );
        info( logProvider, "Selected record format based on config: " + format );
        return format;
    }

    /**
     * Select record format for the given store directory.
     * <p>
     * <b>Note:</b> package private only for testing.
     *
     * @param storeDir directory with the store
     * @param pageCache page cache to read store files
     * @return record format of the given store or <code>null</code> if {@value MetaDataStore#DEFAULT_NAME} file not
     * found or can't be read
     */
    @Nullable
    static RecordFormats selectForStore( File storeDir, PageCache pageCache, LogProvider logProvider )
    {
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        // It's important for the block device support, that we use the page cache file system to check for the
        // neostore file.
        if ( pageCache.getCachedFileSystem().fileExists( neoStoreFile ) )
        {
            try
            {
                long value = MetaDataStore.getRecord( pageCache, neoStoreFile, STORE_VERSION );
                if ( value != MetaDataRecordFormat.FIELD_NOT_PRESENT )
                {
                    String storeVersion = MetaDataStore.versionLongToString( value );

                    for ( RecordFormats format : allFormats() )
                    {
                        if ( format.storeVersion().equals( storeVersion ) )
                        {
                            info( logProvider, "Selected " + format + " record format from store " + storeDir );
                            return format;
                        }
                    }
                }
            }
            catch ( IOException e )
            {
                info( logProvider, "Unable to read store format: " + e.getMessage() );
            }
        }
        return null;
    }

    /**
     * Select record format for the given store (if exists) or from the given configuration. If there is no store and
     * record format is not configured than {@link #DEFAULT_FORMAT} is selected.
     *
     * @param config configuration parameters
     * @param storeDir directory with the store
     * @param pageCache page cache to read store files
     * @return record format from the store (if it can be read) or configured record format or {@link #DEFAULT_FORMAT}
     * @throws IllegalArgumentException when configured format is different from the format present in the store
     */
    @Nonnull
    public static RecordFormats selectForStoreOrConfig(
            Config config, File storeDir, PageCache pageCache, LogProvider logProvider )
    {
        RecordFormats configuredFormat = loadRecordFormat( configuredRecordFormat( config ) );
        boolean formatConfigured = configuredFormat != null;

        RecordFormats currentFormat = selectForStore( storeDir, pageCache, logProvider );
        boolean storeWithFormatExists = currentFormat != null;

        if ( formatConfigured && storeWithFormatExists )
        {
            if ( currentFormat.getFormatFamily().equals( configuredFormat.getFormatFamily() ) &&
                 (currentFormat.generation() == configuredFormat.generation()) )
            {
                info( logProvider, "Configured format matches format in the store. Selected: " + currentFormat );
                return currentFormat;
            }
            throw new IllegalArgumentException( String.format(
                    "Configured format '%s' is different from the actual format in the store '%s'",
                    configuredFormat, currentFormat ) );
        }

        if ( !formatConfigured && storeWithFormatExists )
        {
            info( logProvider, "Format not configured. Selected format from the store: " + currentFormat );
            return currentFormat;
        }

        if ( formatConfigured )
        {
            info( logProvider, "Selected configured format: " + configuredFormat );
            return configuredFormat;
        }

        return DEFAULT_FORMAT;
    }

    /**
     * Check if store and configured formats are compatible. In case if format is not configured or store does not
     * exist yet - we consider formats as compatible.
     * @param config configuration parameters
     * @param storeDir directory with the store
     * @param pageCache page cache to read store files
     * @param logProvider log provider
     * @return true if configured and actual format is compatible, false otherwise.
     */
    public static boolean isStoreAndConfigFormatsCompatible(
            Config config, File storeDir, PageCache pageCache, LogProvider logProvider )
    {
        RecordFormats configuredFormat = loadRecordFormat( configuredRecordFormat( config ) );

        RecordFormats currentFormat = selectForStore( storeDir, pageCache, logProvider );

        return (configuredFormat == null) || (currentFormat == null) ||
                (currentFormat.getFormatFamily().equals( configuredFormat.getFormatFamily() ) &&
                (currentFormat.generation() == configuredFormat.generation()));
    }

    /**
     * Select explicitly configured record format (via given {@code config}) or format from the store. If store does
     * not exist or has old format ({@link RecordFormats#generation()}) than this method returns
     * {@link #DEFAULT_FORMAT}.
     *
     * @param config configuration parameters
     * @param storeDir directory with the store
     * @param pageCache page cache to read store files
     * @return record format from the store (if it can be read) or configured record format or {@link #DEFAULT_FORMAT}
     * @see RecordFormats#generation()
     */
    @Nonnull
    public static RecordFormats selectNewestFormat(
            Config config, File storeDir, PageCache pageCache, LogProvider logProvider )
    {
        boolean formatConfigured = StringUtils.isNotEmpty( configuredRecordFormat( config ) );
        if ( formatConfigured )
        {
            // format was explicitly configured so select it
            return selectForConfig( config, logProvider );
        }
        else
        {
            RecordFormats result = selectForStore( storeDir, pageCache, logProvider );
            if ( result == null )
            {
                // format was not explicitly configured and store does not exist, select default format
                info( logProvider, "Selected format '" + DEFAULT_FORMAT + "' for the new store" );
                result = DEFAULT_FORMAT;
            }
            else if ( FormatFamily.isHigherFamilyFormat( DEFAULT_FORMAT, result ) ||
                      (FormatFamily.isSameFamily( result, DEFAULT_FORMAT ) && (result.generation() < DEFAULT_FORMAT.generation())) )
            {
                // format was not explicitly configured and store has lower format
                // select default format, upgrade is intended
                info( logProvider,
                        "Selected format '" + DEFAULT_FORMAT + "' for existing store with format '" + result + "'" );
                result = DEFAULT_FORMAT;
            }
            return result;
        }
    }

    /**
     * Finds which format, if any, succeeded the specified format. Only formats in the same family are considered.
     *
     * @param format to find successor to.
     * @return the format with the lowest generation > format.generation, or None if no such format is known.
     */
    @Nonnull
    public static Optional<RecordFormats> findSuccessor( @Nonnull final RecordFormats format )
    {
        return StreamSupport.stream( RecordFormatSelector.allFormats().spliterator(), false )
                .filter( candidate -> FormatFamily.isSameFamily( format, candidate ) )
                .filter( candidate -> candidate.generation() > format.generation() )
                .reduce( ( a, b ) -> a.generation() < b.generation() ? a : b );
    }

    /**
     * Gets all {@link RecordFormats} that the selector is aware of.
     * @return An iterable over all known record formats.
     */
    public static Iterable<RecordFormats> allFormats()
    {
        Iterable<RecordFormats.Factory> loadableFormatFactories = Service.load( RecordFormats.Factory.class );
        Iterable<RecordFormats> loadableFormats = map( RecordFormats.Factory::newInstance, loadableFormatFactories );
        return concat( KNOWN_FORMATS, loadableFormats );
    }

    @Nonnull
    private static RecordFormats selectSpecificFormat( String recordFormat )
    {
        RecordFormats formats = loadRecordFormat( recordFormat );
        if ( formats == null )
        {
            throw new IllegalArgumentException( "No record format found with the name '" + recordFormat + "'." );
        }
        return formats;
    }

    @Nullable
    private static RecordFormats loadRecordFormat( String recordFormat )
    {
        if ( StringUtils.isNotEmpty( recordFormat ) )
        {
            if ( Standard.LATEST_NAME.equals( recordFormat ) )
            {
                return Standard.LATEST_RECORD_FORMATS;
            }
            for ( RecordFormats knownFormat : KNOWN_FORMATS )
            {
                if ( recordFormat.equals( knownFormat.name() ) )
                {
                    return knownFormat;
                }
            }
            RecordFormats.Factory formatFactory = Service.loadSilently( RecordFormats.Factory.class, recordFormat );
            if ( formatFactory != null )
            {
                return formatFactory.newInstance();
            }
        }
        return null;
    }

    private static void info( LogProvider logProvider, String message )
    {
        logProvider.getLog( RecordFormatSelector.class ).info( message );
    }

    @Nonnull
    private static String configuredRecordFormat( Config config )
    {
        return config.get( GraphDatabaseSettings.record_format );
    }
}
