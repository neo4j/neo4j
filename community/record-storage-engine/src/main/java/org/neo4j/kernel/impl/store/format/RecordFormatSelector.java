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
package org.neo4j.kernel.impl.store.format;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_1;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_3;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.logging.LogProvider;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.format.CapabilityType;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparingInt;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.concat;
import static org.neo4j.internal.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.format.FormatFamily.isSameFamily;

/**
 * Selects record format that will be used in a database.
 * Supports selection based on the existing store and given configuration.
 * <p>
 * Automatic selection is used by various tools and tests that should pretend being format independent (for
 * example backup).
 */
public class RecordFormatSelector
{
    private static final String STORE_SELECTION_TAG = "storeSelection";
    private static final RecordFormats DEFAULT_FORMAT = PageAlignedV4_3.RECORD_FORMATS;

    private static final List<RecordFormats> KNOWN_FORMATS = asList(
            StandardV3_4.RECORD_FORMATS,
            StandardV4_0.RECORD_FORMATS,
            StandardV4_3.RECORD_FORMATS,
            PageAlignedV4_1.RECORD_FORMATS,
            PageAlignedV4_3.RECORD_FORMATS
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
     * @param databaseLayout directory with the store
     * @param fs file system used to access store files
     * @param pageCache page cache to read store files
     * @param pageCacheTracer underlying page cache operations tracer.
     * @return record format of the given store or <code>null</code> if {@link DatabaseLayout#metadataStore()} file not
     * found or can't be read
     */
    @Nullable
    public static RecordFormats selectForStore( DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider,
            PageCacheTracer pageCacheTracer )
    {
        Path neoStoreFile = databaseLayout.metadataStore();
        if ( fs.fileExists( neoStoreFile ) )
        {
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( STORE_SELECTION_TAG ) )
            {
                long value = MetaDataStore.getRecord( pageCache, neoStoreFile, STORE_VERSION, databaseLayout.getDatabaseName(), cursorTracer );
                if ( value != MetaDataRecordFormat.FIELD_NOT_PRESENT )
                {
                    String storeVersion = MetaDataStore.versionLongToString( value );

                    for ( RecordFormats format : allFormats() )
                    {
                        if ( format.storeVersion().equals( storeVersion ) )
                        {
                            info( logProvider, "Selected " + format + " record format from store " + databaseLayout.databaseDirectory() );
                            return format;
                        }
                    }
                }
            }
            catch ( IOException e )
            {
                info( logProvider, format( "Unable to read format for store %s. %s ", databaseLayout.databaseDirectory(), e.getMessage() ) );
            }
        }
        return null;
    }

    /**
     * Select record format for the given store (if exists) or from the given configuration. If there is no store and
     * record format is not configured than {@link #DEFAULT_FORMAT} is selected.
     *
     * @param config configuration parameters
     * @param databaseLayout database directory structure
     * @param fs file system used to access store files
     * @param pageCache page cache to read store files
     * @param pageCacheTracer underlying page cache operations tracer.
     * @return record format from the store (if it can be read) or configured record format or {@link #DEFAULT_FORMAT}
     * @throws IllegalArgumentException when configured format is different from the format present in the store
     */
    @Nonnull
    public static RecordFormats selectForStoreOrConfig( Config config, DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache,
            LogProvider logProvider, PageCacheTracer pageCacheTracer )
    {
        RecordFormats configuredFormat = getConfiguredRecordFormat( config, databaseLayout );
        boolean formatConfigured = configuredFormat != null;

        RecordFormats currentFormat = selectForStore( databaseLayout, fs, pageCache, logProvider, pageCacheTracer );
        boolean storeWithFormatExists = currentFormat != null;

        if ( formatConfigured && storeWithFormatExists )
        {
            if ( formatSameFamilyAndGeneration( currentFormat, configuredFormat ) )
            {
                info( logProvider, format( "Configured format matches format in the store %s. Selected: %s",
                        databaseLayout.databaseDirectory(), currentFormat ) );
                return currentFormat;
            }
            throw new IllegalArgumentException( format(
                    "Configured format '%s' is different from the actual format in the store %s, which was '%s'",
                    configuredFormat, databaseLayout.databaseDirectory(), currentFormat ) );
        }

        if ( !formatConfigured && storeWithFormatExists )
        {
            info( logProvider, format( "Format not configured for store %s. Selected format from the store files: %s",
                    databaseLayout.databaseDirectory(), currentFormat ) );
            return currentFormat;
        }

        if ( formatConfigured )
        {
            info( logProvider, format( "Selected configured format for store %s: %s", databaseLayout.databaseDirectory(), configuredFormat ) );
            return configuredFormat;
        }

        return DEFAULT_FORMAT;
    }

    private static RecordFormats getConfiguredRecordFormat( Config config, DatabaseLayout databaseLayout )
    {
        if ( SYSTEM_DATABASE_NAME.equals( databaseLayout.getDatabaseName() ) )
        {
            // TODO: System database does not support multiple formats, remove this when it does!
            return null;
        }
        return loadRecordFormat( configuredRecordFormat( config ) );
    }

    private static boolean formatSameFamilyAndGeneration( RecordFormats left, RecordFormats right )
    {
        return left.getFormatFamily().equals( right.getFormatFamily() ) && left.generation() == right.generation();
    }

    /**
     * Check if a format is compatible with another format. In case if format is not configured or store does not
     * exist yet - we consider formats as compatible.
     * @param format a {@link RecordFormats}
     * @param otherFormat another {@link RecordFormats} to compare with.
     * @return true if the two formats are compatible, false otherwise.
     */
    public static boolean isStoreAndConfigFormatsCompatible( RecordFormats format, RecordFormats otherFormat )
    {
        return (format == null) || (otherFormat == null) || formatSameFamilyAndGeneration( format, otherFormat );
    }

    /**
     * Check if a format is compatible with another format. This will return {@code true} if all the following are true:
     * <ul>
     * <li>the formats are of the same family</li>
     * <li>{@code format} has a {@link RecordFormats#generation() generation} equal to or lower than {@code otherFormat}</li>
     * <li>the two formats has {@link RecordFormats#hasCompatibleCapabilities(RecordFormats, CapabilityType) compatible capabilities}</li>
     * </ul>
     * @param format a {@link RecordFormats}
     * @param otherFormat another {@link RecordFormats} to compare with.
     * @return {@code true} if the two formats are compatible (with lee-way for being trivially upgradable, {@code false} otherwise.
     */
    public static boolean isStoreFormatsCompatibleIncludingMinorUpgradable( RecordFormats format, RecordFormats otherFormat )
    {
        Objects.requireNonNull( format );
        Objects.requireNonNull( otherFormat );
        return format.getFormatFamily().equals( otherFormat.getFormatFamily() ) &&
                format.generation() <= otherFormat.generation() &&
                format.hasCompatibleCapabilities( otherFormat, CapabilityType.FORMAT ) &&
                format.hasCompatibleCapabilities( otherFormat, CapabilityType.STORE );
    }

    /**
     * Select explicitly configured record format (via given {@code config}) or format from the store. If store does
     * not exist or has old format ({@link RecordFormats#generation()}) than this method returns
     * {@link #DEFAULT_FORMAT}.
     *
     * @param config configuration parameters
     * @param databaseLayout database directory structure
     * @param fs file system used to access store files
     * @param pageCache page cache to read store files
     * @param pageCacheTracer underlying page cache operations tracer.
     * @return record format from the store (if it can be read) or configured record format or {@link #DEFAULT_FORMAT}
     * @see RecordFormats#generation()
     */
    @Nonnull
    public static RecordFormats selectNewestFormat( Config config, DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache,
            LogProvider logProvider, PageCacheTracer pageCacheTracer )
    {
        boolean formatConfigured = StringUtils.isNotEmpty( configuredRecordFormat( config ) );
        if ( formatConfigured )
        {
            // format was explicitly configured so select it
            return selectForConfig( config, logProvider );
        }
        else
        {
            final RecordFormats result = selectForStore( databaseLayout, fs, pageCache, logProvider, pageCacheTracer );
            if ( result == null )
            {
                // format was not explicitly configured and store does not exist, select default format
                info( logProvider, format( "Selected format '%s' for the new store %s", DEFAULT_FORMAT, databaseLayout.databaseDirectory() ) );
                return DEFAULT_FORMAT;
            }
            Optional<RecordFormats> newestFormatInFamily = findLatestFormatInFamily( result );
            RecordFormats newestFormat = newestFormatInFamily.orElse( result );
            info( logProvider, format( "Selected format '%s' for existing store %s with format '%s'",
                    newestFormat, databaseLayout.databaseDirectory(), result ) );
            return newestFormat;
        }
    }

    public static Optional<RecordFormats> findLatestFormatInFamily( RecordFormats result )
    {
        return Iterables.stream( allFormats() )
                .filter( format -> isSameFamily( result, format ) )
                .max( comparingInt( RecordFormats::generation ) );
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
                .filter( candidate -> isSameFamily( format, candidate ) )
                .filter( candidate -> candidate.generation() > format.generation() )
                .reduce( ( a, b ) -> a.generation() < b.generation() ? a : b );
    }

    /**
     * Gets all {@link RecordFormats} that the selector is aware of.
     * @return An iterable over all known record formats.
     */
    public static Iterable<RecordFormats> allFormats()
    {
        Iterable<RecordFormats.Factory> loadableFormatFactories = Services.loadAll( RecordFormats.Factory.class );
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
        if ( StringUtils.isEmpty( recordFormat ) )
        {
            return null;
        }
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
        return Services.load( RecordFormats.Factory.class, recordFormat )
                .map( RecordFormats.Factory::newInstance )
                .orElse( null );
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
