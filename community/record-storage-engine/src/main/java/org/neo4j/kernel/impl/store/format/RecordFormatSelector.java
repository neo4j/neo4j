/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparingInt;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.concat;
import static org.neo4j.internal.helpers.collection.Iterables.map;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.LegacyMetadataHandler;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_3;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV5_0;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV5_0;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersionIdentifier;

/**
 * Selects record format that will be used in a database.
 * Supports selection based on the existing store and given configuration.
 * <p>
 * Automatic selection is used by various tools and tests that should pretend being format independent (for
 * example backup).
 */
public class RecordFormatSelector {
    private static final String STORE_SELECTION_TAG = "storeSelection";

    /** Default format here should be kept same as {@link GraphDatabaseSettings#db_format}. */
    private static final RecordFormats DEFAULT_FORMAT = PageAligned.LATEST_RECORD_FORMATS;

    private static final List<RecordFormats> KNOWN_FORMATS = asList(
            StandardV4_3.RECORD_FORMATS,
            StandardV5_0.RECORD_FORMATS,
            PageAlignedV4_3.RECORD_FORMATS,
            PageAlignedV5_0.RECORD_FORMATS);

    private RecordFormatSelector() {
        throw new AssertionError("Not for instantiation!");
    }

    /**
     * Select {@link #DEFAULT_FORMAT} record format.
     *
     * @return default record format.
     */
    public static RecordFormats defaultFormat() {
        return defaultFormat(false);
    }

    private static RecordFormats defaultFormat(boolean includeDevFormats) {
        return includeDevFormats
                ? findLatestFormatInFamily(DEFAULT_FORMAT, true).orElse(DEFAULT_FORMAT)
                : DEFAULT_FORMAT;
    }

    /**
     * Select record formats for provided store version identifier.
     */
    public static Optional<RecordFormats> selectForStoreVersionIdentifier(
            StoreVersionIdentifier storeVersionIdentifier) {
        return Iterables.stream(allFormats())
                .filter(format -> format.majorVersion() == storeVersionIdentifier.getMajorVersion()
                        && format.minorVersion() == storeVersionIdentifier.getMinorVersion()
                        && format.getFormatFamily().name().equals(storeVersionIdentifier.getFormatName()))
                .findAny();
    }

    /**
     * Select record format for the given store directory.
     *
     * @param databaseLayout directory with the store
     * @param fs file system used to access store files
     * @param pageCache page cache to read store files
     * @param contextFactory underlying page cache context factory.
     * @return record format of the given store or <code>null</code> if {@link RecordDatabaseLayout#metadataStore()} file not
     * found or can't be read
     */
    public static RecordFormats selectForStore(
            RecordDatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            PageCache pageCache,
            InternalLogProvider logProvider,
            CursorContextFactory contextFactory) {
        Path neoStoreFile = databaseLayout.metadataStore();
        if (fs.fileExists(neoStoreFile)) {
            try (var cursorContext = contextFactory.create(STORE_SELECTION_TAG)) {
                var filedAccess = MetaDataStore.getFieldAccess(
                        pageCache, neoStoreFile, databaseLayout.getDatabaseName(), cursorContext);
                StoreId storeId;
                if (filedAccess.isLegacyFieldValid()) {
                    storeId = filedAccess.readStoreId();
                } else {
                    storeId = LegacyMetadataHandler.readMetadata44FromStore(
                                    pageCache,
                                    databaseLayout.metadataStore(),
                                    databaseLayout.getDatabaseName(),
                                    cursorContext)
                            .storeId();
                }

                return selectForStoreVersionIdentifier(storeId)
                        .map(format -> {
                            info(
                                    logProvider,
                                    "Selected " + format + " record format from store "
                                            + databaseLayout.databaseDirectory());
                            return format;
                        })
                        .orElse(null);

            } catch (IOException e) {
                info(
                        logProvider,
                        format(
                                "Unable to read format for store %s. %s ",
                                databaseLayout.databaseDirectory(), e.getMessage()));
            }
        }
        return null;
    }

    /**
     * Select record format for the given store (if exists) or from the given configuration.
     * For system db the default is used irrespective of the configuration value.
     *
     * @param config configuration parameters
     * @param databaseLayout database directory structure
     * @param fs file system used to access store files
     * @param pageCache page cache to read store files
     * @param contextFactory underlying page cache operations context factory.
     * @return record format from the store (if it can be read) or configured record format or default format
     */
    public static RecordFormats selectForStoreOrConfigForNewDbs(
            Config config,
            RecordDatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            PageCache pageCache,
            InternalLogProvider logProvider,
            CursorContextFactory contextFactory) {
        RecordFormats currentFormat = selectForStore(databaseLayout, fs, pageCache, logProvider, contextFactory);
        if (currentFormat != null) {
            info(logProvider, format("Selected format from the store files: %s", currentFormat));
            return currentFormat;
        }

        RecordFormats configuredFormat = getConfiguredRecordFormatNewDb(config, databaseLayout);
        if (configuredFormat != null) {
            info(
                    logProvider,
                    format(
                            "Selected configured format for store %s: %s",
                            databaseLayout.databaseDirectory(), configuredFormat));
            return configuredFormat;
        }

        return defaultFormat(config.get(GraphDatabaseInternalSettings.include_versions_under_development));
    }

    private static RecordFormats getConfiguredRecordFormatNewDb(Config config, DatabaseLayout databaseLayout) {
        if (SYSTEM_DATABASE_NAME.equals(databaseLayout.getDatabaseName())) {
            // System database record format is not configurable by users.
            return null;
        }

        boolean includeDevFormats = config.get(GraphDatabaseInternalSettings.include_versions_under_development);
        RecordFormats formats = loadRecordFormat(config.get(GraphDatabaseSettings.db_format), includeDevFormats);

        if (includeDevFormats && formats != null) {
            Optional<RecordFormats> newestFormatInFamily = findLatestFormatInFamily(formats, true);
            formats = newestFormatInFamily.orElse(formats);
        }
        return formats;
    }

    private static Optional<RecordFormats> findLatestFormatInFamily(RecordFormats result, boolean includeDevFormats) {
        return Iterables.stream(allFormats())
                .filter(format -> format.getFormatFamily().equals(result.getFormatFamily())
                        && (includeDevFormats || !format.formatUnderDevelopment()))
                .max(comparingInt(RecordFormats::majorVersion).thenComparingInt(RecordFormats::minorVersion));
    }

    /**
     * Gets all {@link RecordFormats} that the selector is aware of.
     * @return An iterable over all known record formats.
     * NOTE this includes formats that are under development.
     */
    public static Iterable<RecordFormats> allFormats() {
        return FormatLoader.AVAILABLE_FORMATS;
    }

    /**
     * Finds the latest store version (both latest major and minor) for the submitted format family.
     * <p>
     * The returned format is not guaranteed to be supported to run a database on ({@link RecordFormats#onlyForMigration()}).
     * Formats under development can be included in the search depending on the corresponding setting in the supplied config.
     */
    public static RecordFormats findLatestFormatInFamily(String formatFamily, Config config) {
        boolean includeDevFormats = config.get(GraphDatabaseInternalSettings.include_versions_under_development);
        RecordFormats formats = loadRecordFormat(formatFamily, includeDevFormats);

        if (includeDevFormats && formats != null) {
            Optional<RecordFormats> newestFormatInFamily = findLatestFormatInFamily(formats, true);
            formats = newestFormatInFamily.orElse(formats);
        }
        return formats;
    }

    /**
     * Finds the latest minor version ({@link RecordFormats#minorVersion()}) for the combination of the format family ({@link RecordFormats#getFormatFamily()})
     * and the major version ({@link RecordFormats#majorVersion()}) of the supplied format.
     * <p>
     * The returned format is not guaranteed to be supported to run a database on ({@link RecordFormats#onlyForMigration()}).
     * Formats under development can be included in the search depending on the corresponding setting in the supplied config.
     */
    public static RecordFormats findLatestMinorVersion(RecordFormats format, Config config) {
        var includeDevFormats = config.get(GraphDatabaseInternalSettings.include_versions_under_development);
        return Iterables.stream(allFormats())
                .filter(candidate -> candidate.getFormatFamily().equals(format.getFormatFamily())
                        && candidate.majorVersion() == format.majorVersion()
                        && candidate.minorVersion() > format.minorVersion()
                        && (includeDevFormats || !candidate.formatUnderDevelopment()))
                .max(comparingInt(RecordFormats::minorVersion))
                .orElse(format);
    }

    private static RecordFormats loadRecordFormat(String recordFormat, boolean includeDevFormats) {
        if (StringUtils.isEmpty(recordFormat)) {
            return null;
        }
        if (Standard.LATEST_NAME.equals(recordFormat)) {
            return Standard.LATEST_RECORD_FORMATS;
        }
        for (RecordFormats knownFormat : KNOWN_FORMATS) {
            if (includeDevFormats || !knownFormat.formatUnderDevelopment()) {
                if (recordFormat.equals(knownFormat.name())) {
                    return knownFormat;
                }
            }
        }
        return Iterables.stream(allFormats())
                .filter(f -> recordFormat.equals(f.name()))
                .filter(recordFormats -> includeDevFormats || !recordFormats.formatUnderDevelopment())
                .findFirst()
                .orElse(null);
    }

    private static void info(InternalLogProvider logProvider, String message) {
        logProvider.getLog(RecordFormatSelector.class).info(message);
    }

    private static final class FormatLoader {
        private static final Iterable<RecordFormats> AVAILABLE_FORMATS = loadFormats();

        private static Iterable<RecordFormats> loadFormats() {
            Iterable<RecordFormats.Factory> loadableFormatFactories = Services.loadAll(RecordFormats.Factory.class);
            Iterable<RecordFormats> loadableFormats = map(RecordFormats.Factory::getInstance, loadableFormatFactories);
            return concat(KNOWN_FORMATS, loadableFormats);
        }
    }
}
