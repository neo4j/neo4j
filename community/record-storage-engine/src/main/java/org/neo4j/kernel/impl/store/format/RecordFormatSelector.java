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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparingInt;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.concat;
import static org.neo4j.internal.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
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
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_3;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV5_0;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV5_0;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StoreVersion;
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

    /** Default format here should be kept same as {@link GraphDatabaseSettings#record_format_created_db#defaultFormat()}. */
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
        return DEFAULT_FORMAT;
    }

    private static RecordFormats defaultFormat(boolean includeDevFormats) {
        if (includeDevFormats) {
            return PageAlignedV5_0.RECORD_FORMATS;
        }
        return DEFAULT_FORMAT;
    }

    /**
     * Select record formats for provided store version.
     *
     * @param storeVersion store version to find format for
     * @return record formats
     * @throws IllegalArgumentException if format for specified store version not found
     */
    @Deprecated
    public static RecordFormats selectForVersion(String storeVersion) {
        // Format can be supplied in two ways:
        // - The high-level/user-friendly name of the format (potentially also having a version)
        // - The internal version that gets put into the meta-data store
        // Here we check for both

        // First check for matching name
        RecordFormats recordFormatsMatchingName = loadRecordFormat(storeVersion, true);
        if (recordFormatsMatchingName != null) {
            return recordFormatsMatchingName;
        }

        // The check for matching internal store version
        for (RecordFormats format : allFormats()) {
            if (format.storeVersion().equals(storeVersion)) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown store version '" + storeVersion + "'");
    }

    /**
     * Select record formats for provided store version identifier.
     */
    public static Optional<RecordFormats> selectForStoreVersionIdentifier(
            StoreVersionIdentifier storeVersionIdentifier) {
        return Iterables.stream(allFormats())
                .filter(format -> format.majorVersion() == storeVersionIdentifier.getMajorVersion()
                        && format.minorVersion() == storeVersionIdentifier.getMinorVersion()
                        && format.getFormatFamily().name().equals(storeVersionIdentifier.getFormatFamilyName()))
                .findAny();
    }

    /**
     * Select record format for the given store directory.
     * <p>
     * <b>Note:</b> package private only for testing.
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
                long value = MetaDataStore.getRecord(
                        pageCache, neoStoreFile, STORE_VERSION, databaseLayout.getDatabaseName(), cursorContext);
                if (value != MetaDataRecordFormat.FIELD_NOT_PRESENT) {
                    String storeVersion = StoreVersion.versionLongToString(value);

                    for (RecordFormats format : allFormats()) {
                        if (format.storeVersion().equals(storeVersion)) {
                            info(
                                    logProvider,
                                    "Selected " + format + " record format from store "
                                            + databaseLayout.databaseDirectory());
                            return format;
                        }
                    }
                }
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

        String specificFormat = config.get(GraphDatabaseInternalSettings.select_specific_record_format);
        if (StringUtils.isNotEmpty(specificFormat)) {
            return selectSpecificFormat(
                    specificFormat, config.get(GraphDatabaseInternalSettings.include_versions_under_development));
        }

        RecordFormats formats = loadRecordFormat(
                config.get(GraphDatabaseSettings.record_format_created_db).name(), false);

        boolean includeDevFormats = config.get(GraphDatabaseInternalSettings.include_versions_under_development);
        if (includeDevFormats && formats != null) {
            Optional<RecordFormats> newestFormatInFamily = findLatestFormatInFamily(formats, true);
            formats = newestFormatInFamily.orElse(formats);
        }
        return formats;
    }

    private static boolean formatSameFamilyAndVersion(RecordFormats left, RecordFormats right) {
        return left.getFormatFamily().equals(right.getFormatFamily()) && left.majorVersion() == right.majorVersion();
    }

    /**
     * Check if a format is compatible with another format. In case if format is not configured or store does not
     * exist yet - we consider formats as compatible.
     * @param format a {@link RecordFormats}
     * @param otherFormat another {@link RecordFormats} to compare with.
     * @return true if the two formats are compatible, false otherwise.
     */
    public static boolean isStoreAndConfigFormatsCompatible(RecordFormats format, RecordFormats otherFormat) {
        return (format == null) || (otherFormat == null) || formatSameFamilyAndVersion(format, otherFormat);
    }

    public static Optional<RecordFormats> findLatestSupportedFormatInFamily(RecordFormats result, Config config) {
        var specificFormat = config.get(GraphDatabaseInternalSettings.select_specific_record_format);
        var includeDevFormats = config.get(GraphDatabaseInternalSettings.include_versions_under_development);
        if (StringUtils.isNotEmpty(specificFormat)) {
            return Optional.of(selectSpecificFormat(specificFormat, includeDevFormats));
        }
        return findLatestFormatInFamily(result, includeDevFormats);
    }

    public static Optional<RecordFormats> findLatestSupportedFormatInFamily(RecordFormats result) {
        return findLatestFormatInFamily(result, false);
    }

    private static Optional<RecordFormats> findLatestFormatInFamily(RecordFormats result, boolean includeDevFormats) {
        return Iterables.stream(allFormats())
                .filter(format -> format.getFormatFamily() == result.getFormatFamily()
                        && (includeDevFormats || !format.formatUnderDevelopment()))
                .max(comparingInt(RecordFormats::majorVersion).thenComparingInt(RecordFormats::minorVersion));
    }

    /**
     * Finds which format, if any, succeeded the specified format. Only formats in the same family are considered.
     *
     * @param format to find successor to.
     * @return the format with the lowest generation > format.generation, or None if no such format is known.
     */
    public static Optional<RecordFormats> findSupportedSuccessor(final RecordFormats format) {
        return StreamSupport.stream(RecordFormatSelector.allFormats().spliterator(), false)
                .filter(candidate ->
                        candidate.getFormatFamily() == format.getFormatFamily() && !candidate.formatUnderDevelopment())
                .filter(candidate -> candidate.majorVersion() > format.majorVersion()
                        || (candidate.majorVersion() == format.majorVersion()
                                && candidate.minorVersion() > format.minorVersion()))
                .min(comparingInt(RecordFormats::majorVersion).thenComparingInt(RecordFormats::minorVersion));
    }

    /**
     * Gets all {@link RecordFormats} that the selector is aware of.
     * @return An iterable over all known record formats.
     * NOTE this includes formats that are under development.
     */
    public static Iterable<RecordFormats> allFormats() {
        Iterable<RecordFormats.Factory> loadableFormatFactories = Services.loadAll(RecordFormats.Factory.class);
        Iterable<RecordFormats> loadableFormats = map(RecordFormats.Factory::newInstance, loadableFormatFactories);
        return concat(KNOWN_FORMATS, loadableFormats);
    }

    /**
     * Finds the latest store version (both latest major and minor) for the submitted format family.
     * <p>
     * The returned format is not guaranteed to be supported to run a database on ({@link RecordFormats#onlyForMigration()}).
     * Formats under development can be included in the search depending on the corresponding setting in the supplied config.
     */
    public static RecordFormats findLatestFormatInFamily(String formatFamily, Config config) {
        RecordFormats formats = findLatestFormatInFamily(formatFamily);

        boolean includeDevFormats = config.get(GraphDatabaseInternalSettings.include_versions_under_development);
        if (includeDevFormats && formats != null) {
            Optional<RecordFormats> newestFormatInFamily = findLatestFormatInFamily(formats, true);
            formats = newestFormatInFamily.orElse(formats);
        }
        return formats;
    }

    /**
     * Finds the latest store version (both latest major and minor) for the submitted format family.
     * <p>
     * The returned format is not guaranteed to be supported to run a database on ({@link RecordFormats#onlyForMigration()}).
     * Formats under development are not included in the search.
     */
    public static RecordFormats findLatestFormatInFamily(String formatFamily) {
        if (Arrays.stream(FormatFamily.values())
                .map(Enum::name)
                .noneMatch(familyName -> familyName.equals(formatFamily))) {
            throw new IllegalArgumentException("Unknown format family: '" + formatFamily + "'");
        }

        return loadRecordFormat(formatFamily, false);
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
                .filter(candidate -> candidate.getFormatFamily() == format.getFormatFamily()
                        && candidate.majorVersion() == format.majorVersion()
                        && candidate.minorVersion() > format.minorVersion()
                        && (includeDevFormats || !candidate.formatUnderDevelopment()))
                .max(comparingInt(RecordFormats::minorVersion))
                .orElse(format);
    }

    private static RecordFormats selectSpecificFormat(String recordFormat, boolean includeDevFormats) {
        RecordFormats formats = loadRecordFormat(recordFormat, includeDevFormats);
        if (formats == null) {
            throw new IllegalArgumentException("No record format found with the name '" + recordFormat + "'.");
        }
        if (includeDevFormats) {
            Optional<RecordFormats> newestFormatInFamily = findLatestFormatInFamily(formats, true);
            formats = newestFormatInFamily.orElse(formats);
        }
        return formats;
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
        return Services.load(RecordFormats.Factory.class, recordFormat)
                .map(RecordFormats.Factory::newInstance)
                .filter(recordFormats -> includeDevFormats || !recordFormats.formatUnderDevelopment())
                .orElse(null);
    }

    private static void info(InternalLogProvider logProvider, String message) {
        logProvider.getLog(RecordFormatSelector.class).info(message);
    }
}
