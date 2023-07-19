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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.LegacyMetadataHandler;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;

public class RecordStoreVersionCheck implements StoreVersionCheck {
    private final PageCache pageCache;
    private final Path metaDataFile;
    private final Config config;
    private final String databaseName;

    public RecordStoreVersionCheck(PageCache pageCache, RecordDatabaseLayout databaseLayout, Config config) {
        this.pageCache = pageCache;
        this.metaDataFile = databaseLayout.metadataStore();
        this.databaseName = databaseLayout.getDatabaseName();
        this.config = config;
    }

    @Override
    public boolean isCurrentStoreVersionFullySupported(CursorContext cursorContext) {
        StoreVersionIdentifier currentVersion;
        try {
            currentVersion = readVersion(cursorContext);
        } catch (Exception e) {
            return false;
        }

        return isStoreVersionFullySupported(currentVersion, cursorContext);
    }

    @Override
    public boolean isStoreVersionFullySupported(StoreVersionIdentifier storeVersion, CursorContext cursorContext) {
        return RecordFormatSelector.selectForStoreVersionIdentifier(storeVersion)
                .map(format -> !format.onlyForMigration())
                .orElse(false);
    }

    private StoreVersionIdentifier readVersion(CursorContext cursorContext) throws IOException {
        var fieldAccess = MetaDataStore.getFieldAccess(pageCache, metaDataFile, databaseName, cursorContext);
        if (fieldAccess.isLegacyFieldValid()) {
            return fieldAccess.readStoreId();
        }

        return LegacyMetadataHandler.readMetadata44FromStore(pageCache, metaDataFile, databaseName, cursorContext)
                .storeId();
    }

    @Override
    public MigrationCheckResult getAndCheckMigrationTargetVersion(String formatFamily, CursorContext cursorContext) {
        RecordFormats formatToMigrateFrom;
        StoreVersionIdentifier currentVersion;
        try {
            currentVersion = readVersion(cursorContext);
            formatToMigrateFrom = RecordFormatSelector.selectForStoreVersionIdentifier(currentVersion)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Unknown store version '" + currentVersion.getStoreVersionUserString() + "'"));
        } catch (Exception e) {
            return new MigrationCheckResult(MigrationOutcome.STORE_VERSION_RETRIEVAL_FAILURE, null, null, e);
        }

        if (formatFamily == null) {
            formatFamily = formatToMigrateFrom.getFormatFamily().name();
        }

        RecordFormats formatToMigrateTo = RecordFormatSelector.findLatestFormatInFamily(formatFamily, config);

        if (formatToMigrateTo == null) {
            return new MigrationCheckResult(MigrationOutcome.UNSUPPORTED_MIGRATION_PATH, currentVersion, null, null);
        }

        if (formatToMigrateTo.onlyForMigration()) {
            return new MigrationCheckResult(
                    MigrationOutcome.UNSUPPORTED_TARGET_VERSION,
                    currentVersion,
                    versionIdentifier(formatToMigrateTo),
                    null);
        }

        if (formatToMigrateFrom.equals(formatToMigrateTo)) {
            return new MigrationCheckResult(
                    MigrationOutcome.NO_OP, currentVersion, versionIdentifier(formatToMigrateTo), null);
        }

        if (formatToMigrateFrom.getFormatFamily().isHigherThan(formatToMigrateTo.getFormatFamily())) {
            return new MigrationCheckResult(
                    MigrationOutcome.UNSUPPORTED_MIGRATION_PATH,
                    currentVersion,
                    versionIdentifier(formatToMigrateTo),
                    null);
        }

        return new MigrationCheckResult(
                MigrationOutcome.MIGRATION_POSSIBLE, currentVersion, versionIdentifier(formatToMigrateTo), null);
    }

    @Override
    public StoreVersionIdentifier getCurrentVersion(CursorContext cursorContext)
            throws IOException, IllegalArgumentException, IllegalStateException {
        RecordFormats formatToMigrateFrom;
        StoreVersionIdentifier currentVersion;
        currentVersion = readVersion(cursorContext);
        formatToMigrateFrom = RecordFormatSelector.selectForStoreVersionIdentifier(currentVersion)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown store version '" + currentVersion.getStoreVersionUserString() + "'"));
        return versionIdentifier(formatToMigrateFrom);
    }

    @Override
    public UpgradeCheckResult getAndCheckUpgradeTargetVersion(CursorContext cursorContext) {
        RecordFormats formatToUpgradeFrom;
        StoreVersionIdentifier currentVersion;
        try {
            currentVersion = readVersion(cursorContext);
            formatToUpgradeFrom = RecordFormatSelector.selectForStoreVersionIdentifier(currentVersion)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Unknown store version '" + currentVersion.getStoreVersionUserString() + "'"));
        } catch (Exception e) {
            return new UpgradeCheckResult(UpgradeOutcome.STORE_VERSION_RETRIEVAL_FAILURE, null, null, e);
        }

        RecordFormats formatToUpgradeTo = RecordFormatSelector.findLatestMinorVersion(formatToUpgradeFrom, config);

        if (formatToUpgradeTo.onlyForMigration()) {
            return new UpgradeCheckResult(
                    UpgradeOutcome.UNSUPPORTED_TARGET_VERSION,
                    currentVersion,
                    // If we are on one of the pre-5.0 formats this will give us the overridden storeVersionUserString.
                    formatToUpgradeTo.equals(formatToUpgradeFrom)
                            ? currentVersion
                            : versionIdentifier(formatToUpgradeTo),
                    null);
        }

        if (formatToUpgradeFrom.equals(formatToUpgradeTo)) {
            return new UpgradeCheckResult(
                    UpgradeOutcome.NO_OP, currentVersion, versionIdentifier(formatToUpgradeTo), null);
        }

        return new UpgradeCheckResult(
                UpgradeOutcome.UPGRADE_POSSIBLE, currentVersion, versionIdentifier(formatToUpgradeTo), null);
    }

    @Override
    public String getIntroductionVersionFromVersion(StoreVersionIdentifier versionIdentifier) {
        return RecordFormatSelector.selectForStoreVersionIdentifier(versionIdentifier)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown store version '" + versionIdentifier.getStoreVersionUserString() + "'"))
                .introductionVersion();
    }

    @Override
    public StoreVersionIdentifier findLatestVersion(String format) {
        return versionIdentifier(RecordFormatSelector.findLatestFormatInFamily(format, config));
    }

    private StoreVersionIdentifier versionIdentifier(RecordFormats format) {
        return new StoreVersionIdentifier(
                RecordStorageEngineFactory.NAME,
                format.getFormatFamily().name(),
                format.majorVersion(),
                format.minorVersion());
    }
}
