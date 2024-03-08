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
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.MigrationStoreVersionCheck;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreFormatLimits;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;

public class AcrossEngineVersionCheck implements MigrationStoreVersionCheck {
    private final StoreVersionCheck srcVersionCheck;
    private final StoreVersionCheck targetVersionCheck;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final DatabaseLayout databaseLayout;
    private final Config config;
    private final LogService logService;
    private final StorageEngineFactory srcStorageEngineFactory;
    private final StorageEngineFactory targetStorageEngineFactory;

    public AcrossEngineVersionCheck(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            Config config,
            LogService logService,
            CursorContextFactory contextFactory,
            StorageEngineFactory srcStorageEngineFactory,
            StorageEngineFactory targetStorageEngineFactory) {
        this.fs = fs;
        this.pageCache = pageCache;
        this.databaseLayout = databaseLayout;
        this.config = config;
        this.logService = logService;
        this.srcStorageEngineFactory = srcStorageEngineFactory;
        this.targetStorageEngineFactory = targetStorageEngineFactory;
        srcVersionCheck =
                srcStorageEngineFactory.versionCheck(fs, databaseLayout, config, pageCache, logService, contextFactory);
        targetVersionCheck = targetStorageEngineFactory.versionCheck(
                fs, databaseLayout, config, pageCache, logService, contextFactory);
    }

    @Override
    public MigrationCheckResult getAndCheckMigrationTargetVersion(
            String formatToMigrateTo, CursorContext cursorContext) {
        StoreVersionIdentifier currentVersion;
        try {
            currentVersion = srcVersionCheck.getCurrentVersion(cursorContext);
        } catch (Exception e) {
            return new MigrationCheckResult(MigrationOutcome.STORE_VERSION_RETRIEVAL_FAILURE, null, null, e);
        }

        StoreVersionIdentifier targetVersion;
        try {
            targetVersion = targetVersionCheck.findLatestVersion(formatToMigrateTo);
        } catch (Exception e) {
            return new MigrationCheckResult(MigrationOutcome.UNSUPPORTED_TARGET_VERSION, currentVersion, null, e);
        }

        if (!targetVersionCheck.isStoreVersionFullySupported(targetVersion, cursorContext)) {
            return new MigrationCheckResult(
                    MigrationOutcome.UNSUPPORTED_TARGET_VERSION, currentVersion, targetVersion, null);
        }

        Boolean includeFormatsUnderDevelopment =
                config.get(GraphDatabaseInternalSettings.include_versions_under_development);
        StoreFormatLimits srcFormatLimits =
                srcStorageEngineFactory.limitsForFormat(currentVersion.getFormatName(), includeFormatsUnderDevelopment);

        StoreFormatLimits targetFormatLimits =
                targetStorageEngineFactory.limitsForFormat(formatToMigrateTo, includeFormatsUnderDevelopment);

        if (goingToLowerLimits(srcFormatLimits, targetFormatLimits)) {
            try {
                if (!srcStorageEngineFactory.fitsWithinStoreFormatLimits(
                        targetFormatLimits, databaseLayout, fs, pageCache, config)) {
                    return new MigrationCheckResult(
                            MigrationOutcome.UNSUPPORTED_MIGRATION_LIMITS,
                            currentVersion,
                            targetVersion,
                            new IllegalStateException(
                                    "Migrating to a format with lower entity limits and the store has more entities than "
                                            + "will fit within the limits. "
                                            + "Switch to a different target format, or use "
                                            + "neo4j-admin database copy to copy only relevant parts."));
                } else {
                    // We can not check everything, there are more cases we don't check, for example relationship
                    // groups where there is no corresponding concept in block format.
                    logService
                            .getUserLog(this.getClass())
                            .info("Migrating to a format with lower entity limits. The store should "
                                    + "fit into the target format, trying migration.");
                }
            } catch (IOException | RuntimeException e) {
                return new MigrationCheckResult(
                        MigrationOutcome.UNSUPPORTED_MIGRATION_LIMITS, currentVersion, targetVersion, e);
            }
        }

        return new MigrationCheckResult(MigrationOutcome.MIGRATION_POSSIBLE, currentVersion, targetVersion, null);
    }

    private boolean goingToLowerLimits(StoreFormatLimits fromLimits, StoreFormatLimits toLimits) {
        return fromLimits.maxLabelId() > toLimits.maxLabelId()
                || fromLimits.maxRelationshipTypeId() > toLimits.maxRelationshipTypeId()
                || fromLimits.maxPropertyKeyId() > toLimits.maxPropertyKeyId()
                || fromLimits.maxNodeId() > toLimits.maxNodeId()
                || fromLimits.maxRelationshipId() > toLimits.maxRelationshipId();
    }
}
