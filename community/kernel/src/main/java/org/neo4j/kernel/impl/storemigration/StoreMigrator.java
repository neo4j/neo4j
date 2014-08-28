/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.impl.standard.StandardPageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.*;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyLogFilenames.*;

/**
 * Migrates a neo4j kernel database from one version to the next.
 * <p/>
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 * <p/>
 * Just one out of many potential participants in a {@link StoreUpgrader migration}.
 *
 * @see StoreUpgrader
 */
public class StoreMigrator extends StoreMigrationParticipant.Adapter {
    private static final Object[] NO_PROPERTIES = new Object[0];

    // Developers: There is a benchmark, storemigrate-benchmark, that generates large stores and benchmarks
    // the upgrade process. Please utilize that when writing upgrade code to ensure the code is fast enough to
    // complete upgrades in a reasonable time period.

    private final MigrationProgressMonitor progressMonitor;
    private final UpgradableDatabase upgradableDatabase;
    private final Config config;
    private final Logging logging;
    private String versionToUpgradeFrom;

    // TODO progress meter should be an aspect of StoreUpgrader, not specific to this participant.

    public StoreMigrator(MigrationProgressMonitor progressMonitor, FileSystemAbstraction fileSystem) {
        this(progressMonitor, new UpgradableDatabase(new StoreVersionCheck(fileSystem)),
                new Config(), new SystemOutLogging());
    }

    public StoreMigrator(MigrationProgressMonitor progressMonitor, UpgradableDatabase upgradableDatabase,
                         Config config, Logging logging) {
        this.progressMonitor = progressMonitor;
        this.upgradableDatabase = upgradableDatabase;
        this.config = config;
        this.logging = logging;
    }

    @Override
    public boolean needsMigration(FileSystemAbstraction fileSystem, File storeDir) throws IOException {
        NeoStoreUtil neoStoreUtil = new NeoStoreUtil(storeDir, fileSystem);
        String versionAsString = NeoStore.versionLongToString(neoStoreUtil.getStoreVersion());
        boolean sameVersion = CommonAbstractStore.ALL_STORES_VERSION.equals(versionAsString);
        if (!sameVersion) {
            upgradableDatabase.checkUpgradeable(storeDir);
        }
        return !sameVersion;
    }

    @Override
    public void migrate(FileSystemAbstraction fileSystem, File storeDir, File migrationDir,
                        DependencyResolver dependencyResolver) throws IOException {
        versionToUpgradeFrom = upgradableDatabase.checkUpgradeable(storeDir);

        progressMonitor.started();

        if (versionToUpgradeFrom.equals(Legacy21Store.LEGACY_VERSION)) {   // Don't migrate any store here
        } else {
            migrateWithBatchImporter(fileSystem, storeDir, migrationDir, dependencyResolver);
        }
        progressMonitor.finished();
    }

    private void migrateWithBatchImporter(FileSystemAbstraction fileSystem, File storeDir, File migrationDir,
                                          DependencyResolver dependencyResolver)
            throws IOException {
        LegacyStore legacyStore;
        switch (versionToUpgradeFrom) {
            case Legacy19Store.LEGACY_VERSION:
                legacyStore = new Legacy19Store(fileSystem, new File(storeDir, NeoStore.DEFAULT_NAME));
                break;
            case Legacy20Store.LEGACY_VERSION:
                legacyStore = new Legacy20Store(fileSystem, new File(storeDir, NeoStore.DEFAULT_NAME));
                break;
            default:
                throw new IllegalStateException("Unknown version to upgrade from: " + versionToUpgradeFrom);
        }

        ExecutionMonitor executionMonitor = new CoarseBoundedProgressExecutionMonitor(
                legacyStore.getNodeStoreReader().getMaxId(), legacyStore.getRelStoreReader().getMaxId()) {
            @Override
            protected void percent(int percent) {
                progressMonitor.percentComplete(percent);
            }
        };
        BatchImporter importer = new ParallelBatchImporter(migrationDir.getAbsolutePath(), fileSystem,
                new Configuration.OverrideFromConfig(config), logging, executionMonitor);
        Iterable<InputNode> nodes = legacyNodesAsInput(legacyStore);
        Iterable<InputRelationship> relationships = legacyRelationshipsAsInput(legacyStore);
        IdMapper idMapper = IdMappers.actualIds();
        importer.doImport(nodes, relationships, idMapper);

        // Finish the import of nodes and relationships
        importer.shutdown();
        if (legacyStore instanceof Legacy19Store) {
            // we may need to upgrade the property keys
            Legacy19Store legacy19Store = (Legacy19Store) legacyStore;

            // TODO 2.2-future: fixme if we set the number of page to 1000 the store migration from 19 when
            // we deduplicate property will loop forever in the page cache trying to free pages.
            StandardPageCache pageCache = new StandardPageCache(fileSystem, 10_000, 8192);
            PropertyStore propertyStore = storeFactory(fileSystem, pageCache, migrationDir, dependencyResolver)
                    .newPropertyStore();
            try {
                migratePropertyKeys(legacy19Store, propertyStore);
            } finally {
                propertyStore.close();
                pageCache.close();
            }
        }
        // Close
        legacyStore.close();
    }

    private StoreFactory storeFactory(FileSystemAbstraction fileSystem, StandardPageCache pageCache,
                                      File migrationDir, DependencyResolver resolver) {
        return new StoreFactory(
                StoreFactory.configForStoreDir(config, migrationDir),
                new DefaultIdGeneratorFactory(), pageCache,
                fileSystem, StringLogger.DEV_NULL, new Monitors());
    }

    private void migratePropertyKeys(Legacy19Store legacyStore, PropertyStore propertyStore) throws IOException {
        Token[] tokens = legacyStore.getPropertyIndexReader().readTokens();

        // dedup and write new property key token store (incl. names)
        Map<Integer, Integer> propertyKeyTranslation = dedupAndWritePropertyKeyTokenStore(propertyStore, tokens);

        // read property store, replace property key ids
        migratePropertyStore(legacyStore, propertyKeyTranslation, propertyStore);
    }

    private Map<Integer, Integer> dedupAndWritePropertyKeyTokenStore(
            PropertyStore propertyStore, Token[] tokens /*ordered ASC*/) {
        PropertyKeyTokenStore keyTokenStore = propertyStore.getPropertyKeyTokenStore();
        Map<Integer/*duplicate*/, Integer/*use this instead*/> translations = new HashMap<>();
        Map<String, Integer> createdTokens = new HashMap<>();
        for (Token token : tokens) {
            Integer id = createdTokens.get(token.name());
            if (id == null) {   // Not a duplicate, add to store
                id = (int) keyTokenStore.nextId();
                PropertyKeyTokenRecord record = new PropertyKeyTokenRecord(id);
                Collection<DynamicRecord> nameRecords =
                        keyTokenStore.allocateNameRecords(encode(token.name()));
                record.setNameId((int) first(nameRecords).getId());
                record.addNameRecords(nameRecords);
                record.setInUse(true);
                record.setCreated();
                keyTokenStore.updateRecord(record);
                createdTokens.put(token.name(), id);
            }
            translations.put(token.id(), id);
        }
        return translations;
    }

    private void migratePropertyStore(Legacy19Store legacyStore, Map<Integer, Integer> propertyKeyTranslation,
                                      PropertyStore propertyStore) throws IOException {
        long lastInUseId = -1;
        for (PropertyRecord propertyRecord : loop(legacyStore.getPropertyStoreReader().readPropertyStore())) {
            // Translate property keys
            for (PropertyBlock block : propertyRecord.getPropertyBlocks()) {
                int key = block.getKeyIndexId();
                Integer translation = propertyKeyTranslation.get(key);
                if (translation != null) {
                    block.setKeyIndexId(translation);
                }
            }
            propertyStore.setHighId(propertyRecord.getId() + 1);
            propertyStore.updateRecord(propertyRecord);
            for (long id = lastInUseId + 1; id < propertyRecord.getId(); id++) {
                propertyStore.freeId(id);
            }
            lastInUseId = propertyRecord.getId();
        }
    }

    private StoreFile[] allExcept(StoreFile... exceptions) {
        List<StoreFile> result = new ArrayList<>();
        result.addAll(Arrays.asList(StoreFile.values()));
        for (StoreFile except : exceptions) {
            result.remove(except);
        }
        return result.toArray(new StoreFile[result.size()]);
    }

    private Iterable<InputRelationship> legacyRelationshipsAsInput(LegacyStore legacyStore) {
        final org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader reader = legacyStore.getRelStoreReader();
        return new Iterable<InputRelationship>() {
            @Override
            public Iterator<InputRelationship> iterator() {
                Iterator<RelationshipRecord> source;
                try {
                    source = reader.iterator(0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                return new IteratorWrapper<InputRelationship, RelationshipRecord>(source) {
                    @Override
                    protected InputRelationship underlyingObjectToObject(RelationshipRecord record) {
                        return new InputRelationship(record.getId(), NO_PROPERTIES, record.getNextProp(),
                                record.getFirstNode(), record.getSecondNode(), null, record.getType());
                    }
                };
            }
        };
    }

    private Iterable<InputNode> legacyNodesAsInput(LegacyStore legacyStore) {
        final LegacyNodeStoreReader reader = legacyStore.getNodeStoreReader();
        final String[] NO_LABELS = new String[0];
        return new Iterable<InputNode>() {
            @Override
            public Iterator<InputNode> iterator() {
                Iterator<NodeRecord> source;
                try {
                    source = reader.iterator();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                return new IteratorWrapper<InputNode, NodeRecord>(source) {
                    @Override
                    protected InputNode underlyingObjectToObject(NodeRecord record) {
                        return new InputNode(record.getId(), NO_PROPERTIES, record.getNextProp(),
                                NO_LABELS, record.getLabelField());
                    }
                };
            }
        };
    }

    @Override
    public void moveMigratedFiles(FileSystemAbstraction fileSystem, File migrationDir,
                                  File storeDir, File leftOversDir) throws IOException {
        // The batch importer will create a whole store. so
        // Disregard the new and empty node/relationship".id" files, i.e. reuse the existing id files

        StoreFile[] filesToDelete;
        StoreFile[] leftoverFiles;
        StoreFile[] idFilesToDelete;
        switch (versionToUpgradeFrom) {
            case Legacy19Store.LEGACY_VERSION:
                filesToDelete = allExcept(
                        StoreFile.NODE_STORE,
                        StoreFile.RELATIONSHIP_STORE,
                        StoreFile.RELATIONSHIP_GROUP_STORE,
                        StoreFile.LABEL_TOKEN_STORE,
                        StoreFile.NODE_LABEL_STORE,
                        StoreFile.PROPERTY_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        StoreFile.LABEL_TOKEN_NAMES_STORE,
                        StoreFile.SCHEMA_STORE
                );
                idFilesToDelete = allExcept(
                        StoreFile.RELATIONSHIP_GROUP_STORE
                );
                leftoverFiles = new StoreFile[]{
                        StoreFile.NODE_STORE,
                        StoreFile.RELATIONSHIP_STORE,
                        StoreFile.PROPERTY_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE
                };
                break;
            case Legacy20Store.LEGACY_VERSION:
                // Note: We don't overwrite the label stores in 2.0
                filesToDelete = allExcept(
                        StoreFile.NODE_STORE,
                        StoreFile.RELATIONSHIP_STORE,
                        StoreFile.RELATIONSHIP_GROUP_STORE);
                idFilesToDelete = allExcept(
                        StoreFile.RELATIONSHIP_GROUP_STORE
                );
                leftoverFiles = new StoreFile[]{
                        StoreFile.NODE_STORE,
                        StoreFile.RELATIONSHIP_STORE
                };
                break;
            case Legacy21Store.LEGACY_VERSION:
                filesToDelete = idFilesToDelete = leftoverFiles = new StoreFile[]{};
                break;
            default:
                throw new IllegalStateException("Unknown version to upgrade from: " + versionToUpgradeFrom);
        }

        StoreFile.deleteStoreFile(fileSystem, migrationDir, filesToDelete);
        StoreFile.deleteIdFile(fileSystem, migrationDir, idFilesToDelete);

        // Move the current ones into the leftovers directory
        StoreFile.move(fileSystem, storeDir, leftOversDir,
                IteratorUtil.asIterable(leftoverFiles), // files
                false,  // does not allow to skip non existent source files
                false,  // does not allow to overwrite target files
                StoreFileType.STORE);

        // Move the migrated ones into the store directory
        StoreFile.move(fileSystem, migrationDir, storeDir,
                StoreFile.currentStoreFiles(), // files
                true,   // allow to skip non existent source files
                true,   // allow to overwrite target files
                StoreFileType.values());

        // ensure the store version is correct
        StoreFile.ensureStoreVersion(fileSystem, storeDir, StoreFile.currentStoreFiles());

        renameLogFiles(fileSystem, storeDir);
    }

    private void renameLogFiles(FileSystemAbstraction fileSystem, File storeDir) throws IOException {
        // rename files
        for (File file : fileSystem.listFiles(storeDir, versionedLegacyLogFilesFilter)) {
            final String oldName = file.getName();
            final long version = getLegacyLogVersion(oldName);
            final String newName = PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + version;
            fileSystem.renameFile(file, new File(file.getParent(), newName));
        }

        // delete old an unused log files
        for (File file : fileSystem.listFiles(storeDir, allLegacyLogFilesFilter)) {
            fileSystem.deleteFile(file);
        }
    }

    @Override
    public void cleanup(FileSystemAbstraction fileSystem, File migrationDir) throws IOException {
        for (StoreFile storeFile : StoreFile.values()) {
            fileSystem.deleteFile(new File(migrationDir, storeFile.storeFileName()));
            fileSystem.deleteFile(new File(migrationDir, storeFile.idFileName()));
        }
    }

    @Override
    public String toString() {
        return "Kernel StoreMigrator";
    }
}
