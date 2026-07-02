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
package org.neo4j.io.layout.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.io.layout.CommonDatabaseStores.METADATA;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class RecordDatabaseLayoutTest {
    @Inject
    Neo4jLayout neo4jLayout;

    RecordDatabaseLayout layout;

    @BeforeEach
    void setUp() {
        layout = new RecordStorageEngineFactory()
                .databaseLayout(neo4jLayout, GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
    }

    @Test
    void storeFilesHaveExpectedNames() {
        assertEquals("neostore", layout.metadataStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.counts.db", layout.countStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.labeltokenstore.db",
                layout.labelTokenStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.labeltokenstore.db.names",
                layout.labelTokenNamesStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.nodestore.db", layout.nodeStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.nodestore.db.labels",
                layout.nodeLabelStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db",
                layout.propertyStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.arrays",
                layout.propertyArrayStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index",
                layout.propertyKeyTokenStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index.keys",
                layout.propertyKeyTokenNamesStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.strings",
                layout.propertyStringStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.relationshipgroupstore.db",
                layout.relationshipGroupStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.relationshipstore.db",
                layout.relationshipStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.relationshiptypestore.db",
                layout.relationshipTypeTokenStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.relationshiptypestore.db.names",
                layout.relationshipTypeTokenNamesStore()
                        .baseSegment()
                        .getFileName()
                        .toString());
        assertEquals(
                "neostore.schemastore.db",
                layout.schemaStore().storeBaseFileName().toString());
    }

    @Test
    void idFilesHaveExpectedNames() {
        assertEquals(
                "neostore.labeltokenstore.db.id",
                layout.idLabelTokenStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.labeltokenstore.db.names.id",
                layout.idLabelTokenNamesStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.nodestore.db.id",
                layout.idNodeStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.nodestore.db.labels.id",
                layout.idNodeLabelStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.arrays.id",
                layout.idPropertyArrayStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.id",
                layout.idPropertyStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index.id",
                layout.idPropertyKeyTokenStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index.keys.id",
                layout.idPropertyKeyTokenNamesStore()
                        .baseSegment()
                        .getFileName()
                        .toString());
        assertEquals(
                "neostore.propertystore.db.strings.id",
                layout.idPropertyStringStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.relationshipgroupstore.db.id",
                layout.idRelationshipGroupStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.relationshipstore.db.id",
                layout.idRelationshipStore().storeBaseFileName().toString());
        assertEquals(
                "neostore.relationshiptypestore.db.id",
                layout.idRelationshipTypeTokenStore()
                        .baseSegment()
                        .getFileName()
                        .toString());
        assertEquals(
                "neostore.relationshiptypestore.db.names.id",
                layout.idRelationshipTypeTokenNamesStore()
                        .baseSegment()
                        .getFileName()
                        .toString());
        assertEquals(
                "neostore.schemastore.db.id",
                layout.idSchemaStore().storeBaseFileName().toString());
    }

    @Test
    void allFilesContainsStoreFiles() {
        RecordDatabaseFile nodeStore = RecordDatabaseFile.NODE_STORE;
        List<StoreFile> allNodeStoreFile = layout.allFiles(nodeStore).toList();
        StoreFile nodeStoreStoreFile = layout.file(nodeStore);
        assertThat(allNodeStoreFile).contains(nodeStoreStoreFile);
    }

    @Test
    void allFilesContainsIdFileIfPresent() {
        RecordDatabaseFile nodeStore = RecordDatabaseFile.NODE_STORE;
        List<StoreFile> allNodeStoreFile = layout.allFiles(nodeStore).toList();
        StoreFile nodeStoreIdFile = layout.idFile(nodeStore).orElseThrow();
        assertThat(allNodeStoreFile).contains(nodeStoreIdFile);
    }

    @Test
    void lookupFileByDatabaseFile() {
        RecordDatabaseFile[] databaseFiles = RecordDatabaseFile.values();
        for (RecordDatabaseFile databaseFile : databaseFiles) {
            assertNotNull(layout.file(databaseFile));
        }

        assertEquals(
                "neostore", layout.pathForStore(METADATA).storeBaseFileName().toString());
    }

    @Test
    void lookupIdFileByDatabaseFile() {
        RecordDatabaseFile[] databaseFiles = RecordDatabaseFile.values();
        for (RecordDatabaseFile databaseFile : databaseFiles) {
            Optional<StoreFile> idFile = layout.idFile(databaseFile);
            assertEquals(databaseFile.hasIdFile(), idFile.isPresent());
        }
    }
}
