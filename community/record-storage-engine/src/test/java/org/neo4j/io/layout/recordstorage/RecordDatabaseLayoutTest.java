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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.Neo4jLayout;
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
        assertEquals("neostore", layout.metadataStore().getFileName().toString());
        assertEquals("neostore.counts.db", layout.countStore().getFileName().toString());
        assertEquals(
                "neostore.labeltokenstore.db",
                layout.labelTokenStore().getFileName().toString());
        assertEquals(
                "neostore.labeltokenstore.db.names",
                layout.labelTokenNamesStore().getFileName().toString());
        assertEquals("neostore.nodestore.db", layout.nodeStore().getFileName().toString());
        assertEquals(
                "neostore.nodestore.db.labels",
                layout.nodeLabelStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db",
                layout.propertyStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.arrays",
                layout.propertyArrayStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index",
                layout.propertyKeyTokenStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index.keys",
                layout.propertyKeyTokenNamesStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.strings",
                layout.propertyStringStore().getFileName().toString());
        assertEquals(
                "neostore.relationshipgroupstore.db",
                layout.relationshipGroupStore().getFileName().toString());
        assertEquals(
                "neostore.relationshipstore.db",
                layout.relationshipStore().getFileName().toString());
        assertEquals(
                "neostore.relationshiptypestore.db",
                layout.relationshipTypeTokenStore().getFileName().toString());
        assertEquals(
                "neostore.relationshiptypestore.db.names",
                layout.relationshipTypeTokenNamesStore().getFileName().toString());
        assertEquals(
                "neostore.schemastore.db", layout.schemaStore().getFileName().toString());
    }

    @Test
    void idFilesHaveExpectedNames() {
        assertEquals(
                "neostore.labeltokenstore.db.id",
                layout.idLabelTokenStore().getFileName().toString());
        assertEquals(
                "neostore.labeltokenstore.db.names.id",
                layout.idLabelTokenNamesStore().getFileName().toString());
        assertEquals(
                "neostore.nodestore.db.id", layout.idNodeStore().getFileName().toString());
        assertEquals(
                "neostore.nodestore.db.labels.id",
                layout.idNodeLabelStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.arrays.id",
                layout.idPropertyArrayStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.id",
                layout.idPropertyStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index.id",
                layout.idPropertyKeyTokenStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.index.keys.id",
                layout.idPropertyKeyTokenNamesStore().getFileName().toString());
        assertEquals(
                "neostore.propertystore.db.strings.id",
                layout.idPropertyStringStore().getFileName().toString());
        assertEquals(
                "neostore.relationshipgroupstore.db.id",
                layout.idRelationshipGroupStore().getFileName().toString());
        assertEquals(
                "neostore.relationshipstore.db.id",
                layout.idRelationshipStore().getFileName().toString());
        assertEquals(
                "neostore.relationshiptypestore.db.id",
                layout.idRelationshipTypeTokenStore().getFileName().toString());
        assertEquals(
                "neostore.relationshiptypestore.db.names.id",
                layout.idRelationshipTypeTokenNamesStore().getFileName().toString());
        assertEquals(
                "neostore.schemastore.db.id",
                layout.idSchemaStore().getFileName().toString());
    }

    @Test
    void allStoreFiles() {
        Set<String> files = layout.storeFiles().stream()
                .map(Path::getFileName)
                .map(Path::toString)
                .collect(toSet());
        assertThat(files)
                .containsExactlyInAnyOrder(
                        "neostore",
                        "neostore.counts.db",
                        "neostore.labeltokenstore.db",
                        "neostore.labeltokenstore.db.names",
                        "neostore.nodestore.db",
                        "neostore.nodestore.db.labels",
                        "neostore.propertystore.db",
                        "neostore.propertystore.db.arrays",
                        "neostore.propertystore.db.index",
                        "neostore.propertystore.db.index.keys",
                        "neostore.propertystore.db.strings",
                        "neostore.relationshipgroupstore.db",
                        "neostore.relationshipgroupstore.degrees.db",
                        "neostore.relationshipstore.db",
                        "neostore.relationshiptypestore.db",
                        "neostore.relationshiptypestore.db.names",
                        "neostore.schemastore.db",
                        "neostore.indexstats.db");
    }

    @Test
    void allMandatoryStoreFiles() {
        Set<String> files = layout.mandatoryStoreFiles().stream()
                .map(Path::getFileName)
                .map(Path::toString)
                .collect(toSet());
        assertThat(files)
                .containsExactlyInAnyOrder(
                        "neostore",
                        "neostore.labeltokenstore.db",
                        "neostore.labeltokenstore.db.names",
                        "neostore.nodestore.db",
                        "neostore.nodestore.db.labels",
                        "neostore.propertystore.db",
                        "neostore.propertystore.db.arrays",
                        "neostore.propertystore.db.index",
                        "neostore.propertystore.db.index.keys",
                        "neostore.propertystore.db.strings",
                        "neostore.relationshipgroupstore.db",
                        "neostore.relationshipstore.db",
                        "neostore.relationshiptypestore.db",
                        "neostore.relationshiptypestore.db.names",
                        "neostore.schemastore.db");
    }

    @Test
    void allFilesContainsStoreFiles() {
        RecordDatabaseFile nodeStore = RecordDatabaseFile.NODE_STORE;
        List<Path> allNodeStoreFile = layout.allFiles(nodeStore).collect(toList());
        Path nodeStoreStoreFile = layout.file(nodeStore);
        assertThat(allNodeStoreFile).contains(nodeStoreStoreFile);
    }

    @Test
    void allFilesContainsIdFileIfPresent() {
        RecordDatabaseFile nodeStore = RecordDatabaseFile.NODE_STORE;
        List<Path> allNodeStoreFile = layout.allFiles(nodeStore).collect(toList());
        Path nodeStoreIdFile = layout.idFile(nodeStore).orElseThrow();
        assertThat(allNodeStoreFile).contains(nodeStoreIdFile);
    }

    @Test
    void lookupFileByDatabaseFile() {
        RecordDatabaseFile[] databaseFiles = RecordDatabaseFile.values();
        for (RecordDatabaseFile databaseFile : databaseFiles) {
            assertNotNull(layout.file(databaseFile));
        }

        Path metadata = layout.pathForStore(CommonDatabaseStores.METADATA);
        assertEquals("neostore", metadata.getFileName().toString());
    }

    @Test
    void lookupIdFileByDatabaseFile() {
        RecordDatabaseFile[] databaseFiles = RecordDatabaseFile.values();
        for (RecordDatabaseFile databaseFile : databaseFiles) {
            Optional<Path> idFile = layout.idFile(databaseFile);
            assertEquals(databaseFile.hasIdFile(), idFile.isPresent());
        }
    }
}
