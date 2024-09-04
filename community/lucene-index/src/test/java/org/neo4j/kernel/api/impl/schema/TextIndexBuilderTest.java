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
package org.neo4j.kernel.api.impl.schema;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class TextIndexBuilderTest {
    @Inject
    private TestDirectory testDir;

    @Inject
    private DefaultFileSystemAbstraction fileSystemRule;

    private final IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 0))
            .withName("a")
            .withIndexType(IndexType.TEXT)
            .withIndexProvider(AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR)
            .materialise(0);

    @Test
    void readOnlyIndexCreation() throws Exception {
        try (var index = TextIndexBuilder.create(descriptor, readOnly(), getDefaultConfig())
                .withFileSystem(fileSystemRule)
                .withIndexRootFolder(testDir.directory("a"))
                .build()) {
            assertTrue(index.isReadOnly(), "Builder should construct read only index.");
        }
    }

    @Test
    void writableIndexCreation() throws Exception {
        try (var index = TextIndexBuilder.create(descriptor, writable(), getDefaultConfig())
                .withFileSystem(fileSystemRule)
                .withIndexRootFolder(testDir.directory("b"))
                .build()) {
            assertFalse(index.isReadOnly(), "Builder should construct writable index.");
        }
    }

    private static Config getDefaultConfig() {
        return Config.defaults();
    }
}
