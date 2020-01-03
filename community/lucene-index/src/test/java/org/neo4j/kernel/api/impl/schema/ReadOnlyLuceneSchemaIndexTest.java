/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class ReadOnlyLuceneSchemaIndexTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private ReadOnlyDatabaseSchemaIndex luceneSchemaIndex;

    @BeforeEach
    void setUp()
    {
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage( DirectoryFactory.PERSISTENT, fileSystem, testDirectory.directory() );
        Config config = Config.defaults();
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
        luceneSchemaIndex = new ReadOnlyDatabaseSchemaIndex( indexStorage, TestIndexDescriptorFactory.forLabel( 0, 0 ), samplingConfig,
                new ReadOnlyIndexPartitionFactory() );
    }

    @AfterEach
    void tearDown() throws IOException
    {
        luceneSchemaIndex.close();
    }

    @Test
    void indexDeletionIndReadOnlyModeIsNotSupported()
    {
        assertThrows( UnsupportedOperationException.class, () -> luceneSchemaIndex.drop() );
    }

    @Test
    void indexCreationInReadOnlyModeIsNotSupported()
    {
        assertThrows( UnsupportedOperationException.class, () -> luceneSchemaIndex.create() );
    }

    @Test
    void readOnlyIndexMarkingIsNotSupported()
    {
        assertThrows( UnsupportedOperationException.class, () -> luceneSchemaIndex.markAsOnline() );
    }

    @Test
    void readOnlyIndexMode()
    {
        assertTrue( luceneSchemaIndex.isReadOnly() );
    }

    @Test
    void writerIsNotAccessibleInReadOnlyMode()
    {
        assertThrows( UnsupportedOperationException.class, () -> luceneSchemaIndex.getIndexWriter() );
    }
}
