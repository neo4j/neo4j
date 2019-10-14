/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class LuceneSchemaIndexBuilderTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private DefaultFileSystemAbstraction fileSystemRule;

    private final IndexDescriptor descriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, 0 ) ).withName( "a" ).materialise( 0 );

    @Test
    void readOnlyIndexCreation() throws Exception
    {
        try ( SchemaIndex schemaIndex = LuceneSchemaIndexBuilder.create( descriptor, getReadOnlyConfig() )
                .withFileSystem( fileSystemRule )
                .withOperationalMode( true )
                .withIndexRootFolder( testDir.directory( "a" ) )
                .build() )
        {
            assertTrue( schemaIndex.isReadOnly(), "Builder should construct read only index." );
        }
    }

    @Test
    void writableIndexCreation() throws Exception
    {
        try ( SchemaIndex schemaIndex = LuceneSchemaIndexBuilder.create( descriptor, getDefaultConfig() )
                .withFileSystem( fileSystemRule )
                .withOperationalMode( true )
                .withIndexRootFolder( testDir.directory( "b" ) )
                .build() )
        {
            assertFalse( schemaIndex.isReadOnly(), "Builder should construct writable index." );
        }
    }

    private static Config getDefaultConfig()
    {
        return Config.defaults();
    }

    private static Config getReadOnlyConfig()
    {
        return Config.defaults( GraphDatabaseSettings.read_only, true );
    }
}
