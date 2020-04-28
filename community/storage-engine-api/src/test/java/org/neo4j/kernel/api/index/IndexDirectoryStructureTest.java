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
package org.neo4j.kernel.api.index;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.internal.schema.IndexProviderDescriptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.fs.FileUtils.path;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.baseSchemaIndexFolder;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

class IndexDirectoryStructureTest
{
    private final IndexProviderDescriptor provider = new IndexProviderDescriptor( "test", "0.5" );
    private final File databaseStoreDir = new File( "db" ).getAbsoluteFile();
    private final File baseIndexDirectory = baseSchemaIndexFolder( databaseStoreDir );
    private final long indexId = 15;

    @Test
    void shouldSeeCorrectDirectoriesForProvider()
    {
        assertCorrectDirectories( directoriesByProvider( databaseStoreDir ).forProvider( provider ),
                path( baseIndexDirectory, provider.getKey() + '-' + provider.getVersion() ),
                path( baseIndexDirectory, provider.getKey() + '-' +  provider.getVersion(), String.valueOf( indexId ) ) );
    }

    @Test
    void shouldSeeCorrectDirectoriesForSubProvider()
    {
        IndexDirectoryStructure parentStructure = directoriesByProvider( databaseStoreDir ).forProvider( provider );
        IndexProviderDescriptor subProvider = new IndexProviderDescriptor( "sub", "0.3" );
        assertCorrectDirectories( directoriesBySubProvider( parentStructure ).forProvider( subProvider ),
                path( baseIndexDirectory, provider.getKey() + '-' + provider.getVersion() ),
                path( baseIndexDirectory, provider.getKey() + '-' + provider.getVersion(),
                        String.valueOf( indexId ), subProvider.getKey() + '-' + subProvider.getVersion() ) );
    }

    @Test
    void shouldHandleWeirdCharactersInProviderKey()
    {
        IndexProviderDescriptor providerWithWeirdName = new IndexProviderDescriptor( "native+lucene", "1.0" );
        assertCorrectDirectories( directoriesByProvider( databaseStoreDir ).forProvider( providerWithWeirdName ),
                path( baseIndexDirectory, "native_lucene-1.0" ),
                path( baseIndexDirectory, "native_lucene-1.0", String.valueOf( indexId ) ) );
    }

    private void assertCorrectDirectories( IndexDirectoryStructure directoryStructure,
            File expectedRootDirectory, File expectedIndexDirectory )
    {
        // when
        File rootDirectory = directoryStructure.rootDirectory();
        File indexDirectory = directoryStructure.directoryForIndex( indexId );

        // then
        assertEquals( expectedRootDirectory, rootDirectory );
        assertEquals( expectedIndexDirectory, indexDirectory );
    }
}
