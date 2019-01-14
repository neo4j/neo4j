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
package org.neo4j.io.layout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith( TestDirectoryExtension.class )
class DatabaseLayoutTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void databaseLayoutForAbsoluteFile()
    {
        File databaseDir = testDirectory.databaseDir();
        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDir );
        assertEquals( databaseLayout.databaseDirectory(), databaseDir );
    }

    @Test
    void databaseLayoutResolvesLinks() throws IOException
    {
        Path basePath = testDirectory.directory().toPath();
        File databaseDir = testDirectory.databaseDir("notAbsolute");
        Path linkPath = basePath.resolve( "link" );
        Path symbolicLink = Files.createSymbolicLink( linkPath, databaseDir.toPath() );
        DatabaseLayout databaseLayout = DatabaseLayout.of( symbolicLink.toFile() );
        assertEquals( databaseLayout.databaseDirectory(), databaseDir );
    }

    @Test
    void databaseLayoutUseCanonicalRepresentation()
    {
        File storeDir = testDirectory.storeDir( "notCanonical" );
        Path basePath = testDirectory.databaseDir( storeDir ).toPath();
        Path notCanonicalPath = basePath.resolve( "../anotherDatabase" );
        DatabaseLayout databaseLayout = DatabaseLayout.of( notCanonicalPath.toFile() );
        File expectedDirectory = StoreLayout.of( storeDir ).databaseLayout( "anotherDatabase" ).databaseDirectory();
        assertEquals( expectedDirectory, databaseLayout.databaseDirectory() );
    }

    @Test
    void databaseLayoutForName()
    {
        String databaseName = "testDatabase";
        StoreLayout storeLayout = testDirectory.storeLayout();
        DatabaseLayout testDatabase = DatabaseLayout.of( storeLayout, databaseName );
        assertEquals( new File( storeLayout.storeDirectory(), databaseName ), testDatabase.databaseDirectory() );
    }

    @Test
    void databaseLayoutForFolderAndName()
    {
        String database = "database";
        DatabaseLayout databaseLayout = DatabaseLayout.of( testDirectory.storeDir(), database );
        assertEquals( testDirectory.databaseLayout( database ).databaseDirectory(), databaseLayout.databaseDirectory() );
    }

    @Test
    void databaseLayoutProvideCorrectDatabaseName()
    {
        assertEquals( "graph.db", testDirectory.databaseLayout().getDatabaseName() );
        assertEquals( "testDb", testDirectory.databaseLayout( "testDb" ).getDatabaseName() );
    }

    @Test
    void storeFilesHaveExpectedNames()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        assertEquals( "neostore", layout.metadataStore().getName() );
        assertEquals( "neostore.counts.db.a", layout.countStoreA().getName() );
        assertEquals( "neostore.counts.db.b", layout.countStoreB().getName() );
        assertEquals( "neostore.labelscanstore.db", layout.labelScanStore().getName() );
        assertEquals( "neostore.labeltokenstore.db", layout.labelTokenStore().getName() );
        assertEquals( "neostore.labeltokenstore.db.names", layout.labelTokenNamesStore().getName() );
        assertEquals( "neostore.nodestore.db", layout.nodeStore().getName() );
        assertEquals( "neostore.nodestore.db.labels", layout.nodeLabelStore().getName() );
        assertEquals( "neostore.propertystore.db", layout.propertyStore().getName() );
        assertEquals( "neostore.propertystore.db.arrays", layout.propertyArrayStore().getName() );
        assertEquals( "neostore.propertystore.db.index", layout.propertyKeyTokenStore().getName() );
        assertEquals( "neostore.propertystore.db.index.keys", layout.propertyKeyTokenNamesStore().getName() );
        assertEquals( "neostore.propertystore.db.strings", layout.propertyStringStore().getName() );
        assertEquals( "neostore.relationshipgroupstore.db", layout.relationshipGroupStore().getName() );
        assertEquals( "neostore.relationshipstore.db", layout.relationshipStore().getName() );
        assertEquals( "neostore.relationshiptypestore.db", layout.relationshipTypeTokenStore().getName() );
        assertEquals( "neostore.relationshiptypestore.db.names", layout.relationshipTypeTokenNamesStore().getName() );
        assertEquals( "neostore.schemastore.db", layout.schemaStore().getName() );
    }

    @Test
    void idFilesHaveExpectedNames()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        assertEquals( "neostore.id", layout.idMetadataStore().getName() );
        assertEquals( "neostore.labeltokenstore.db.id", layout.idLabelTokenStore().getName() );
        assertEquals( "neostore.labeltokenstore.db.names.id", layout.idLabelTokenNamesStore().getName() );
        assertEquals( "neostore.nodestore.db.id", layout.idNodeStore().getName() );
        assertEquals( "neostore.nodestore.db.labels.id", layout.idNodeLabelStore().getName() );
        assertEquals( "neostore.propertystore.db.arrays.id", layout.idPropertyArrayStore().getName() );
        assertEquals( "neostore.propertystore.db.id", layout.idPropertyStore().getName() );
        assertEquals( "neostore.propertystore.db.index.id", layout.idPropertyKeyTokenStore().getName() );
        assertEquals( "neostore.propertystore.db.index.keys.id", layout.idPropertyKeyTokenNamesStore().getName() );
        assertEquals( "neostore.propertystore.db.strings.id", layout.idPropertyStringStore().getName() );
        assertEquals( "neostore.relationshipgroupstore.db.id", layout.idRelationshipGroupStore().getName() );
        assertEquals( "neostore.relationshipstore.db.id", layout.idRelationshipStore().getName() );
        assertEquals( "neostore.relationshiptypestore.db.id", layout.idRelationshipTypeTokenStore().getName() );
        assertEquals( "neostore.relationshiptypestore.db.names.id", layout.idRelationshipTypeTokenNamesStore().getName() );
        assertEquals( "neostore.schemastore.db.id", layout.idSchemaStore().getName() );
    }

    @Test
    void allStoreFiles()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        Set<String> files = layout.storeFiles().stream().map( File::getName ).collect( toSet() );
        assertThat( files, hasItem( "neostore" ) );
        assertThat( files, hasItem( "neostore.counts.db.a" ) );
        assertThat( files, hasItem( "neostore.counts.db.b" ) );
        assertThat( files, hasItem( "neostore.labelscanstore.db" ) );
        assertThat( files, hasItem( "neostore.labeltokenstore.db" ) );
        assertThat( files, hasItem( "neostore.labeltokenstore.db.names" ) );
        assertThat( files, hasItem( "neostore.nodestore.db" ) );
        assertThat( files, hasItem( "neostore.nodestore.db.labels" ) );
        assertThat( files, hasItem( "neostore.propertystore.db" ) );
        assertThat( files, hasItem( "neostore.propertystore.db.arrays" ) );
        assertThat( files, hasItem( "neostore.propertystore.db.index" ) );
        assertThat( files, hasItem( "neostore.propertystore.db.index.keys" ) );
        assertThat( files, hasItem( "neostore.propertystore.db.strings" ) );
        assertThat( files, hasItem( "neostore.relationshipgroupstore.db" ) );
        assertThat( files, hasItem( "neostore.relationshipstore.db" ) );
        assertThat( files, hasItem( "neostore.relationshiptypestore.db" ) );
        assertThat( files, hasItem( "neostore.relationshiptypestore.db.names" ) );
        assertThat( files, hasItem( "neostore.schemastore.db" ) );
    }

    @Test
    void lookupFileByDatabaseFile()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        DatabaseFile[] databaseFiles = DatabaseFile.values();
        for ( DatabaseFile databaseFile : databaseFiles )
        {
            assertNotNull( layout.file( databaseFile ).findAny().orElseThrow( () -> new RuntimeException( "Mapping was expected to be found" ) ) );
        }

        File metadata = layout.file( DatabaseFile.METADATA_STORE ).findFirst().orElseThrow( () -> new RuntimeException( "Mapping was expected to be found" ) );
        assertEquals( "neostore", metadata.getName() );
    }

    @Test
    void lookupIdFileByDatabaseFile()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        DatabaseFile[] databaseFiles = DatabaseFile.values();
        for ( DatabaseFile databaseFile : databaseFiles )
        {
            Optional<File> idFile = layout.idFile( databaseFile );
            assertEquals(databaseFile.hasIdFile(),  idFile.isPresent() );
        }

        File metadataId = layout.idFile( DatabaseFile.METADATA_STORE ).orElseThrow( () -> new RuntimeException( "Mapping was expected to be found" ) );
        assertEquals( "neostore.id", metadataId.getName() );
    }
}
