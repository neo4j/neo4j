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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Neo4jLayoutExtension
class DatabaseLayoutTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void databaseLayoutForAbsoluteFile()
    {
        File databaseDir = testDirectory.directory( "neo4j" );
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( databaseDir );
        assertEquals( databaseLayout.databaseDirectory(), databaseDir );
    }

    @Test
    void databaseLayoutResolvesLinks() throws IOException
    {
        Path basePath = testDirectory.homeDir().toPath();
        File databaseDir = databaseLayout.databaseDirectory();
        Path linkPath = basePath.resolve( "link" );
        Path symbolicLink = Files.createSymbolicLink( linkPath, databaseDir.toPath() );
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( symbolicLink.toFile() );
        assertEquals( databaseLayout.databaseDirectory(), databaseDir );
    }

    @Test
    void databaseLayoutUseCanonicalRepresentation()
    {
        File dbDir = testDirectory.directory( "notCanonical" );
        Path notCanonicalPath = dbDir.toPath().resolve( "../anotherDatabase" ) ;
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( notCanonicalPath.toFile() );
        assertEquals( testDirectory.directory( "anotherDatabase" ), databaseLayout.databaseDirectory() );
    }

    @Test
    void databaseLayoutForName()
    {
        String databaseName = "testDatabase";
        Neo4jLayout storeLayout = neo4jLayout;
        DatabaseLayout testDatabase = storeLayout.databaseLayout( databaseName );
        assertEquals( new File( storeLayout.databasesDirectory(), databaseName ), testDatabase.databaseDirectory() );
    }

    @Test
    void databaseLayoutForFolderAndName()
    {
        String database = "database";
        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout( database );
        assertEquals( database, databaseLayout.databaseDirectory().getName() );
    }

    @Test
    void databaseLayoutProvideCorrectDatabaseName()
    {
        assertEquals( "neo4j", databaseLayout.getDatabaseName() );
        assertEquals( "testDb", neo4jLayout.databaseLayout( "testDb" ).getDatabaseName() );
    }

    @Test
    void storeFilesHaveExpectedNames()
    {
        DatabaseLayout layout = databaseLayout;
        assertEquals( "neostore", layout.metadataStore().getName() );
        assertEquals( "neostore.counts.db", layout.countStore().getName() );
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
        DatabaseLayout layout = databaseLayout;
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
        DatabaseLayout layout = databaseLayout;
        Set<String> files = layout.storeFiles().stream().map( File::getName ).collect( toSet() );
        assertThat( files ).contains( "neostore" );
        assertThat( files ).contains( "neostore.counts.db" );
        assertThat( files ).contains( "neostore.labelscanstore.db" );
        assertThat( files ).contains( "neostore.labeltokenstore.db" );
        assertThat( files ).contains( "neostore.labeltokenstore.db.names" );
        assertThat( files ).contains( "neostore.nodestore.db" );
        assertThat( files ).contains( "neostore.nodestore.db.labels" );
        assertThat( files ).contains( "neostore.propertystore.db" );
        assertThat( files ).contains( "neostore.propertystore.db.arrays" );
        assertThat( files ).contains( "neostore.propertystore.db.index" );
        assertThat( files ).contains( "neostore.propertystore.db.index.keys" );
        assertThat( files ).contains( "neostore.propertystore.db.strings" );
        assertThat( files ).contains( "neostore.relationshipgroupstore.db" );
        assertThat( files ).contains( "neostore.relationshipstore.db" );
        assertThat( files ).contains( "neostore.relationshiptypestore.db" );
        assertThat( files ).contains( "neostore.relationshiptypestore.db.names" );
        assertThat( files ).contains( "neostore.schemastore.db" );
    }

    @Test
    void allFilesContainsStoreFiles()
    {

        DatabaseFile nodeStore = DatabaseFile.NODE_STORE;
        List<File> allNodeStoreFile = databaseLayout.allFiles( nodeStore ).collect( toList() );
        File nodeStoreStoreFile = databaseLayout.file( nodeStore );
        assertThat( allNodeStoreFile ).contains( nodeStoreStoreFile );
    }

    @Test
    void allFilesContainsIdFileIfPresent()
    {

        DatabaseFile nodeStore = DatabaseFile.NODE_STORE;
        List<File> allNodeStoreFile = databaseLayout.allFiles( nodeStore ).collect( toList() );
        File nodeStoreIdFile = databaseLayout.idFile( nodeStore ).get();
        assertThat( allNodeStoreFile ).contains( nodeStoreIdFile );
    }

    @Test
    void lookupFileByDatabaseFile()
    {
        DatabaseLayout layout = databaseLayout;
        DatabaseFile[] databaseFiles = DatabaseFile.values();
        for ( DatabaseFile databaseFile : databaseFiles )
        {
            assertNotNull( layout.file( databaseFile ) );
        }

        File metadata = layout.file( DatabaseFile.METADATA_STORE );
        assertEquals( "neostore", metadata.getName() );
    }

    @Test
    void lookupIdFileByDatabaseFile()
    {
        DatabaseLayout layout = databaseLayout;
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
