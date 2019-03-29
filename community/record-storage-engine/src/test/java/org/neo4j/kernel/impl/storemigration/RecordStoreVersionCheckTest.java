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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionCheck.Outcome;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@PageCacheExtension
class RecordStoreVersionCheckTest
{
    @Inject
    protected TestDirectory testDirectory;
    @Inject
    protected FileSystemAbstraction fileSystem;
    @Inject
    protected PageCache pageCache;

    @Test
    void shouldFailIfFileDoesNotExist()
    {
        // given
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.Result result = storeVersionCheck.checkUpgrade( "version" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( Outcome.missingStoreFile, result.outcome );
        assertNull( result.actualVersion );
    }

    @Test
    void shouldReportShortFileDoesNotHaveSpecifiedVersion() throws IOException
    {
        // given
        metaDataFileContaining( testDirectory.databaseLayout(), fileSystem, "nothing interesting" );
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.Result result = storeVersionCheck.checkUpgrade( "version" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( Outcome.storeVersionNotFound, result.outcome );
        assertNull( result.actualVersion );
    }

    @Test
    void shouldReportFileWithIncorrectVersion() throws IOException
    {
        // given
        File neoStore = emptyFile( fileSystem );
        long v1 = MetaDataStore.versionStringToLong( "V1" );
        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION, v1 );
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.Result result = storeVersionCheck.checkUpgrade( "V2" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( Outcome.unexpectedStoreVersion, result.outcome );
        assertEquals( "V1", result.actualVersion );
    }

    @Test
    void shouldReportFileWithCorrectVersion() throws IOException
    {
        // given
        File neoStore = emptyFile( fileSystem );
        String storeVersion = "V1";
        long v1 = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION, v1 );
        RecordStoreVersionCheck storeVersionCheck = newStoreVersionCheck();

        // when
        StoreVersionCheck.Result result = storeVersionCheck.checkUpgrade( storeVersion );

        // then
        assertTrue( result.outcome.isSuccessful() );
        assertEquals( Outcome.ok, result.outcome );
        assertEquals( storeVersion, result.actualVersion );
    }

    private File emptyFile( FileSystemAbstraction fs ) throws IOException
    {
        File shortFile = testDirectory.databaseLayout().metadataStore();
        fs.deleteFile( shortFile );
        fs.write( shortFile ).close();
        return shortFile;
    }

    private void metaDataFileContaining( DatabaseLayout layout, FileSystemAbstraction fs, String content ) throws IOException
    {
        File shortFile = layout.metadataStore();
        fs.deleteFile( shortFile );
        try ( OutputStream outputStream = fs.openAsOutputStream( shortFile, false ) )
        {
            outputStream.write( UTF8.encode( content ) );
        }
    }

    private RecordStoreVersionCheck newStoreVersionCheck()
    {
        return new RecordStoreVersionCheck( fileSystem, pageCache, testDirectory.databaseLayout(), NullLogProvider.getInstance(), Config.defaults() );
    }
}
