/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
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
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    @Test
    void shouldFailIfFileDoesNotExist()
    {
        // given
        File missingFile = new File( testDirectory.directory(), "missing-file" );
        RecordStoreVersionCheck storeVersionCheck = new RecordStoreVersionCheck( pageCache );

        // when
        RecordStoreVersionCheck.Result result = storeVersionCheck.hasVersion( missingFile, "version" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( RecordStoreVersionCheck.Result.Outcome.missingStoreFile, result.outcome );
        assertNull( result.actualVersion );
    }

    @Test
    void shouldReportShortFileDoesNotHaveSpecifiedVersion() throws IOException
    {
        // given
        File shortFile = fileContaining( fileSystem, "nothing interesting" );
        RecordStoreVersionCheck storeVersionCheck = new RecordStoreVersionCheck( pageCache );

        // when
        RecordStoreVersionCheck.Result result = storeVersionCheck.hasVersion( shortFile, "version" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( RecordStoreVersionCheck.Result.Outcome.storeVersionNotFound, result.outcome );
        assertNull( result.actualVersion );
    }

    @Test
    void shouldReportFileWithIncorrectVersion() throws IOException
    {
        // given
        File neoStore = emptyFile( fileSystem );
        long v1 = MetaDataStore.versionStringToLong( "V1" );
        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION, v1 );
        RecordStoreVersionCheck storeVersionCheck = new RecordStoreVersionCheck( pageCache );

        // when
        RecordStoreVersionCheck.Result result = storeVersionCheck.hasVersion( neoStore, "V2" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( RecordStoreVersionCheck.Result.Outcome.unexpectedStoreVersion, result.outcome );
        assertEquals( "V1", result.actualVersion );
    }

    @Test
    void shouldReportFileWithCorrectVersion() throws IOException
    {
        // given
        File neoStore = emptyFile( fileSystem );
        long v1 = MetaDataStore.versionStringToLong( "V1" );
        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION, v1 );
        RecordStoreVersionCheck storeVersionCheck = new RecordStoreVersionCheck( pageCache );

        // when
        RecordStoreVersionCheck.Result result = storeVersionCheck.hasVersion( neoStore, "V1" );

        // then
        assertTrue( result.outcome.isSuccessful() );
        assertEquals( RecordStoreVersionCheck.Result.Outcome.ok, result.outcome );
        assertNull( result.actualVersion );
    }

    private File emptyFile( FileSystemAbstraction fs ) throws IOException
    {
        File shortFile = testDirectory.file( "empty" );
        fs.deleteFile( shortFile );
        fs.create( shortFile ).close();
        return shortFile;
    }

    private File fileContaining( FileSystemAbstraction fs, String content ) throws IOException
    {
        File shortFile = testDirectory.file( "file" );
        fs.deleteFile( shortFile );
        try ( OutputStream outputStream = fs.openAsOutputStream( shortFile, false ) )
        {
            outputStream.write( UTF8.encode( content ) );
            return shortFile;
        }
    }
}
