/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StoreVersionCheckTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void shouldFailIfFileDoesNotExist()
    {
        // given
        File missingFile = new File("/you/will/never/find/me");
        PageCache pageCache = pageCacheRule.getPageCache( fs.get() );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );

        // when
        StoreVersionCheck.Result result = storeVersionCheck.hasVersion( missingFile, "version" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( StoreVersionCheck.Result.Outcome.missingStoreFile, result.outcome );
        assertNull( result.actualVersion );
    }

    @Test
    public void shouldReportShortFileDoesNotHaveSpecifiedVersion() throws IOException
    {
        // given
        File shortFile = fileContaining( fs.get(), "nothing interesting" );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCacheRule.getPageCache( fs.get() ) );

        // when
        StoreVersionCheck.Result result = storeVersionCheck.hasVersion( shortFile, "version" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( StoreVersionCheck.Result.Outcome.storeVersionNotFound, result.outcome );
        assertNull( result.actualVersion );
    }

    @Test
    public void shouldReportFileWithIncorrectVersion() throws IOException
    {
        // given
        File neoStore = emptyFile( fs.get() );
        long v1 = MetaDataStore.versionStringToLong( "V1" );
        PageCache pageCache = pageCacheRule.getPageCache( fs.get() );
        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION, v1 );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );

        // when
        StoreVersionCheck.Result result = storeVersionCheck.hasVersion( neoStore, "V2" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( StoreVersionCheck.Result.Outcome.unexpectedUpgradingStoreVersion, result.outcome );
        assertEquals( "V1", result.actualVersion );
    }

    @Test
    public void shouldReportFileWithCorrectVersion() throws IOException
    {
        // given
        File neoStore = emptyFile( fs.get() );
        long v1 = MetaDataStore.versionStringToLong( "V1" );
        PageCache pageCache = pageCacheRule.getPageCache( fs.get() );
        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION, v1 );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );

        // when
        StoreVersionCheck.Result result = storeVersionCheck.hasVersion( neoStore, "V1" );

        // then
        assertTrue( result.outcome.isSuccessful() );
        assertEquals( StoreVersionCheck.Result.Outcome.ok, result.outcome );
        assertNull( result.actualVersion );
    }

    private File emptyFile( FileSystemAbstraction fs ) throws IOException
    {
        File shortFile = new File( "shortFile" );
        fs.deleteFile( shortFile );
        fs.create( shortFile );
        return shortFile;
    }

    private File fileContaining( FileSystemAbstraction fs, String content ) throws IOException
    {
        File shortFile = new File( "shortFile" );
        fs.deleteFile( shortFile );
        try ( OutputStream outputStream = fs.openAsOutputStream( shortFile, false ) )
        {
            outputStream.write( UTF8.encode( content ) );
            return shortFile;
        }
    }
}
