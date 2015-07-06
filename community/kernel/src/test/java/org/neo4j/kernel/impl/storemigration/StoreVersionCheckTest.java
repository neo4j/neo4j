/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StoreVersionCheckTest
{
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void shouldReportMissingFileDoesNotHaveSpecifiedVersion()
    {
        // given
        File missingFile = new File("/you/will/never/find/me");
        PageCache pageCache = pageCacheRule.getPageCache( new EphemeralFileSystemAbstraction() );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );

        // then
        assertFalse( storeVersionCheck.hasVersion( missingFile, "version" ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportShortFileDoesNotHaveSpecifiedVersion() throws IOException
    {
        // given
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File shortFile = fileContaining( fs, "a" );

        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCacheRule.getPageCache( fs ) );

        // then
        assertFalse( storeVersionCheck.hasVersion( shortFile, "version" ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportFileWithIncorrectVersion() throws IOException
    {
        // given
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File shortFile = fileContaining( fs, "versionWhichIsIncorrect" );

        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCacheRule.getPageCache( fs ) );

        // then
        assertFalse( storeVersionCheck.hasVersion( shortFile, "correctVersion 1" ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportFileWithCorrectVersion() throws IOException
    {
        // given
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File shortFile = fileContaining( fs, "correctVersion 1" );

        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCacheRule.getPageCache( fs ) );

        // then
        assertTrue( storeVersionCheck.hasVersion( shortFile, "correctVersion 1" ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportFileWithCorrectVersionWhenTrailerSpansMultiplePages() throws IOException
    {
        // given
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        String trailer = "correctVersion 1";
        byte[] trailerData = UTF8.encode( trailer );
        byte[] data = new byte[pageCache.pageSize() + trailerData.length / 2];
        System.arraycopy( trailerData, 0, data, pageCache.pageSize() - trailerData.length / 2, trailerData.length );
        File shortFile = fileContaining( fs, UTF8.decode( data ) );

        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );

        // then
        assertTrue( storeVersionCheck.hasVersion( shortFile, trailer ).outcome.isSuccessful() );
    }

    private File fileContaining( FileSystemAbstraction fs, String content ) throws IOException
    {
        File shortFile = File.createTempFile( "shortFile", "" );
        shortFile.deleteOnExit();
        OutputStream outputStream = fs.openAsOutputStream( shortFile, true );
        outputStream.write( UTF8.encode( content ) );
        outputStream.close();
        return shortFile;
    }
}
