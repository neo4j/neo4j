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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.string.UTF8;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StoreVersionCheckTest
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory )
                    .around( fileSystemRule ).around( pageCacheRule );

    @Test
    public void shouldFailIfFileDoesNotExist()
    {
        // given
        File missingFile = new File( directory.directory(), "missing-file" );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
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
        File shortFile = fileContaining( fileSystemRule.get(), "nothing interesting" );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCacheRule.getPageCache( fileSystemRule.get() ) );

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
        File neoStore = emptyFile( fileSystemRule.get() );
        long v1 = MetaDataStore.versionStringToLong( "V1" );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION, v1 );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );

        // when
        StoreVersionCheck.Result result = storeVersionCheck.hasVersion( neoStore, "V2" );

        // then
        assertFalse( result.outcome.isSuccessful() );
        assertEquals( StoreVersionCheck.Result.Outcome.unexpectedStoreVersion, result.outcome );
        assertEquals( "V1", result.actualVersion );
    }

    @Test
    public void shouldReportFileWithCorrectVersion() throws IOException
    {
        // given
        File neoStore = emptyFile( fileSystemRule.get() );
        long v1 = MetaDataStore.versionStringToLong( "V1" );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
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
        File shortFile = directory.file( "empty" );
        fs.deleteFile( shortFile );
        fs.create( shortFile ).close();
        return shortFile;
    }

    private File fileContaining( FileSystemAbstraction fs, String content ) throws IOException
    {
        File shortFile = directory.file( "file" );
        fs.deleteFile( shortFile );
        try ( OutputStream outputStream = fs.openAsOutputStream( shortFile, false ) )
        {
            outputStream.write( UTF8.encode( content ) );
            return shortFile;
        }
    }
}
