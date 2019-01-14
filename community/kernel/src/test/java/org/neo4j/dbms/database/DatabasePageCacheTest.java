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
package org.neo4j.dbms.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;

@ExtendWith( TestDirectoryExtension.class )
class DatabasePageCacheTest
{
    @Inject
    private TestDirectory testDirectory;
    private DatabasePageCache databasePageCache;
    private PageCache globalPageCache;
    private PagedFileAnswer pagedFileMapper;

    @BeforeEach
    void setUp() throws IOException
    {
        globalPageCache = mock( PageCache.class );
        pagedFileMapper = new PagedFileAnswer();
        when( globalPageCache.map( any( File.class ), eq( PAGE_SIZE ) ) ).then( pagedFileMapper );
        databasePageCache = new DatabasePageCache( globalPageCache );
    }

    @AfterEach
    void tearDown()
    {
        if ( databasePageCache != null )
        {
            databasePageCache.close();
        }
    }

    @Test
    void mapDatabaseFile() throws IOException
    {
        File mapFile = testDirectory.createFile( "mapFile" );
        PagedFile pagedFile = databasePageCache.map( mapFile, PAGE_SIZE );

        assertNotNull( pagedFile );
        verify( globalPageCache ).map( mapFile, PAGE_SIZE );
    }

    @Test
    void listExistingDatabaseMappings() throws IOException
    {
        File mapFile1 = testDirectory.createFile( "mapFile1" );
        File mapFile2 = testDirectory.createFile( "mapFile2" );
        PagedFile pagedFile = databasePageCache.map( mapFile1, PAGE_SIZE );
        PagedFile pagedFile2 = databasePageCache.map( mapFile2, PAGE_SIZE );

        List<PagedFile> pagedFiles = databasePageCache.listExistingMappings();
        assertThat( pagedFiles, hasSize( 2 ) );
        assertThat( pagedFiles, hasItem( pagedFile ) );
        assertThat( pagedFiles, hasItem( pagedFile2 ) );
    }

    @Test
    void doNotIncludeNotDatabaseFilesInMappingsList() throws IOException
    {
        try ( DatabasePageCache anotherDatabaseCache = new DatabasePageCache( globalPageCache ) )
        {
            File mapFile1 = testDirectory.createFile( "mapFile1" );
            File mapFile2 = testDirectory.createFile( "mapFile2" );
            File mapFile3 = testDirectory.createFile( "mapFile3" );
            File mapFile4 = testDirectory.createFile( "mapFile4" );
            PagedFile pagedFile = databasePageCache.map( mapFile1, PAGE_SIZE );
            PagedFile pagedFile2 = databasePageCache.map( mapFile2, PAGE_SIZE );
            PagedFile pagedFile3 = anotherDatabaseCache.map( mapFile3, PAGE_SIZE );
            PagedFile pagedFile4 = anotherDatabaseCache.map( mapFile4, PAGE_SIZE );

            List<PagedFile> pagedFiles = databasePageCache.listExistingMappings();
            assertThat( pagedFiles, hasSize( 2 ) );
            assertThat( pagedFiles, hasItems( pagedFile, pagedFile2 ) );

            List<PagedFile> anotherPagedFiles = anotherDatabaseCache.listExistingMappings();
            assertThat( anotherPagedFiles, hasSize( 2 ) );
            assertThat( anotherPagedFiles, hasItems( pagedFile3, pagedFile4 ) );
        }
    }

    @Test
    void existingMappingRestrictedToDatabaseMappedFiles() throws IOException
    {
        try ( DatabasePageCache anotherDatabaseCache = new DatabasePageCache( globalPageCache ) )
        {
            File mapFile1 = testDirectory.createFile( "mapFile1" );
            File mapFile2 = testDirectory.createFile( "mapFile2" );
            File mapFile3 = testDirectory.createFile( "mapFile3" );
            File mapFile4 = testDirectory.createFile( "mapFile4" );
            databasePageCache.map( mapFile1, PAGE_SIZE );
            databasePageCache.map( mapFile2, PAGE_SIZE );
            anotherDatabaseCache.map( mapFile3, PAGE_SIZE );
            anotherDatabaseCache.map( mapFile4, PAGE_SIZE );

            assertTrue( databasePageCache.getExistingMapping( mapFile1 ).isPresent() );
            assertTrue( databasePageCache.getExistingMapping( mapFile2 ).isPresent() );
            assertFalse( databasePageCache.getExistingMapping( mapFile3 ).isPresent() );
            assertFalse( databasePageCache.getExistingMapping( mapFile4 ).isPresent() );

            assertFalse( anotherDatabaseCache.getExistingMapping( mapFile1 ).isPresent() );
            assertFalse( anotherDatabaseCache.getExistingMapping( mapFile2 ).isPresent() );
            assertTrue( anotherDatabaseCache.getExistingMapping( mapFile3 ).isPresent() );
            assertTrue( anotherDatabaseCache.getExistingMapping( mapFile4 ).isPresent() );
        }
    }

    @Test
    void throwOnMultipleCloseAttempts()
    {
        databasePageCache.close();
        assertThrows( IllegalStateException.class, () -> databasePageCache.close() );
        databasePageCache = null;
    }

    @Test
    void flushOnlyAffectsDatabaseRelatedFiles() throws IOException
    {
        try ( DatabasePageCache anotherDatabaseCache = new DatabasePageCache( globalPageCache ) )
        {
            File mapFile1 = testDirectory.createFile( "mapFile1" );
            File mapFile2 = testDirectory.createFile( "mapFile2" );
            File mapFile3 = testDirectory.createFile( "mapFile3" );
            File mapFile4 = testDirectory.createFile( "mapFile4" );
            databasePageCache.map( mapFile1, PAGE_SIZE );
            databasePageCache.map( mapFile2, PAGE_SIZE );
            anotherDatabaseCache.map( mapFile3, PAGE_SIZE );
            anotherDatabaseCache.map( mapFile4, PAGE_SIZE );

            databasePageCache.flushAndForce();

            List<PagedFile> pagedFiles = pagedFileMapper.getPagedFiles();
            PagedFile originalPagedFile1 = findPagedFile( pagedFiles, mapFile1 );
            PagedFile originalPagedFile2 = findPagedFile( pagedFiles, mapFile2 );
            PagedFile originalPagedFile3 = findPagedFile( pagedFiles, mapFile3 );
            PagedFile originalPagedFile4 = findPagedFile( pagedFiles, mapFile4 );

            verify( originalPagedFile1 ).flushAndForce();
            verify( originalPagedFile2 ).flushAndForce();
            verify( originalPagedFile3, never() ).flushAndForce();
            verify( originalPagedFile4, never() ).flushAndForce();
        }
    }

    @Test
    void flushWithLimiterOnlyAffectsDatabaseRelatedFiles() throws IOException
    {
        try ( DatabasePageCache anotherDatabaseCache = new DatabasePageCache( globalPageCache ) )
        {
            File mapFile1 = testDirectory.createFile( "mapFile1" );
            File mapFile2 = testDirectory.createFile( "mapFile2" );
            File mapFile3 = testDirectory.createFile( "mapFile3" );
            File mapFile4 = testDirectory.createFile( "mapFile4" );
            databasePageCache.map( mapFile1, PAGE_SIZE );
            databasePageCache.map( mapFile2, PAGE_SIZE );
            anotherDatabaseCache.map( mapFile3, PAGE_SIZE );
            anotherDatabaseCache.map( mapFile4, PAGE_SIZE );

            databasePageCache.flushAndForce( IOLimiter.UNLIMITED);

            List<PagedFile> pagedFiles = pagedFileMapper.getPagedFiles();
            PagedFile originalPagedFile1 = findPagedFile( pagedFiles, mapFile1 );
            PagedFile originalPagedFile2 = findPagedFile( pagedFiles, mapFile2 );
            PagedFile originalPagedFile3 = findPagedFile( pagedFiles, mapFile3 );
            PagedFile originalPagedFile4 = findPagedFile( pagedFiles, mapFile4 );

            verify( originalPagedFile1 ).flushAndForce( IOLimiter.UNLIMITED );
            verify( originalPagedFile2 ).flushAndForce( IOLimiter.UNLIMITED );
            verify( originalPagedFile3, never() ).flushAndForce( IOLimiter.UNLIMITED );
            verify( originalPagedFile4, never() ).flushAndForce( IOLimiter.UNLIMITED );
        }
    }

    @Test
    void closingFileCloseCacheMapping() throws IOException
    {
        File mapFile1 = testDirectory.createFile( "mapFile1" );
        File mapFile2 = testDirectory.createFile( "mapFile2" );
        PagedFile pagedFile1 = databasePageCache.map( mapFile1, PAGE_SIZE );
        PagedFile pagedFile2 = databasePageCache.map( mapFile2, PAGE_SIZE );

        assertEquals( 2, databasePageCache.listExistingMappings().size() );

        pagedFile1.close();

        assertEquals( 1, databasePageCache.listExistingMappings().size() );

        pagedFile2.close();

        assertTrue( databasePageCache.listExistingMappings().isEmpty() );
    }

    private PagedFile findPagedFile( List<PagedFile> pagedFiles, File mapFile )
    {
        return pagedFiles.stream().filter( pagedFile -> pagedFile.file().equals( mapFile ) ).findFirst().orElseThrow(
                () -> new IllegalStateException( format( "Mapped paged file '%s' not found", mapFile.getName() ) ) );
    }

    private static class PagedFileAnswer implements Answer<PagedFile>
    {
        private List<PagedFile> pagedFiles = new ArrayList<>();

        @Override
        public PagedFile answer( InvocationOnMock invocation )
        {
            PagedFile pagedFile = mock( PagedFile.class );
            when( pagedFile.file() ).thenReturn( invocation.getArgument( 0 ) );
            pagedFiles.add( pagedFile );
            return pagedFile;
        }

        List<PagedFile> getPagedFiles()
        {
            return pagedFiles;
        }
    }
}
