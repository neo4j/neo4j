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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionLogFilesTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private File storeDirectory;
    private final String filename = "filename";

    @Before
    public void setUp()
    {
        storeDirectory = testDirectory.directory();
    }

    @Test
    public void shouldGetTheFileNameForAGivenVersion() throws IOException
    {
        // given
        final LogFiles files = createLogFiles();
        final int version = 12;

        // when
        final File versionFileName = files.getLogFileForVersion( version );

        // then
        final File expected = new File( storeDirectory, getVersionedLogFileName( version ) );
        assertEquals( expected, versionFileName );
    }

    @Test
    public void shouldVisitEachLofFile() throws Throwable
    {
        // given
        LogFiles files = createLogFiles();

        fileSystemRule.create( new File( storeDirectory, getVersionedLogFileName( "1" ) ) ).close();
        fileSystemRule.create( new File( storeDirectory, getVersionedLogFileName( "some", "2" ) ) ).close();
        fileSystemRule.create( new File( storeDirectory, getVersionedLogFileName( "3" ) ) ).close();
        fileSystemRule.create( new File( storeDirectory, filename ) ).close();

        // when
        final List<File> seenFiles = new ArrayList<>();
        final List<Long> seenVersions = new ArrayList<>();

        files.accept( ( file, logVersion ) ->
        {
            seenFiles.add( file );
            seenVersions.add( logVersion );
        } );

        // then
        assertThat( seenFiles, containsInAnyOrder(
                new File( storeDirectory, getVersionedLogFileName( filename, "1" ) ),
                new File( storeDirectory, getVersionedLogFileName( filename, "3" ) ) )  );
        assertThat( seenVersions, containsInAnyOrder( 1L, 3L ) );
        files.shutdown();
    }

    @Test
    public void shouldBeAbleToRetrieveTheHighestLogVersion() throws Throwable
    {
        // given
        LogFiles files = createLogFiles();

        fileSystemRule.create( new File( storeDirectory, getVersionedLogFileName( "1" ) ) ).close();
        fileSystemRule.create( new File( storeDirectory, getVersionedLogFileName( "some", "4" ) ) ).close();
        fileSystemRule.create( new File( storeDirectory, getVersionedLogFileName( "3" ) ) ).close();
        fileSystemRule.create( new File( storeDirectory, filename ) ).close();

        // when
        final long highestLogVersion = files.getHighestLogVersion();

        // then
        assertEquals( 3, highestLogVersion );
        files.shutdown();
    }

    @Test
    public void shouldReturnANegativeValueIfThereAreNoLogFiles() throws Throwable
    {
        // given
        LogFiles files = createLogFiles();

        fileSystemRule.create( new File( storeDirectory, getVersionedLogFileName( "some", "4" ) ) ).close();
        fileSystemRule.create( new File( storeDirectory, filename ) ).close();

        // when
        final long highestLogVersion = files.getHighestLogVersion();

        // then
        assertEquals( -1, highestLogVersion );
        files.shutdown();
    }

    @Test
    public void shouldFindTheVersionBasedOnTheFilename() throws Throwable
    {
        // given
        LogFiles logFiles = createLogFiles();
        final File file = new File( "v....2" );

        // when
        long logVersion = logFiles.getLogVersion( file );

        // then
        assertEquals( 2, logVersion );
        logFiles.shutdown();
    }

    @Test
    public void shouldThrowIfThereIsNoVersionInTheFileName() throws IOException
    {
        LogFiles logFiles = createLogFiles();
        final File file = new File( "wrong" );

        // when
        try
        {
            logFiles.getLogVersion( file );
            fail( "should have thrown" );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( "Invalid log file '" + file.getName() + "'", ex.getMessage() );
        }
    }

    @Test( expected = NumberFormatException.class )
    public void shouldThrowIfVersionIsNotANumber() throws IOException
    {
        // given
        LogFiles logFiles = createLogFiles();
        final File file = new File( getVersionedLogFileName( "aa", "A" ) );

        // when
        logFiles.getLogVersion( file );
    }

    @Test
    public void isLogFile() throws IOException
    {
        LogFiles logFiles = createLogFiles();
        assertFalse( logFiles.isLogFile( new File( "aaa.tx.log" ) ) );
        assertTrue( logFiles.isLogFile( new File( "filename.0" ) ) );
        assertTrue( logFiles.isLogFile( new File( "filename.17" ) ) );
    }

    private LogFiles createLogFiles() throws IOException
    {
        return LogFilesBuilder
                .builder( storeDirectory, fileSystemRule )
                .withLogFileName( filename )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .build();
    }

    private String getVersionedLogFileName( int version )
    {
        return getVersionedLogFileName( filename, String.valueOf( version ) );
    }

    private String getVersionedLogFileName( String version )
    {
        return getVersionedLogFileName( filename, version );
    }

    private String getVersionedLogFileName( String filename, String version )
    {
        return filename + "." + version;
    }
}
