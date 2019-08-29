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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

@TestDirectoryExtension
class TransactionLogFilesTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    private final String filename = "filename";

    @Test
    void shouldGetTheFileNameForAGivenVersion() throws IOException
    {
        // given
        final LogFiles files = createLogFiles();
        final int version = 12;

        // when
        final File versionFileName = files.getLogFileForVersion( version );

        // then
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        final File expected = createTransactionLogFile( databaseLayout, getVersionedLogFileName( version ) );
        assertEquals( expected, versionFileName );
    }

    @Test
    void shouldVisitEachLofFile() throws Throwable
    {
        // given
        LogFiles files = createLogFiles();
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();

        fileSystem.write( createTransactionLogFile( databaseLayout, getVersionedLogFileName( "1" ) ) ).close();
        fileSystem.write( createTransactionLogFile( databaseLayout, getVersionedLogFileName( "some", "2" ) ) ).close();
        fileSystem.write( createTransactionLogFile( databaseLayout, getVersionedLogFileName( "3" ) ) ).close();
        fileSystem.write( createTransactionLogFile( databaseLayout, filename ) ).close();

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
                createTransactionLogFile( databaseLayout, getVersionedLogFileName( filename, "1" ) ),
                createTransactionLogFile( databaseLayout, getVersionedLogFileName( filename, "3" ) ) )  );
        assertThat( seenVersions, containsInAnyOrder( 1L, 3L ) );
        files.shutdown();
    }

    @Test
    void shouldBeAbleToRetrieveTheHighestLogVersion() throws Throwable
    {
        // given
        LogFiles files = createLogFiles();

        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        fileSystem.write( createTransactionLogFile( databaseLayout, getVersionedLogFileName( "1" ) ) ).close();
        fileSystem.write( createTransactionLogFile( databaseLayout, getVersionedLogFileName( "some", "4" ) ) ).close();
        fileSystem.write( createTransactionLogFile( databaseLayout, getVersionedLogFileName( "3" ) ) ).close();
        fileSystem.write( createTransactionLogFile( databaseLayout, filename ) ).close();

        // when
        final long highestLogVersion = files.getHighestLogVersion();

        // then
        assertEquals( 3, highestLogVersion );
        files.shutdown();
    }

    @Test
    void shouldReturnANegativeValueIfThereAreNoLogFiles() throws Throwable
    {
        // given
        LogFiles files = createLogFiles();
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();

        fileSystem.write( databaseLayout.file( getVersionedLogFileName( "some", "4" ) ) ).close();
        fileSystem.write( databaseLayout.file( filename ) ).close();

        // when
        final long highestLogVersion = files.getHighestLogVersion();

        // then
        assertEquals( -1, highestLogVersion );
        files.shutdown();
    }

    @Test
    void shouldFindTheVersionBasedOnTheFilename() throws Throwable
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
    void shouldThrowIfThereIsNoVersionInTheFileName() throws IOException
    {
        LogFiles logFiles = createLogFiles();
        final File file = new File( "wrong" );

        // when
        RuntimeException exception = assertThrows( RuntimeException.class, () -> logFiles.getLogVersion( file ) );
        assertEquals( "Invalid log file '" + file.getName() + "'", exception.getMessage() );
    }

    @Test
    void shouldThrowIfVersionIsNotANumber() throws IOException
    {
        // given
        LogFiles logFiles = createLogFiles();
        final File file = new File( getVersionedLogFileName( "aa", "A" ) );

        // when
        assertThrows( NumberFormatException.class, () -> logFiles.getLogVersion( file ) );
    }

    @Test
    void isLogFile() throws IOException
    {
        LogFiles logFiles = createLogFiles();
        assertFalse( logFiles.isLogFile( new File( "aaa.tx.log" ) ) );
        assertTrue( logFiles.isLogFile( new File( "filename.0" ) ) );
        assertTrue( logFiles.isLogFile( new File( "filename.17" ) ) );
    }

    @Test
    void emptyFileWithoutEntriesDoesNotHaveThem() throws IOException
    {
        LogFiles logFiles = createLogFiles();
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        String file = getVersionedLogFileName( "1" );
        fileSystem.write( createTransactionLogFile( databaseLayout, file ) ).close();
        assertFalse( logFiles.hasAnyEntries( 1 ) );
    }

    @Test
    void fileWithoutEntriesDoesNotHaveThemIndependentlyOfItsSize() throws IOException
    {
        LogFiles logFiles = createLogFiles();
        try ( PhysicalLogVersionedStoreChannel channel = logFiles.createLogChannelForVersion( 1, () -> 1L ) )
        {
            assertThat( channel.size(), greaterThanOrEqualTo( (long) LOG_HEADER_SIZE ) );
            assertFalse( logFiles.hasAnyEntries( 1 ) );
        }
    }

    private File createTransactionLogFile( DatabaseLayout databaseLayout, String fileName )
    {
        File transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        return new File( transactionLogsDirectory, fileName );
    }

    private LogFiles createLogFiles() throws IOException
    {
        return LogFilesBuilder
                .builder( testDirectory.databaseLayout(), fileSystem )
                .withLogFileName( filename )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withLogEntryReader( new VersionAwareLogEntryReader( new TestCommandReaderFactory(), InvalidLogEntryHandler.STRICT ) )
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
