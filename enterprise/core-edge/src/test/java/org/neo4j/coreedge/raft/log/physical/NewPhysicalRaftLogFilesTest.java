/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log.physical;

public class NewPhysicalRaftLogFilesTest
{
//    private final FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
//    private final File tmpDirectory = new File( "." );
//
//    @Test
//    public void shouldGetTheFileNameForAGivenVersion()
//    {
//        // given
//        final ToBeRemoved files = new ToBeRemoved( tmpDirectory, fs, mock( ChannelMarshal.class ),
//                mock( ToBeRemoved3.class ) );
//        final int version = 12;
//
//        // when
//        final File versionFileName = files.getLogFileForVersion( version );
//
//        // then
//        final File expected = new File( tmpDirectory, BASE_FILE_NAME + DEFAULT_VERSION_SUFFIX + version );
//        assertEquals( expected, versionFileName );
//    }
//
//    @Test
//    public void shouldBeAbleToRetrieveTheHighestLogVersionWithOtherFilesPresent() throws Exception
//    {
//        // given
//        fs.mkdir( tmpDirectory );
//        ToBeRemoved files = new ToBeRemoved( tmpDirectory, fs, mock( ChannelMarshal.class ),
//                new ToBeRemoved3() );
//
//        writeLogHeader( fs, new File( tmpDirectory, BASE_FILE_NAME + DEFAULT_VERSION_SUFFIX + "1" ), 1, -1 );
//        writeLogHeader( fs, new File( tmpDirectory, "unknown" + DEFAULT_VERSION_SUFFIX + "4" ), 1, -1 );
//        writeLogHeader( fs, new File( tmpDirectory, BASE_FILE_NAME + DEFAULT_VERSION_SUFFIX + "3" ), 3, -1 );
//        writeLogHeader( fs, new File( tmpDirectory, BASE_FILE_NAME ), 1, -1 );
//
//        files.init();
//
//        // when
//        final long highestLogVersion = files.getHighestLogVersion();
//
//        // then
//        assertEquals( 3, highestLogVersion );
//    }
//
//    @Test
//    public void shouldBeAbleToRetrieveTheHighestLogVersionWithEmptyFilePresent() throws Exception
//    {
//        // given
//        fs.mkdir( tmpDirectory );
//        ToBeRemoved files = new ToBeRemoved( tmpDirectory, fs, mock( ChannelMarshal.class ),
//                new ToBeRemoved3()  );
//
//        writeLogHeader( fs, new File( tmpDirectory, BASE_FILE_NAME + DEFAULT_VERSION_SUFFIX + "1" ), 1, -1 );
//        writeLogHeader( fs, new File( tmpDirectory, BASE_FILE_NAME + DEFAULT_VERSION_SUFFIX + "3" ), 3, 42 );
//        File emptyFile = new File( tmpDirectory, BASE_FILE_NAME + DEFAULT_VERSION_SUFFIX + "4" );
//        fs.create( emptyFile );
//
//        files.init();
//
//        // when
//        final long highestLogVersion = files.getHighestLogVersion();
//
//        // then
//        assertEquals( 4, highestLogVersion );
//        LogHeader logHeader = readLogHeader( fs, emptyFile );
//        assertEquals( 4, logHeader.logVersion );
//        assertEquals( 42, logHeader.lastCommittedTxId );
//    }
//
//    @Test
//    public void shouldFindTheVersionBasedOnTheFilename()
//    {
//        // given
//        final File file =
//                new File( "v" + DEFAULT_VERSION_SUFFIX + DEFAULT_VERSION_SUFFIX + DEFAULT_VERSION_SUFFIX + "2" );
//
//        // when
//        // Get version based on the name
//        long logVersion = ToBeRemoved.getLogVersion( file.getName() );
//
//        // then
//        assertEquals( 2, logVersion );
//    }
//
//    @Test
//    public void shouldThrowIfThereIsNoVersionInTheFileName()
//    {
//        // given
//        final File file = new File( "wrong" );
//
//        // when
//        try
//        {
//            // Get version based on the name
//            ToBeRemoved.getLogVersion( file.getName() );
//            fail( "should have thrown" );
//        }
//        catch ( RuntimeException ex )
//        {
//            assertEquals( "Invalid log file '" + file.getName() + "'", ex.getMessage() );
//        }
//    }
//
//    @Test(expected = NumberFormatException.class)
//    public void shouldThrowIfVersionIsNotANumber()
//    {
//        // given
//        final File file = new File( "aa" + DEFAULT_VERSION_SUFFIX + "A" );
//
//        // when
//        // Get version based on the name
//        ToBeRemoved.getLogVersion( file.getName() );
//    }
//
//    @Test
//    public void shouldIncrementVersionOnRegisterNewVersion() throws Exception
//
//    {
//        // given
//        long prevLogIndex = 123L;
//        ToBeRemoved files = new ToBeRemoved( tmpDirectory, fs, mock( ChannelMarshal.class ),
//                new ToBeRemoved3() );
//
//        // when
//        long initialVersion = files.getHighestLogVersion();
//        // then
//        assertEquals( 0, initialVersion );
//
//        // when
//        long newVersion = files.registerNextVersion( prevLogIndex );
//
//        // then
//        assertEquals( initialVersion + 1, newVersion );
//        assertEquals( initialVersion + 1, files.getHighestLogVersion() );
//    }
}
