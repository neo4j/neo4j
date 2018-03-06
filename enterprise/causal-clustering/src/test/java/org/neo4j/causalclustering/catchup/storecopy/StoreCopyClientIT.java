/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;

public class StoreCopyClientIT
{
    FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
    private final LogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );
    private final TerminationCondition defaultTerminationCondition = TerminationCondition.CONTINUE_INDEFINITELY;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemAbstraction );

    private FakeCatchupServer catchupServerRule;

    private StoreCopyClient subject;
    private FakeFile fileA = new FakeFile( "fileA", "This is file a content" );
    private FakeFile fileB = new FakeFile( "another-file-b", "Totally different content 123" );

    private FakeFile indexFileA = new FakeFile( "lucene", "Lucene 123" );

    private static void writeContents( FileSystemAbstraction fileSystemAbstraction, File file, String contents )
    {
        byte[] bytes = contents.getBytes();
        try ( StoreChannel storeChannel = fileSystemAbstraction.create( file ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Before
    public void setup()
    {
        catchupServerRule = new FakeCatchupServer( this, logProvider );
        catchupServerRule.addFile( fileA );
        catchupServerRule.addFile( fileB );
        catchupServerRule.addIndexFile( indexFileA );
        writeContents( fileSystemAbstraction, relative( fileA.getFilename() ), fileA.getContent() );
        writeContents( fileSystemAbstraction, relative( fileB.getFilename() ), fileB.getContent() );
        writeContents( fileSystemAbstraction, relative( indexFileA.getFilename() ), indexFileA.getContent() );

        CatchUpClient catchUpClient =
                new CatchUpClient( logProvider, Clock.systemDefaultZone(), 2000, new Monitors(), VoidPipelineWrapperFactory.VOID_WRAPPER );
        catchUpClient.start();
        subject = new StoreCopyClient( catchUpClient, logProvider );
    }

    @After
    public void shutdown()
    {
        catchupServerRule.stop();
    }

    @Test
    public void canPerformCatchup() throws StoreCopyFailedException
    {
        // given remote node has a store
        catchupServerRule.before(); // assume it is running
        catchupServerRule.start();

        // and local client has a store
        InMemoryFileSystemStream storeFileStream = new InMemoryFileSystemStream();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( catchupServerRule.getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, catchupServerRule.getStoreId(), storeFileStream, () -> defaultTerminationCondition );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename(), indexFileA.getFilename() ) );
        assertEquals( expectedFiles, storeFileStream.filesystem.keySet() );
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( storeFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( storeFileStream, fileB.getFilename() ) );
    }

    @Test
    public void failedFileCopyShouldRetry() throws StoreCopyFailedException
    {
        // given a file will fail twice before succeeding
        fileB.setRemainingFailed( 2 );

        // and remote node has a store
        catchupServerRule.before(); // assume it is running
        catchupServerRule.start();

        // and local client has a store
        InMemoryFileSystemStream clientStoreFileStream = new InMemoryFileSystemStream();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( catchupServerRule.getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, catchupServerRule.getStoreId(), clientStoreFileStream, () -> defaultTerminationCondition );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename(), indexFileA.getFilename() ) );
        assertEquals( expectedFiles, clientStoreFileStream.filesystem.keySet() );

        // and
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( clientStoreFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( clientStoreFileStream, fileB.getFilename() ) );

        // and verify server had exactly 2 failed calls before having a 3rd succeeding request
        assertEquals( 3, catchupServerRule.getRequestCount( fileB.getFilename() ) );

        // and verify server had exactly 1 call for all other files
        assertEquals( 1, catchupServerRule.getRequestCount( fileA.getFilename() ) );
    }

    @Test
    public void reconnectingWorks() throws StoreCopyFailedException
    {
        // given a remote catchup will fail midway
        catchupServerRule.before();
        catchupServerRule.start();

        // and local client has a store
        InMemoryFileSystemStream storeFileStream = new InMemoryFileSystemStream();

        // and file B is broken once (after retry it works)
        fileB.setRemainingNoResponse( 1 );

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( catchupServerRule.getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, catchupServerRule.getStoreId(), storeFileStream, () -> defaultTerminationCondition );

        // then the catchup is possible to complete
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( storeFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( storeFileStream, fileB.getFilename() ) );

        // and verify server had exactly 2 calls for failing file
        assertEquals( 2, catchupServerRule.getRequestCount( fileB.getFilename() ) );

        // and verify server had exactly 1 call for all other files
        assertEquals( 1, catchupServerRule.getRequestCount( fileA.getFilename() ) );
    }

    private static AdvertisedSocketAddress from( int port )
    {
        return new AdvertisedSocketAddress( "localhost", port );
    }

    private File relative( String filename )
    {
        return testDirectory.file( filename );
    }

    private String fileContent( File file )
    {
        return fileContent( file, fileSystemAbstraction );
    }

    private static StringBuilder serverFileContentsStringBuilder( File file, FileSystemAbstraction fileSystemAbstraction )
    {
        try ( StoreChannel storeChannel = fileSystemAbstraction.open( file, OpenMode.READ ) )
        {
            int MAX_BUFFER_SIZE = 100;
            ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[MAX_BUFFER_SIZE] );
            StringBuilder stringBuilder = new StringBuilder();
            Predicate<Integer> inRange = betweenZeroAndRange( MAX_BUFFER_SIZE );
            Supplier<Integer> readNext = unchecked( () -> storeChannel.read( byteBuffer ) );
            for ( int readBytes = readNext.get(); inRange.test( readBytes ); readBytes = readNext.get() )
            {
                for ( byte index = 0; index < readBytes; index++ )
                {
                    char actual = (char) byteBuffer.get( index );
                    stringBuilder.append( actual );
                }
            }
            return stringBuilder;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    static String fileContent( File file, FileSystemAbstraction fileSystemAbstraction )
    {
        return serverFileContentsStringBuilder( file, fileSystemAbstraction ).toString();
    }

    private static Supplier<Integer> unchecked( ThrowingSupplier<Integer,?> throwableSupplier )
    {
        return () ->
        {
            try
            {
                return throwableSupplier.get();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        };
    }

    private static Predicate<Integer> betweenZeroAndRange( int RANGE )
    {
        return bytes -> bytes > 0 && bytes <= RANGE;
    }

    private String clientFileContents( InMemoryFileSystemStream storeFileStreams, String filename )
    {
        return storeFileStreams.filesystem.get( filename ).toString();
    }
}
