/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.compress.utils.Charsets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.catchup.CatchupClientBuilder;
import org.neo4j.causalclustering.catchup.CatchupServerBuilder;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StoreCopyClientIT
{
    private final FileSystemAbstraction fsa = new DefaultFileSystemAbstraction();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory( fsa );
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private final AssertableLogProvider assertableLogProvider = new AssertableLogProvider( true );
    private final TerminationCondition defaultTerminationCondition = TerminationCondition.CONTINUE_INDEFINITELY;
    private final FakeFile fileA = new FakeFile( "fileA", "This is file a content" );
    private final FakeFile fileB = new FakeFile( "another-file-b", "Totally different content 123" );
    private final FakeFile indexFileA = new FakeFile( "lucene", "Lucene 123" );
    private final File targetLocation = new File( "copyTargetLocation" );
    private LogProvider logProvider;
    private StoreCopyClient subject;
    private Server catchupServer;
    private TestCatchupServerHandler serverHandler;

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
    public void setup() throws Throwable
    {
        logProvider = new DuplicatingLogProvider( assertableLogProvider, FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ) );
        serverHandler = new TestCatchupServerHandler( logProvider, testDirectory, fsa );
        serverHandler.addFile( fileA );
        serverHandler.addFile( fileB );
        serverHandler.addIndexFile( indexFileA );
        writeContents( fsa, relative( fileA.getFilename() ), fileA.getContent() );
        writeContents( fsa, relative( fileB.getFilename() ), fileB.getContent() );
        writeContents( fsa, relative( indexFileA.getFilename() ), indexFileA.getContent() );

        ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", PortAuthority.allocatePort() );
        catchupServer = new CatchupServerBuilder( serverHandler ).listenAddress( listenAddress ).build();
        catchupServer.start();

        CatchUpClient catchUpClient = new CatchupClientBuilder().build();
        catchUpClient.start();

        ConstantTimeTimeoutStrategy storeCopyBackoffStrategy = new ConstantTimeTimeoutStrategy( 1, TimeUnit.MILLISECONDS );

        Monitors monitors = new Monitors();
        subject = new StoreCopyClient( catchUpClient, monitors, logProvider, storeCopyBackoffStrategy );
    }

    @After
    public void shutdown() throws Throwable
    {
        catchupServer.stop();
    }

    @Test
    public void canPerformCatchup() throws StoreCopyFailedException, IOException
    {
        // given local client has a store
        InMemoryStoreStreamProvider storeFileStream = new InMemoryStoreStreamProvider();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( catchupServer.address().getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, serverHandler.getStoreId(), storeFileStream, () -> defaultTerminationCondition, targetLocation );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename(), indexFileA.getFilename() ) );
        assertEquals( expectedFiles, storeFileStream.fileStreams().keySet() );
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( storeFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( storeFileStream, fileB.getFilename() ) );
    }

    @Test
    public void failedFileCopyShouldRetry() throws StoreCopyFailedException, IOException
    {
        // given a file will fail twice before succeeding
        fileB.setRemainingFailed( 2 );

        // and remote node has a store
        // and local client has a store
        InMemoryStoreStreamProvider clientStoreFileStream = new InMemoryStoreStreamProvider();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( from( catchupServer.address().getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, serverHandler.getStoreId(), clientStoreFileStream, () -> defaultTerminationCondition, targetLocation );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename(), indexFileA.getFilename() ) );
        assertEquals( expectedFiles, clientStoreFileStream.fileStreams().keySet() );

        // and
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( clientStoreFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( clientStoreFileStream, fileB.getFilename() ) );

        // and verify server had exactly 2 failed calls before having a 3rd succeeding request
        assertEquals( 3, serverHandler.getRequestCount( fileB.getFilename() ) );

        // and verify server had exactly 1 call for all other files
        assertEquals( 1, serverHandler.getRequestCount( fileA.getFilename() ) );
    }

    @Test
    public void shouldNotAppendToFileWhenRetryingWithNewFile() throws Throwable
    {
        // given
        String fileName = "foo";
        String copyFileName = "bar";
        String unfinishedContent = "abcd";
        String finishedContent = "abcdefgh";
        Iterator<String> contents = Iterators.iterator( unfinishedContent, finishedContent );

        // and
        TestCatchupServerHandler halfWayFailingServerhandler = new TestCatchupServerHandler( logProvider, testDirectory, fsa )
        {
            @Override
            public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
            {
                return new SimpleChannelInboundHandler<GetStoreFileRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, GetStoreFileRequest msg )
                    {
                        // create the files and write the given content
                        File file = new File( fileName );
                        File fileCopy = new File( copyFileName );
                        String thisConent = contents.next();
                        writeContents( fsa, file, thisConent );
                        writeContents( fsa, fileCopy, thisConent );

                        sendFile( ctx, file );
                        sendFile( ctx, fileCopy );
                        StoreCopyFinishedResponse.Status status =
                                contents.hasNext() ? StoreCopyFinishedResponse.Status.E_UNKNOWN : StoreCopyFinishedResponse.Status.SUCCESS;
                        new StoreFileStreamingProtocol().end( ctx, status );
                        catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                    }

                    private void sendFile( ChannelHandlerContext ctx, File file )
                    {
                        ctx.write( ResponseMessageType.FILE );
                        ctx.write( new FileHeader( file.getName() ) );
                        ctx.writeAndFlush( new FileSender( new StoreResource( file, file.getName(), 16, fsa ) ) ).addListener(
                                future -> fsa.deleteFile( file ) );
                    }
                };
            }

            @Override
            public ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol )
            {
                return new SimpleChannelInboundHandler<PrepareStoreCopyRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, PrepareStoreCopyRequest msg )
                    {
                        ctx.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                        ctx.writeAndFlush( PrepareStoreCopyResponse.success( new File[]{new File( fileName )}, LongSets.immutable.empty(), 1 ) );
                        catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                    }
                };
            }

            @Override
            public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol catchupServerProtocol )
            {
                return new SimpleChannelInboundHandler<GetIndexFilesRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, GetIndexFilesRequest msg )
                    {
                        throw new IllegalStateException( "There should not be any index requests" );
                    }
                };
            }
        };

        Server halfWayFailingServer = null;

        try
        {
            // when
            ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", PortAuthority.allocatePort() );
            halfWayFailingServer = new CatchupServerBuilder( halfWayFailingServerhandler ).listenAddress( listenAddress ).build();
            halfWayFailingServer.start();

            CatchupAddressProvider addressProvider =
                    CatchupAddressProvider.fromSingleAddress( new AdvertisedSocketAddress( listenAddress.getHostname(), listenAddress.getPort() ) );

            StoreId storeId = halfWayFailingServerhandler.getStoreId();
            File databaseDir = testDirectory.databaseDir();
            StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( databaseDir, fsa, new Monitors() );

            // and
            subject.copyStoreFiles( addressProvider, storeId, streamToDiskProvider, () -> defaultTerminationCondition, targetLocation );

            // then
            assertEquals( fileContent( new File( databaseDir, fileName ) ), finishedContent );

            // and
            File fileCopy = new File( databaseDir, copyFileName );

            ByteBuffer buffer = ByteBuffer.wrap( new byte[finishedContent.length()] );
            try ( StoreChannel storeChannel = fsa.create( fileCopy ) )
            {
                storeChannel.read( buffer );
            }
            assertEquals( finishedContent, new String( buffer.array(), Charsets.UTF_8 ) );
        }
        finally
        {
            halfWayFailingServer.stop();
            halfWayFailingServer.shutdown();
        }
    }

    @Test
    public void shouldLogConnetionRefusedMessage()
    {
        InMemoryStoreStreamProvider clientStoreFileStream = new InMemoryStoreStreamProvider();
        int port = PortAuthority.allocatePort();
        try
        {
            subject.copyStoreFiles( new CatchupAddressProvider()
            {
                @Override
                public AdvertisedSocketAddress primary()
                {
                    return from( catchupServer.address().getPort() );
                }

                @Override
                public AdvertisedSocketAddress secondary()
                {

                    return new AdvertisedSocketAddress( "localhost", port );
                }
            }, serverHandler.getStoreId(), clientStoreFileStream, Once::new, targetLocation );
            fail();
        }
        catch ( StoreCopyFailedException e )
        {
            assertableLogProvider.assertContainsExactlyOneMessageMatching(
                    both( startsWith( "Connection refused:" ) ).and( containsString( "localhost/127.0.0.1:" + port ) ) );
        }
    }

    @Test
    public void shouldLogUpstreamIssueMessage()
    {
        InMemoryStoreStreamProvider clientStoreFileStream = new InMemoryStoreStreamProvider();
        CatchupAddressResolutionException catchupAddressResolutionException = new CatchupAddressResolutionException( new MemberId( UUID.randomUUID() ) );
        try
        {
            subject.copyStoreFiles( new CatchupAddressProvider()
            {
                @Override
                public AdvertisedSocketAddress primary()
                {
                    return from( catchupServer.address().getPort() );
                }

                @Override
                public AdvertisedSocketAddress secondary() throws CatchupAddressResolutionException
                {
                    throw catchupAddressResolutionException;
                }
            }, serverHandler.getStoreId(), clientStoreFileStream, Once::new, targetLocation );
            fail();
        }
        catch ( StoreCopyFailedException e )
        {
            assertableLogProvider.assertContainsExactlyOneMessageMatching( startsWith( "Unable to resolve address for" ) );
            assertableLogProvider.assertLogStringContains(catchupAddressResolutionException.getMessage() );
        }
    }

    private static AdvertisedSocketAddress from( int port )
    {
        return new AdvertisedSocketAddress( "localhost", port );
    }

    private File relative( String filename )
    {
        return testDirectory.file( filename );
    }

    private String fileContent( File file ) throws IOException
    {
        return fileContent( file, fsa );
    }

    static String fileContent( File file, FileSystemAbstraction fsa ) throws IOException
    {
        int chunkSize = 128;
        StringBuilder stringBuilder = new StringBuilder();
        try ( Reader reader = fsa.openAsReader( file, Charsets.UTF_8 ) )
        {
            CharBuffer charBuffer = CharBuffer.wrap( new char[chunkSize] );
            while ( reader.read( charBuffer ) != -1 )
            {
                charBuffer.flip();
                stringBuilder.append( charBuffer );
                charBuffer.clear();
            }
        }
        return stringBuilder.toString();
    }

    private static String clientFileContents( InMemoryStoreStreamProvider storeFileStreamsProvider, String filename )
    {
        return storeFileStreamsProvider.fileStreams().get( filename ).toString();
    }

    private static class Once implements TerminationCondition
    {
        @Override
        public void assertContinue() throws StoreCopyFailedException
        {
            throw new StoreCopyFailedException( "One try only" );
        }
    }
}
