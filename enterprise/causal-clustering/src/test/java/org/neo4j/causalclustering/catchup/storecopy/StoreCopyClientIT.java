/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.compress.utils.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
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
import org.neo4j.collection.primitive.base.Empty;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class StoreCopyClientIT
{
    private FileSystemAbstraction fsa = new DefaultFileSystemAbstraction();
    private final AssertableLogProvider assertableLogProvider = new AssertableLogProvider( true );
    private final LogProvider logProvider =
            new DuplicatingLogProvider( assertableLogProvider, FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ) );
    private final TerminationCondition defaultTerminationCondition = TerminationCondition.CONTINUE_INDEFINITELY;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fsa );

    private StoreCopyClient subject;
    private FakeFile fileA = new FakeFile( "fileA", "This is file a content" );
    private FakeFile fileB = new FakeFile( "another-file-b", "Totally different content 123" );

    private FakeFile indexFileA = new FakeFile( "lucene", "Lucene 123" );
    private Server catchupServer;
    private TestCatchupServerHandler serverHandler;
    private File targetLocation = new File( "copyTargetLocation" );

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
        String pageCacheFileName = "bar";
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
                    protected void channelRead0( ChannelHandlerContext ctx, GetStoreFileRequest msg ) throws IOException
                    {
                        // create the files and write the given content
                        File file = new File( fileName );
                        String thisConent = contents.next();
                        writeContents( fsa, file, thisConent );
                        PageCache pageCache = neverSupportingFileOperationPageCache( StandalonePageCacheFactory.createPageCache( fsa ) );
                        PagedFile pagedFile =
                                pageCache.map( new File( pageCacheFileName ), pageCache.pageSize(), StandardOpenOption.CREATE, StandardOpenOption.WRITE );
                        try ( WritableByteChannel writableByteChannel = pagedFile.openWritableByteChannel() )
                        {
                            writableByteChannel.write( ByteBuffer.wrap( thisConent.getBytes( Charsets.UTF_8 ) ) );
                        }

                        sendFile( ctx, file, pageCache );
                        sendFile( ctx, pagedFile.file(), pageCache );
                        StoreCopyFinishedResponse.Status status =
                                contents.hasNext() ? StoreCopyFinishedResponse.Status.E_UNKNOWN : StoreCopyFinishedResponse.Status.SUCCESS;
                        new StoreFileStreamingProtocol().end( ctx, status );
                        catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                    }

                    private void sendFile( ChannelHandlerContext ctx, File file, PageCache pageCache )
                    {
                        ctx.write( ResponseMessageType.FILE );
                        ctx.write( new FileHeader( file.getName() ) );
                        ctx.writeAndFlush( new FileSender( new StoreResource( file, file.getName(), 16, pageCache, fsa ) ) ).addListener(
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
                        ctx.writeAndFlush( PrepareStoreCopyResponse.success( new File[]{new File( fileName )}, new Empty.EmptyPrimitiveLongSet(), 1 ) );
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
            File storeDir = testDirectory.makeGraphDbDir();
            PageCache pageCache = StandalonePageCacheFactory.createPageCache( fsa );
            StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( storeDir, fsa, pageCache, new Monitors() );

            // and
            subject.copyStoreFiles( addressProvider, storeId, streamToDiskProvider, () -> defaultTerminationCondition, targetLocation );

            // then
            assertEquals( fileContent( new File( storeDir, fileName ) ), finishedContent );

            // and
            PagedFile pagedFile = pageCache.map( new File( storeDir, pageCacheFileName ), pageCache.pageSize(), StandardOpenOption.READ );
            ByteBuffer buffer = ByteBuffer.wrap( new byte[finishedContent.length()] );
            try ( ReadableByteChannel readableByteChannel = pagedFile.openReadableByteChannel() )
            {
                readableByteChannel.read( buffer );
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

    private PageCache neverSupportingFileOperationPageCache( PageCache pageCache )
    {
        PageCache spy = spy( pageCache );
        when( spy.fileSystemSupportsFileOperations() ).thenReturn( false );
        return spy;
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

    private String clientFileContents( InMemoryStoreStreamProvider storeFileStreamsProvider, String filename )
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
