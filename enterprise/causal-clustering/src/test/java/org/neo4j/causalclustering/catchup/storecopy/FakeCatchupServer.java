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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.Mockito.mock;

class TestCatchupServerHandler implements CatchupServerHandler
{
    private final Set<FakeFile> filesystem = new HashSet<>();
    private final Set<FakeFile> indexFiles = new HashSet<>();
    private final Map<String,Integer> pathToRequestCountMapping = new HashMap<>();
    private final Log log;
    private TestDirectory testDirectory;
    private FileSystemAbstraction fileSystemAbstraction;

    TestCatchupServerHandler( LogProvider logProvider, TestDirectory testDirectory, FileSystemAbstraction fileSystemAbstraction )
    {
        log = logProvider.getLog( TestCatchupServerHandler.class );
        this.testDirectory = testDirectory;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    void addFile( FakeFile fakeFile )
    {
        filesystem.add( fakeFile );
    }

    void addIndexFile( FakeFile fakeFile )
    {
        indexFiles.add( fakeFile );
    }

    public int getRequestCount( String file )
    {
        return pathToRequestCountMapping.getOrDefault( file, 0 );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new SimpleChannelInboundHandler<GetStoreFileRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext channelHandlerContext, GetStoreFileRequest getStoreFileRequest )
            {
                log.info( "Received request for file %s", getStoreFileRequest.file().getName() );
                incrementRequestCount( getStoreFileRequest.file() );
                try
                {
                    if ( handleFileDoesNotExist( channelHandlerContext, getStoreFileRequest ) )
                    {
                        catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                        return;
                    }
                    handleFileExists( channelHandlerContext, getStoreFileRequest.file() );
                }
                finally
                {
                    catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                }
            }
        };
    }

    private boolean handleFileDoesNotExist( ChannelHandlerContext channelHandlerContext, GetStoreFileRequest getStoreFileRequest )
    {
        FakeFile file = findFile( filesystem, getStoreFileRequest.file().getName() );
        if ( file.getRemainingFailed() > 0 )
        {
            file.setRemainingFailed( file.getRemainingFailed() - 1 );
            log.info( "FakeServer failing for file %s", getStoreFileRequest.file() );
            failed( channelHandlerContext );
            return true;
        }
        return false;
    }

    private void failed( ChannelHandlerContext channelHandlerContext )
    {
        new StoreFileStreamingProtocol().end( channelHandlerContext, StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND );
    }

    private FakeFile findFile( Set<FakeFile> filesystem, String filename )
    {
        return filesystem.stream()
                .filter( fakeFile -> filename.equals( fakeFile.getFilename() ) )
                .findFirst()
                .orElseThrow( () -> new RuntimeException( "FakeFile should handle all cases with regards to how server should respond" ) );
    }

    private void handleFileExists( ChannelHandlerContext channelHandlerContext, File file )
    {
        log.info( "FakeServer File %s does exist", file );
        channelHandlerContext.writeAndFlush( ResponseMessageType.FILE );
        channelHandlerContext.writeAndFlush( new FileHeader( file.getName() ) );
        StoreResource storeResource = storeResourceFromEntry( file );
        channelHandlerContext.writeAndFlush( new FileSender( storeResource ) );
        new StoreFileStreamingProtocol().end( channelHandlerContext, StoreCopyFinishedResponse.Status.SUCCESS );
    }

    private void incrementRequestCount( File file )
    {
        String path = file.getName();
        int count = pathToRequestCountMapping.getOrDefault( path, 0 );
        pathToRequestCountMapping.put( path, count + 1 );
    }

    private StoreResource storeResourceFromEntry( File file )
    {
        file = testDirectory.file( file.getName() );
        return new StoreResource( file, file.getAbsolutePath(), 16, mock( PageCache.class ), fileSystemAbstraction );
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new ChannelInboundHandlerAdapter();
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new ChannelInboundHandlerAdapter();
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new SimpleChannelInboundHandler<PrepareStoreCopyRequest>()
        {

            @Override
            protected void channelRead0( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest )
            {
                channelHandlerContext.writeAndFlush( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                List<File> list = filesystem.stream().map( FakeFile::getFile ).collect( Collectors.toList() );
                File[] files = new File[list.size()];
                files = list.toArray( files );
                long transactionId = 123L;
                PrimitiveLongSet indexIds = Primitive.longSet();
                indexIds.add( 13 );
                channelHandlerContext.writeAndFlush( PrepareStoreCopyResponse.success( files, indexIds, transactionId ) );
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
            protected void channelRead0( ChannelHandlerContext channelHandlerContext, GetIndexFilesRequest snapshotRequest )
            {
                log.info( "Received request for index %s", snapshotRequest.indexId() );
                try
                {
                    for ( FakeFile indexFile : indexFiles )
                    {
                        log.info( "FakeServer File %s does exist", indexFile.getFile() );
                        channelHandlerContext.writeAndFlush( ResponseMessageType.FILE );
                        channelHandlerContext.writeAndFlush( new FileHeader( indexFile.getFile().getName() ) );
                        StoreResource storeResource = storeResourceFromEntry( indexFile.getFile() );
                        channelHandlerContext.writeAndFlush( new FileSender( storeResource ) );
                    }
                    new StoreFileStreamingProtocol().end( channelHandlerContext, StoreCopyFinishedResponse.Status.SUCCESS );
                }
                finally
                {
                    catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                }
            }
        };
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.empty();
    }

    public StoreId getStoreId()
    {
        return new StoreId( 1, 2, 3, 4 );
    }
}
