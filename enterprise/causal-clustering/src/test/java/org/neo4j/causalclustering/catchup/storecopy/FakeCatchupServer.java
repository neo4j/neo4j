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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.StrippedCatchupServer;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.mockito.Mockito.mock;

/**
 * The purpose of having a fake catchup server, that has separate code from the production catchup server is so that we have finer grained control over how it
 * behaves. Otherwise we would need to prepare difficult conditions on a production instance which could be difficult to compose or reproduce.
 */
class FakeCatchupServer extends StrippedCatchupServer
{
    private StoreCopyClientIT storeCopyClientIT;
    private final Set<FakeFile> filesystem = new HashSet<>();
    private final Set<FakeFile> indexFiles = new HashSet<>();
    private final Map<String,Integer> pathToRequestCountMapping = new HashMap<>();
    private final Log log;

    FakeCatchupServer( StoreCopyClientIT storeCopyClientIT, LogProvider logProvider )
    {
        this.storeCopyClientIT = storeCopyClientIT;
        log = logProvider.getLog( FakeCatchupServer.class );
    }

    void addFile( FakeFile fakeFile )
    {
        filesystem.add( fakeFile );
    }

    void addIndexFile( FakeFile fakeFile )
    {
        indexFiles.add( fakeFile );
    }

    int getRequestCount( String file )
    {
        return pathToRequestCountMapping.getOrDefault( file, 0 );
    }

    @Override
    protected ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol )
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
                        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                        return;
                    }
                    handleFileExists( channelHandlerContext, getStoreFileRequest.file() );
                }
                finally
                {
                    protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
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
        else if ( file.getRemainingNoResponse() > 0 )
        {
            log.info( "FakeServer not going to response for file %s", getStoreFileRequest.file() );
            file.setRemainingNoResponse( file.getRemainingNoResponse() - 1 );
            return true; // no response
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

    private boolean handleFileExists( ChannelHandlerContext channelHandlerContext, File file )
    {
        log.info( "FakeServer File %s does exist", file );
        channelHandlerContext.writeAndFlush( ResponseMessageType.FILE );
        channelHandlerContext.writeAndFlush( new FileHeader( file.getName() ) );
        StoreResource storeResource = storeResourceFromEntry( file );
        channelHandlerContext.writeAndFlush( new FileSender( storeResource ) );
        new StoreFileStreamingProtocol().end( channelHandlerContext, StoreCopyFinishedResponse.Status.SUCCESS );
        return true;
    }

    private void incrementRequestCount( File file )
    {
        String path = file.getName();
        int count = pathToRequestCountMapping.getOrDefault( path, 0 );
        pathToRequestCountMapping.put( path, count + 1 );
    }

    private StoreResource storeResourceFromEntry( File file )
    {
        file = storeCopyClientIT.testDirectory.file( file.getName() );
        return new StoreResource( file, file.getAbsolutePath(), 16, mock( PageCache.class ), storeCopyClientIT.fileSystemAbstraction );
    }

    @Override
    protected ChannelHandler getStoreListingRequestHandler( CatchupServerProtocol protocol )
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
                channelHandlerContext.writeAndFlush( PrepareStoreCopyResponse.success( files,
                        new IndexDescriptor[]{new IndexDescriptor( new LabelSchemaDescriptor( 1, 2, 3 ), IndexDescriptor.Type.GENERAL )}, transactionId ) );
                protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
            }
        };
    }

    @Override
    protected ChannelHandler getIndexRequestHandler( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<GetIndexFilesRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext channelHandlerContext, GetIndexFilesRequest snapshotRequest )
            {
                log.info( "Received request for index %s", snapshotRequest.descriptor() );
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
                    protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                }
            }
        };
    }
}
