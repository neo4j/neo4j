/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import io.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static org.neo4j.kernel.impl.util.Cursors.rawCursorOf;

public class StoreStreamingProtocolTest
{
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private PageCache pageCache;

    @Before
    public void setup() throws IOException
    {
        pageCache = pageCacheRule.getPageCache( fs.get() );
    }

    @Test
    public void shouldStreamResources() throws Exception
    {
        // given
        StoreStreamingProtocol protocol = new StoreStreamingProtocol();
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );

        fs.mkdir( new File( "dirA" ) );
        fs.mkdir( new File( "dirB" ) );

        String[] files = new String[]{"dirA/one", "dirA/two", "dirB/one", "dirB/two", "one", "two", "three"};

        List<StoreResource> resourceList = new ArrayList<>();
        for ( String file : files )
        {
            resourceList.add( createResource( new File( file ), ThreadLocalRandom.current().nextInt( 1, 4096 ) ) );
        }
        RawCursor<StoreResource,IOException> resources = rawCursorOf( resourceList );

        // when
        protocol.stream( ctx, resources );

        // then
        InOrder inOrder = Mockito.inOrder( ctx );

        for ( StoreResource resource : resourceList )
        {
            inOrder.verify( ctx ).write( ResponseMessageType.FILE );
            inOrder.verify( ctx ).write( new FileHeader( resource.path(), resource.recordSize() ) );
            inOrder.verify( ctx ).write( new FileSender( resource ) );
        }
        verifyNoMoreInteractions( ctx );
    }

    @Test
    public void shouldBeAbleToEndWithFailure() throws Exception
    {
        // given
        StoreStreamingProtocol protocol = new StoreStreamingProtocol();
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );

        // when
        protocol.end( ctx, E_STORE_ID_MISMATCH, -1 );

        // then
        InOrder inOrder = Mockito.inOrder( ctx );
        inOrder.verify( ctx ).write( ResponseMessageType.STORE_COPY_FINISHED );
        inOrder.verify( ctx ).writeAndFlush( new StoreCopyFinishedResponse( E_STORE_ID_MISMATCH, -1 ) );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldBeAbleToEndWithSuccess() throws Exception
    {
        // given
        StoreStreamingProtocol protocol = new StoreStreamingProtocol();
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );

        // when
        int lastCommittedTxBeforeStoreCopy = 100000;
        protocol.end( ctx, StoreCopyFinishedResponse.Status.SUCCESS, lastCommittedTxBeforeStoreCopy );

        // then
        InOrder inOrder = Mockito.inOrder( ctx );
        inOrder.verify( ctx ).write( ResponseMessageType.STORE_COPY_FINISHED );
        inOrder.verify( ctx ).writeAndFlush( new StoreCopyFinishedResponse( SUCCESS, lastCommittedTxBeforeStoreCopy ) );
        inOrder.verifyNoMoreInteractions();
    }

    private StoreResource createResource( File file, int recordSize ) throws IOException
    {
        fs.create( file );
        return new StoreResource( file, file.getPath(), recordSize, pageCache, fs );
    }
}
