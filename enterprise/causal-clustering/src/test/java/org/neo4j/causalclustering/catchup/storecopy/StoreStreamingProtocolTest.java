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
