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
package org.neo4j.io.pagecache.impl;

import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.StubPagedFile;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith( Theories.class )
public class PagedByteChannelsTest
{
    @DataPoint
    public static final ThrowingFunction<PagedFile,ReadableByteChannel,IOException> readable =
            PagedReadableByteChannel::new;
    @DataPoint
    public static final ThrowingFunction<PagedFile,WritableByteChannel,IOException> writable =
            PagedWritableByteChannel::new;

    @Theory
    public void mustCloseCursorOnClose(
            ThrowingFunction<PagedFile,? extends Channel,IOException> channelConstructor ) throws Exception
    {
        AtomicInteger closeCounter = new AtomicInteger();
        PagedFile pf = new StubPagedFile( PageCache.PAGE_SIZE )
        {
            @Override
            public PageCursor io( long pageId, int pf_flags ) throws IOException
            {
                return new DelegatingPageCursor( super.io( pageId, pf_flags ) )
                {
                    @Override
                    public void close()
                    {
                        super.close();
                        closeCounter.getAndIncrement();
                    }
                };
            }
        };

        channelConstructor.apply( pf ).close();
        assertThat( closeCounter.get(), is( 1 ) );
    }
}
