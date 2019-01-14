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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class PageCacheFlusherTest
{
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test( timeout = 10_000 )
    public void shouldWaitForCompletionInHalt() throws Exception
    {
        // GIVEN
        PageCache pageCache = mock( PageCache.class );
        Barrier.Control barrier = new Barrier.Control();
        doAnswer( invocation ->
        {
            barrier.reached();
            return null;
        } ).when( pageCache ).flushAndForce();
        PageCacheFlusher flusher = new PageCacheFlusher( pageCache );
        flusher.start();

        // WHEN
        barrier.await();
        Future<Object> halt = t2.execute( state ->
        {
            flusher.halt();
            return null;
        } );
        t2.get().waitUntilWaiting( details -> details.isAt( PageCacheFlusher.class, "halt" ) );
        barrier.release();

        // THEN halt call exits normally after (confirmed) ongoing flushAndForce call completed.
        halt.get();
    }

    @Test
    public void shouldExitOnErrorInHalt() throws Exception
    {
        // GIVEN
        PageCache pageCache = mock( PageCache.class );
        RuntimeException failure = new RuntimeException();
        doAnswer( invocation ->
        {
            throw failure;
        } ).when( pageCache ).flushAndForce();
        PageCacheFlusher flusher = new PageCacheFlusher( pageCache );
        flusher.run();

        // WHEN
        try
        {
            flusher.halt();
            fail();
        }
        catch ( RuntimeException e )
        {
            // THEN
            assertSame( failure, e );
        }
    }
}
