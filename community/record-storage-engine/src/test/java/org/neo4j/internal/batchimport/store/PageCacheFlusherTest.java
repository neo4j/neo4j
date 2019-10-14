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
package org.neo4j.internal.batchimport.store;


import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ActorsExtension
class PageCacheFlusherTest
{
    @Inject
    Actor t2;

    @Test
    void shouldWaitForCompletionInHalt() throws Exception
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
        Future<Void> halt = t2.submit( flusher::halt );
        t2.untilWaitingIn( PageCacheFlusher.class.getDeclaredMethod( "halt" ) );
        barrier.release();

        // THEN halt call exits normally after (confirmed) ongoing flushAndForce call completed.
        halt.get();
    }

    @Test
    void shouldExitOnErrorInHalt() throws Exception
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
        RuntimeException e = assertThrows( RuntimeException.class, flusher::halt );
        // THEN
        assertSame( failure, e );
    }
}
