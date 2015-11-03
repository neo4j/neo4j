/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.concurrent;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompleteableFutureMapTest
{
    @Test
    public void shouldBeAbleToCompleteARegisteredFuture() throws Exception
    {
        // given
        int key = 1;
        CompleteableFutureMap<Integer, Integer> map = new CompleteableFutureMap();

        AutoCloseableFuture<Integer> future = map.createFuture( key );

        // when
        map.complete( 1, 10 );

        // then
        assertTrue( future.isDone() );
    }

    @Test
    public void shouldNotCompleteAKeyThatWasNotRegistered() throws Exception
    {
        // given
        CompletableFuture<Object> future = new CompletableFuture<>();
        CompleteableFutureMap<Integer, Integer> map = new CompleteableFutureMap();

        // when
        map.complete( 1, 10 );

        // then
        assertFalse( future.isDone() );
    }
}