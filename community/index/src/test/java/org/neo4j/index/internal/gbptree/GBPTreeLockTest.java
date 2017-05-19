/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.internal.gbptree;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

public class GBPTreeLockTest
{
    // Lock can be in following states and this test verify transitions back and forth between states
    // and also verify expected behaviour after each transition.
    //            Writer   | Cleaner
    // State UU - unlocked | unlocked
    // State UL - unlocked | locked
    // State LU - locked   | unlocked
    // State LL - locked   | locked

    private GBPTreeLock lock = new GBPTreeLock();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Test
    public void test_UU_UL_UU() throws Exception
    {
        // given
        assertUU();

        // then
        lock.cleanerLock();
        assertUL();

        lock.cleanerUnlock();
        assertUU();
    }

    @Test
    public void test_UL_LL_UL() throws Exception
    {
        // given
        lock.cleanerLock();
        assertUL();

        // then
        lock.writerLock();
        assertLL();

        lock.writerUnlock();
        assertUL();
    }

    @Test
    public void test_LL_LU_LL() throws Exception
    {
        // given
        lock.writerLock();
        lock.cleanerLock();
        assertLL();

        // then
        lock.cleanerUnlock();
        assertLU();

        lock.cleanerLock();
        assertLL();
    }

    @Test
    public void test_LU_UU_LU() throws Exception
    {
        // given
        lock.writerLock();
        assertLU();

        // then
        lock.writerUnlock();
        assertUU();

        lock.writerLock();
        assertLU();
    }

    private void assertThrow( Runnable unlock )
    {
        try
        {
            unlock.run();
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
    }

    private void assertBlock( Runnable lock, Runnable unlock ) throws ExecutionException, InterruptedException
    {
        Future<?> future = executor.submit( lock );
        shouldWait( future );
        unlock.run();
        future.get();
    }

    private void shouldWait( Future<?> future )throws InterruptedException, ExecutionException
    {
        try
        {
            future.get( 200, TimeUnit.MILLISECONDS );
            fail( "Expected timeout" );
        }
        catch ( TimeoutException e )
        {
            // good
        }
    }

    private void assertUU()
    {
        assertThrow( lock::writerUnlock );
        assertThrow( lock::cleanerUnlock );
    }

    private void assertUL() throws ExecutionException, InterruptedException
    {
        assertThrow( lock::writerUnlock );
        assertBlock( lock::cleanerLock, lock::cleanerUnlock );
    }

    private void assertLU() throws ExecutionException, InterruptedException
    {
        assertBlock( lock::writerLock, lock::writerUnlock );
        assertThrow( lock::cleanerUnlock );
    }

    private void assertLL() throws ExecutionException, InterruptedException
    {
        assertBlock( lock::writerLock, lock::writerUnlock );
        assertBlock( lock::cleanerLock, lock::cleanerUnlock );
    }
}
