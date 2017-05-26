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

import org.neo4j.test.Race;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
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
    private GBPTreeLock copy;
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

    @Test
    public void test_UU_LL_UU() throws Exception
    {
        // given
        assertUU();

        // then
        lock.writerAndCleanerLock();
        assertLL();

        lock.writerAndCleanerUnlock();
        assertUU();
    }

    @Test
    public void test_race_ULvsUL() throws Throwable
    {
        assertOnlyOneSucceeds( lock::cleanerLock, lock::cleanerLock );
    }

    @Test
    public void test_race_ULvsLU() throws Throwable
    {
        assertBothSucceeds( lock::cleanerLock, lock::writerLock );
    }

    @Test
    public void test_race_ULvsLL() throws Throwable
    {
        assertOnlyOneSucceeds( lock::cleanerLock, lock::writerAndCleanerLock );
    }

    @Test
    public void test_race_LUvsLU() throws Throwable
    {
        assertOnlyOneSucceeds( lock::writerLock, lock::writerLock );
    }

    @Test
    public void test_race_LUvsLL() throws Throwable
    {
        assertOnlyOneSucceeds( lock::writerLock, lock::writerAndCleanerLock );
    }

    @Test
    public void test_race_LLvsLL() throws Throwable
    {
        assertOnlyOneSucceeds( lock::writerAndCleanerLock, lock::writerAndCleanerLock );
    }

    private void assertOnlyOneSucceeds( Runnable lockAction1, Runnable lockAction2 ) throws Throwable
    {
        assertUU();
        Race race = new Race();
        LockContestant c1 = new LockContestant( lockAction1 );
        LockContestant c2 = new LockContestant( lockAction2 );

        // when
        race.addContestant( c1 );
        race.addContestant( c2 );
        try
        {
            race.go( 10, TimeUnit.MILLISECONDS );
            fail( "One of the contestants should be blocked" );
        }
        catch ( TimeoutException throwable )
        {
            // This is fine. We expect one of the contestants to block
        }

        // then
        assertNotEquals( c1.lockAcquired, c2.lockAcquired );
        assertNotEquals( c1.blocked, c2.blocked );
    }

    private void assertBothSucceeds( Runnable lockAction1, Runnable lockAction2 ) throws Throwable
    {
        assertUU();
        Race race = new Race();
        LockContestant c1 = new LockContestant( lockAction1 );
        LockContestant c2 = new LockContestant( lockAction2 );

        // when
        race.addContestant( c1 );
        race.addContestant( c2 );

        race.go( 10, TimeUnit.MILLISECONDS );

        // then
        assertTrue( c1.lockAcquired );
        assertTrue( c2.lockAcquired );
        assertFalse( c1.blocked );
        assertFalse( c2.blocked );
    }

    private class LockContestant implements Runnable
    {
        private final Runnable lockAction;
        private boolean lockAcquired;
        private boolean blocked;

        LockContestant( Runnable lockAction )
        {
            this.lockAction = lockAction;
        }

        @Override
        public void run()
        {
            blocked = true;
            lockAction.run();
            lockAcquired = true;
            blocked = false;
        }
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

    private void assertBlock( Runnable runLock, Runnable runUnlock ) throws ExecutionException, InterruptedException
    {
        Future<?> future = executor.submit( runLock );
        shouldWait( future );
        runUnlock.run();
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
        assertThrow( lock::writerAndCleanerUnlock );
    }

    private void assertUL() throws ExecutionException, InterruptedException
    {
        assertThrow( lock::writerUnlock );
        assertThrow( lock::writerAndCleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::cleanerLock, copy::cleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::writerAndCleanerLock, copy::cleanerUnlock );
    }

    private void assertLU() throws ExecutionException, InterruptedException
    {
        assertThrow( lock::cleanerUnlock );
        assertThrow( lock::writerAndCleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::writerLock, copy::writerUnlock );
    }

    private void assertLL() throws ExecutionException, InterruptedException
    {
        copy = lock.copy();
        assertBlock( copy::writerLock, copy::writerUnlock );
        copy = lock.copy();
        assertBlock( copy::cleanerLock, copy::cleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::writerAndCleanerLock, copy::writerAndCleanerUnlock );
    }
}
