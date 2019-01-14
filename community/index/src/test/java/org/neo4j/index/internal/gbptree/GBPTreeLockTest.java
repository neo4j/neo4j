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
package org.neo4j.index.internal.gbptree;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.Race;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

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

    private final GBPTreeLock lock = new GBPTreeLock();
    private GBPTreeLock copy;

    @Rule
    public final OtherThreadRule<Void> executor = new OtherThreadRule<>();

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

    @Test( timeout = 10_000 )
    public void test_race_ULvsUL() throws Throwable
    {
        assertOnlyOneSucceeds( lock::cleanerLock, lock::cleanerLock );
    }

    @Test
    public void test_race_ULvsLU() throws Throwable
    {
        assertBothSucceeds( lock::cleanerLock, lock::writerLock );
    }

    @Test( timeout = 10_000 )
    public void test_race_ULvsLL() throws Throwable
    {
        assertOnlyOneSucceeds( lock::cleanerLock, lock::writerAndCleanerLock );
    }

    @Test( timeout = 10_000 )
    public void test_race_LUvsLU() throws Throwable
    {
        assertOnlyOneSucceeds( lock::writerLock, lock::writerLock );
    }

    @Test( timeout = 10_000 )
    public void test_race_LUvsLL() throws Throwable
    {
        assertOnlyOneSucceeds( lock::writerLock, lock::writerAndCleanerLock );
    }

    @Test( timeout = 10_000 )
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

        race.goAsync();
        while ( !(c1.lockAcquired() || c2.lockAcquired()) || !(c1.started() && c2.started()) )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 1 ) );
        }

        // then
        Pair<Boolean,Boolean> c1State = c1.state();
        Pair<Boolean,Boolean> c2State = c2.state();
        assertNotEquals( withState( "Expected exactly one to acquire lock.", c1State, c2State ), c1State.first(), c2State.first() );
        assertTrue( withState( "Expected both to be started.", c1State, c2State ), c1State.other() && c2State.other() );
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

        race.go();

        // then
        Pair<Boolean,Boolean> c1State = c1.state();
        Pair<Boolean,Boolean> c2State = c2.state();
        assertTrue( withState( "Expected both to acquire lock.", c1State, c2State ), c1State.first() && c2State.first() );
        assertTrue( withState( "Expected both to be started.", c1State, c2State ), c1State.other() && c2State.other() );
    }

    private String withState( String message, Pair<Boolean,Boolean> c1State, Pair<Boolean,Boolean> c2State )
    {
        return String.format( "%s c1.lockAcquired=%b, c1.started=%b, c2.lockAcquired=%b, c2.started=%b",
                message, c1State.first(), c1State.other(), c2State.first(), c2State.other() );
    }

    private static class LockContestant implements Runnable
    {
        private final Runnable lockAction;
        private final AtomicBoolean lockAcquired = new AtomicBoolean();
        private final AtomicBoolean started = new AtomicBoolean();

        LockContestant( Runnable lockAction )
        {
            this.lockAction = lockAction;
        }

        @Override
        public void run()
        {
            started.set( true );
            lockAction.run();
            lockAcquired.set( true );
        }

        Pair<Boolean,Boolean> state()
        {
            return Pair.of( lockAcquired(), started() );
        }

        boolean lockAcquired()
        {
            return lockAcquired.get();
        }

        boolean started()
        {
            return started.get();
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

    private void assertBlock( Runnable runLock, Runnable runUnlock ) throws Exception
    {
        Future<Object> future = executor.execute( state ->
        {
            runLock.run();
            return null;
        } );
        executor.get().waitUntilWaiting( details -> details.isAt( GBPTreeLock.class, "doLock" ) );
        runUnlock.run();
        future.get();
    }

    private void assertUU()
    {
        assertThrow( lock::writerUnlock );
        assertThrow( lock::cleanerUnlock );
        assertThrow( lock::writerAndCleanerUnlock );
    }

    private void assertUL() throws Exception
    {
        assertThrow( lock::writerUnlock );
        assertThrow( lock::writerAndCleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::cleanerLock, copy::cleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::writerAndCleanerLock, copy::cleanerUnlock );
    }

    private void assertLU() throws Exception
    {
        assertThrow( lock::cleanerUnlock );
        assertThrow( lock::writerAndCleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::writerLock, copy::writerUnlock );
    }

    private void assertLL() throws Exception
    {
        copy = lock.copy();
        assertBlock( copy::writerLock, copy::writerUnlock );
        copy = lock.copy();
        assertBlock( copy::cleanerLock, copy::cleanerUnlock );
        copy = lock.copy();
        assertBlock( copy::writerAndCleanerLock, copy::writerAndCleanerUnlock );
    }
}
