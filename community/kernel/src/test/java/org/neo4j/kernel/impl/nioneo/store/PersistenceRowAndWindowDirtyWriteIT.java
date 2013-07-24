/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;

@RunWith(SubProcessTestRunner.class)
@ForeignBreakpoints({
        @ForeignBreakpoints.BreakpointDef(type = "org.neo4j.kernel.impl.nioneo.store.PersistenceRow",
                method = "lock", on = BreakPoint.Event.ENTRY ) })
/*
 * If this test hangs, it's because a deadlock has happened between the threads. If, for example, the refreshBricks
 * does not happen in theOverwrittenOne then it will deadlock with the evil one. Look for that sort of thing if
 * this test starts breaking.
 */
@Ignore("Fix pending")
public class PersistenceRowAndWindowDirtyWriteIT
{
    @Test
    @EnabledBreakpoints( { "waitForFirstWriterToWrite", "waitForBreakingToAcquire" } )
    public void theTest() throws Exception
    {
        /*
         * If you want to change or fix this test, you sort of have to read what follows.
         *
         * I can hear you, exasperated, giving up all hope, thinking "Oh noes! Another one of those tests".
         * I know.
         * I understand.
         * I sympathise.
         * After all, I wrote it.
         * So let me, dear reader, to try and show you what is going on here.
         * The bug is the following: A thread, let's call it theTriggeringOne, will read in a record through a
         * PersistentRow. It locks it, it writes stuff to it. In the mean time, before theTriggeringOne releases the lock,
         * another thread, we name it affectionately theEvilOne, will try to *read* (hence, no locks on the object)
         * the same record. It, too, will attempt to go to the same PersistentRow, stored now in the row map in
         * PersistenceWindowPool. It will mark it, but not lock it, because theTriggeringOne still holds the lock. Now,
         * theTriggeringOne releases the lock. It does not write out the changes though, because well, theEvilOne has it
         * already marked.
         * A third thread comes in now, which we'll call theOverwrittenOne. That one grabs a brick for the same position,
         * but in doing so it triggers a refresh of the bricks (actually, any thread could have done that, but this way
         * we don't need to introduce a 4th thread). That leave the contended file position being now memory mapped. Note
         * how we are suddenly in a world of crap, since we have one PersistenceWindow *and* a PersistenceRow, which
         * btw is dirty, for the same file position. I assume you can guess the rest. theOverwrittenOne will write
         * something, release and flush it, and theEvilOne will finish its read operation right after that, release
         * the row and then its contents will be written out, overwriting theOverwrittenOne's changes.
         *
         * Now, the test below reproduces the exact scenario above. The thread names are the same. Synchronization is
         * achieved mostly through latches. There is one point though where an external breakpoint is required, and
         * that is between theEvilOne marks and attempts to lock the row that theTriggeringOne already has locked.
         * The breakpoint is added on PersistenceRow.lock() and what it does is it resumes theEvilOne which is
         * suspended by theTriggeringOne on its startup. That's what the 3 breakpoint handlers do - one is for suspending
         * theEvilOne on startup, the other is for pausing theTriggeringOne before releasing the lock and the
         * lock handler allows theTriggeringOne to continue and release the lock when theEvilOne marks the row.
         *
         * The rest is details explained inline.
         */
        File dataFile = TargetDirectory.forTest( getClass() ).file( "dataFile" );
        FileChannel dataFileChannel = new RandomAccessFile( dataFile, "rw" ).getChannel();
        final PersistenceWindowPool pool =
                new PersistenceWindowPool( dataFile, 4, // record size
                        dataFileChannel,
                        50000, // memory available, must be at least that big for 2 windows to exist
                        true, // memory map?
                        false, // read only?
                        StringLogger.DEV_NULL );

        Thread theTriggeringOne = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                /*
                 * When we attempt to grab a brick for the first time, we always immediately memory map it if there is
                 * enough memory. But, we don't want to grab a window for position 1. We need a row. So, we ask for
                 * a position that is in the second brick of the pool (we create two in total, the first has our
                 * contended position, the other is the dummy we create now). That will cause a copy of the brick
                 * array which will get rid of the mapping for position 1.
                 * Look into PersistenceWindowPool.acquire() and PWP.expandBricks() for a better understanding.
                 */
                pool.release( pool.acquire( 13, OperationType.READ ) );
                /*
                 * For the bug to be triggered, we'll need the position to be memory mapped. That happens only if
                 * enough hits have happened on the brick that covers that position. We will ask for that position
                 * 3 times in this test, the third of which must return a window and the first two a row. We set things
                 * up so that this happens here
                 */
                for ( int i = 0; i < PersistenceWindowPool.REFRESH_BRICK_COUNT - 3; i++ )
                {
                    pool.release( pool.acquire( 1, OperationType.READ ) );
                }
                // This will grab and write lock a row, marking it as dirty
                PersistenceRow row = (PersistenceRow) pool.acquire( 1, OperationType.WRITE );
                row.getOffsettedBuffer( 1 ).put( new byte[]{1, 2, 3, 4} );
                // Do not release, theEvilOne must mark it first
                waitForBreakingToAcquire();
                // Release, now it's in the hands of theEvilOne
                pool.release( row );
                // done
            }
        });

        final CountDownLatch theOverwrittenOneHasWrittenItsChanges = new CountDownLatch( 1 );
        final CountDownLatch theBreakingOneHasLockedTheRow = new CountDownLatch( 1 );

        Thread theEvilOne = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    // We will lock this row, but we must wait for theTriggeringOne to write to it
                    waitForFirstWriterToWrite();
                    /*
                     * This would deadlock, since theTriggeringOne waits for us to grab it before releasing, and
                     * we cannot grab it unless theTriggeringOne releases it. This is broken by the external
                     * breakpoint on PersistenceRow.lock()
                     */
                    PersistenceRow row = (PersistenceRow) pool.acquire( 1, OperationType.READ );
                    // And we allow theOverwrittenOne to refresh bricks and read in the memory mapped buffer
                    theBreakingOneHasLockedTheRow.countDown();
                    // Wait for it to write
                    theOverwrittenOneHasWrittenItsChanges.await();
                    // And we broke it, since releasing this row will overwrite whatever theOverwrittenOne wrote
                    pool.release( row );
                }
                catch( Exception e)
                {
                    throw new RuntimeException( e );
                }
            }
        });

        Thread theOverwrittenOne = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    // Wait for theEvilOne to grab the lock on the row
                    theBreakingOneHasLockedTheRow.await();
                    /*
                     * Because of the setup theTriggeringOne did, this will do a refreshBricks() and read in a
                     * LockableWindow instead of a PersistenceRow.
                     */
                    LockableWindow window = (LockableWindow) pool.acquire( 1, OperationType.WRITE );
                    // Write the new stuff in - that will be overwritten by the flush when theEvilOne releases
                    window.getOffsettedBuffer( 1 ).put( new byte[]{5, 6, 7, 8} );
                    // Release the lock - not really necessary, just good form
                    pool.release( window );
                    // Allow theEvilOne to continue
                    theOverwrittenOneHasWrittenItsChanges.countDown();
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        });

        theEvilOne.start();
        theOverwrittenOne.start();
        theTriggeringOne.start();

        theEvilOne.join();
        theTriggeringOne.join();
        theOverwrittenOne.join();

        byte[] finalResult = new byte[4];
        pool.acquire( 1, OperationType.READ ).getOffsettedBuffer( 1 ).get( finalResult );
        /*
         * This is the assertion. The content should be the ones that theOverwrittenOne wrote, as it locked position
         * 1 after theTriggeringOne had released it. All high level locks have been respected, but still the thread
         * that has happened-before the last lock grab for position 1 is applied last.
         */
        assertTrue( Arrays.toString( finalResult ), Arrays.equals( new byte[]{5, 6, 7, 8}, finalResult ) );
        pool.close();
    }

    // Debug stuff

    private static DebuggedThread theTriggeringOne;
    private static DebuggedThread theEvilOne;

    @BreakpointTrigger("waitForFirstWriterToWrite")
    public void waitForFirstWriterToWrite()
    {}

    @BreakpointHandler("waitForFirstWriterToWrite")
    public static void waitForFirstWriterToWriteHandler( BreakPoint self, DebugInterface di )
    {
        theEvilOne = di.thread().suspend( null );
        self.disable();
    }

    @BreakpointTrigger("waitForBreakingToAcquire")
    public void waitForBreakingToAcquire()
    {}

    @BreakpointHandler("waitForBreakingToAcquire")
    public static void waitForBreakingToAcquireHandler( BreakPoint self, DebugInterface di,
                                                        @BreakpointHandler("lock") BreakPoint onWindowLock )
    {
        theEvilOne.resume();
        theTriggeringOne = di.thread().suspend( null );
        onWindowLock.enable();
        self.disable();
    }

    @BreakpointHandler( "lock" )
    public static void lockHandler( BreakPoint self, DebugInterface di )
    {
        theTriggeringOne.resume();
        self.disable();
    }
}
