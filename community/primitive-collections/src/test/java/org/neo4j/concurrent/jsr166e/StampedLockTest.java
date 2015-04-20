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
// Relicensed from public domain. Original license:
/*
 * Written by Doug Lea and Martin Buchholz
 * with assistance from members of JCP JSR-166 Expert Group and
 * released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

// Based on revision 1.1: http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/test/tck-jsr166e/StampedLockTest.java?revision=1.1&view=markup

package org.neo4j.concurrent.jsr166e;

import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StampedLockTest extends JSR166TestCase
{

    /**
     * A runnable calling writeLockInterruptibly
     */
    class InterruptibleLockRunnable extends CheckedRunnable {
        final StampedLock lock;
        InterruptibleLockRunnable(StampedLock l) { lock = l; }
        public void realRun() throws InterruptedException {
            lock.writeLockInterruptibly();
        }
    }

    /**
     * A runnable calling writeLockInterruptibly that expects to be
     * interrupted
     */
    class InterruptedLockRunnable extends CheckedInterruptedRunnable {
        final StampedLock lock;
        InterruptedLockRunnable(StampedLock l) { lock = l; }
        public void realRun() throws InterruptedException {
            lock.writeLockInterruptibly();
        }
    }

    /**
     * Releases write lock, checking isWriteLocked before and after
     */
    void releaseWriteLock(StampedLock lock, long s) {
        assertTrue(lock.isWriteLocked());
        lock.unlockWrite(s);
        assertFalse(lock.isWriteLocked());
    }

    /**
     * Constructed StampedLock is in unlocked state
     */
    public void testConstructor() {
        StampedLock lock;
        lock = new StampedLock();
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
    }

    /**
     * write-locking and read-locking an unlocked lock succeed
     */
    public void testLock() {
        StampedLock lock = new StampedLock();
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        long s = lock.writeLock();
        assertTrue(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        lock.unlockWrite(s);
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        long rs = lock.readLock();
        assertFalse(lock.isWriteLocked());
        assertTrue(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 1);
        lock.unlockRead(rs);
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
    }

    /**
     * unlock releases either a read or write lock
     */
    public void testUnlock() {
        StampedLock lock = new StampedLock();
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        long s = lock.writeLock();
        assertTrue(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        lock.unlock(s);
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        long rs = lock.readLock();
        assertFalse(lock.isWriteLocked());
        assertTrue(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 1);
        lock.unlock(rs);
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
    }

    /**
     * tryUnlockRead/Write succeeds if locked in associated mode else
     * returns false
     */
    public void testTryUnlock() {
        StampedLock lock = new StampedLock();
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        long s = lock.writeLock();
        assertTrue(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        assertFalse(lock.tryUnlockRead());
        assertTrue(lock.tryUnlockWrite());
        assertFalse(lock.tryUnlockWrite());
        assertFalse(lock.tryUnlockRead());
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
        long rs = lock.readLock();
        assertFalse(lock.isWriteLocked());
        assertTrue(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 1);
        assertFalse(lock.tryUnlockWrite());
        assertTrue(lock.tryUnlockRead());
        assertFalse(lock.tryUnlockRead());
        assertFalse(lock.tryUnlockWrite());
        assertFalse(lock.isWriteLocked());
        assertFalse(lock.isReadLocked());
        assertEquals(lock.getReadLockCount(), 0);
    }

    /**
     * write-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    public void testWriteUnlock_IMSE() {
        StampedLock lock = new StampedLock();
        try {
            lock.unlockWrite(0L);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
    }

    /**
     * write-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    public void testWriteUnlock_IMSE2() {
        StampedLock lock = new StampedLock();
        try {
            long s = lock.writeLock();
            lock.unlockWrite(s);
            lock.unlockWrite(s);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
    }

    /**
     * write-unlocking after readlock throws IllegalMonitorStateException
     */
    public void testWriteUnlock_IMSE3() {
        StampedLock lock = new StampedLock();
        try {
            long s = lock.readLock();
            lock.unlockWrite(s);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
    }

    /**
     * read-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    public void testReadUnlock_IMSE() {
        StampedLock lock = new StampedLock();
        try {
            long s = lock.readLock();
            lock.unlockRead(s);
            lock.unlockRead(s);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
    }

    /**
     * read-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    public void testReadUnlock_IMSE2() {
        StampedLock lock = new StampedLock();
        try {
            lock.unlockRead(0L);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
    }

    /**
     * read-unlocking after writeLock throws IllegalMonitorStateException
     */
    public void testReadUnlock_IMSE3() {
        StampedLock lock = new StampedLock();
        try {
            long s = lock.writeLock();
            lock.unlockRead(s);
            shouldThrow();
        } catch (IllegalMonitorStateException success) {}
    }

    /**
     * validate(0) fails
     */
    public void testValidate0() {
        StampedLock lock = new StampedLock();
        assertFalse(lock.validate(0L));
    }

    /**
     * A stamp obtained from a successful lock operation validates
     */
    public void testValidate() {
        try {
            StampedLock lock = new StampedLock();
            long s = lock.writeLock();
            assertTrue(lock.validate(s));
            lock.unlockWrite(s);
            s = lock.readLock();
            assertTrue(lock.validate(s));
            lock.unlockRead(s);
            assertTrue((s = lock.tryWriteLock()) != 0L);
            assertTrue(lock.validate(s));
            lock.unlockWrite(s);
            assertTrue((s = lock.tryReadLock()) != 0L);
            assertTrue(lock.validate(s));
            lock.unlockRead(s);
            assertTrue((s = lock.tryWriteLock(100L, MILLISECONDS)) != 0L);
            assertTrue(lock.validate(s));
            lock.unlockWrite(s);
            assertTrue((s = lock.tryReadLock(100L, MILLISECONDS)) != 0L);
            assertTrue(lock.validate(s));
            lock.unlockRead(s);
            assertTrue((s = lock.tryOptimisticRead()) != 0L);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * A stamp obtained from an unsuccessful lock operation does not validate
     */
    public void testValidate2() {
        try {
            StampedLock lock = new StampedLock();
            long s;
            assertTrue((s = lock.writeLock()) != 0L);
            assertTrue(lock.validate(s));
            assertFalse(lock.validate(lock.tryWriteLock()));
            assertFalse(lock.validate(lock.tryWriteLock(100L, MILLISECONDS)));
            assertFalse(lock.validate(lock.tryReadLock()));
            assertFalse(lock.validate(lock.tryReadLock(100L, MILLISECONDS)));
            assertFalse(lock.validate(lock.tryOptimisticRead()));
            lock.unlockWrite(s);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * writeLockInterruptibly is interruptible
     */
    public void testWriteLockInterruptibly_Interruptible() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                running.countDown();
                lock.writeLockInterruptibly();
            }});
        try {
            running.await();
            waitForThreadToEnterWaitState(t, 100);
            t.interrupt();
            awaitTermination(t);
            releaseWriteLock(lock, s);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * timed tryWriteLock is interruptible
     */
    public void testWriteTryLock_Interruptible() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                running.countDown();
                lock.tryWriteLock(2 * LONG_DELAY_MS, MILLISECONDS);
            }});
        try {
            running.await();
            waitForThreadToEnterWaitState(t, 100);
            t.interrupt();
            awaitTermination(t);
            releaseWriteLock(lock, s);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * readLockInterruptibly is interruptible
     */
    public void testReadLockInterruptibly_Interruptible() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                running.countDown();
                lock.readLockInterruptibly();
            }});
        try {
            running.await();
            waitForThreadToEnterWaitState(t, 100);
            t.interrupt();
            awaitTermination(t);
            releaseWriteLock(lock, s);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * timed tryReadLock is interruptible
     */
    public void testReadTryLock_Interruptible() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                running.countDown();
                lock.tryReadLock(2 * LONG_DELAY_MS, MILLISECONDS);
            }});
        try {
            running.await();
            waitForThreadToEnterWaitState(t, 100);
            t.interrupt();
            awaitTermination(t);
            releaseWriteLock(lock, s);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * tryWriteLock on an unlocked lock succeeds
     */
    public void testWriteTryLock() {
        final StampedLock lock = new StampedLock();
        long s = lock.tryWriteLock();
        assertTrue(s != 0L);
        assertTrue(lock.isWriteLocked());
        long s2 = lock.tryWriteLock();
        assertEquals(s2, 0L);
        releaseWriteLock(lock, s);
    }

    /**
     * tryWriteLock fails if locked
     */
    public void testWriteTryLockWhenLocked() {
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long ws = lock.tryWriteLock();
                assertTrue(ws == 0L);
            }});

        awaitTermination(t);
        releaseWriteLock(lock, s);
    }

    /**
     * tryReadLock fails if write-locked
     */
    public void testReadTryLockWhenLocked() {
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long rs = lock.tryReadLock();
                assertEquals(rs, 0L);
            }});

        awaitTermination(t);
        releaseWriteLock(lock, s);
    }

    /**
     * Multiple threads can hold a read lock when not write-locked
     */
    public void testMultipleReadLocks() {
        final StampedLock lock = new StampedLock();
        final long s = lock.readLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long s2 = lock.tryReadLock();
                assertTrue(s2 != 0L);
                lock.unlockRead(s2);
                long s3 = lock.tryReadLock(LONG_DELAY_MS, MILLISECONDS);
                assertTrue(s3 != 0L);
                lock.unlockRead(s3);
                long s4 = lock.readLock();
                lock.unlockRead(s4);
            }});

        awaitTermination(t);
        lock.unlockRead(s);
    }

    /**
     * A writelock succeeds only after a reading thread unlocks
     */
    public void testWriteAfterReadLock() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long rs = lock.readLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                running.countDown();
                long s = lock.writeLock();
                lock.unlockWrite(s);
            }});
        try {
            running.await();
            waitForThreadToEnterWaitState(t, 100);
            assertFalse(lock.isWriteLocked());
            lock.unlockRead(rs);
            awaitTermination(t);
            assertFalse(lock.isWriteLocked());
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * A writelock succeeds only after reading threads unlock
     */
    public void testWriteAfterMultipleReadLocks() {
        final StampedLock lock = new StampedLock();
        long s = lock.readLock();
        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long rs = lock.readLock();
                lock.unlockRead(rs);
            }});
        awaitTermination(t1);

        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long ws = lock.writeLock();
                lock.unlockWrite(ws);
            }});
        assertFalse(lock.isWriteLocked());
        lock.unlockRead(s);
        awaitTermination(t2);
        assertFalse(lock.isWriteLocked());
    }

    /**
     * Readlocks succeed only after a writing thread unlocks
     */
    public void testReadAfterWriteLock() {
        final StampedLock lock = new StampedLock();
        final long s = lock.writeLock();
        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long rs = lock.readLock();
                lock.unlockRead(rs);
            }});
        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long rs = lock.readLock();
                lock.unlockRead(rs);
            }});

        releaseWriteLock(lock, s);
        awaitTermination(t1);
        awaitTermination(t2);
    }

    /**
     * tryReadLock succeeds if readlocked but not writelocked
     */
    public void testTryLockWhenReadLocked() {
        final StampedLock lock = new StampedLock();
        long s = lock.readLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long rs = lock.tryReadLock();
                threadAssertTrue(rs != 0L);
                lock.unlockRead(rs);
            }});

        awaitTermination(t);
        lock.unlockRead(s);
    }

    /**
     * tryWriteLock fails when readlocked
     */
    public void testWriteTryLockWhenReadLocked() {
        final StampedLock lock = new StampedLock();
        long s = lock.readLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long ws = lock.tryWriteLock();
                threadAssertEquals(ws, 0L);
            }});

        awaitTermination(t);
        lock.unlockRead(s);
    }

    /**
     * timed tryWriteLock times out if locked
     */
    public void testWriteTryLock_Timeout() {
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                long ws = lock.tryWriteLock(timeoutMillis, MILLISECONDS);
                assertEquals(ws, 0L);
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        awaitTermination(t);
        releaseWriteLock(lock, s);
    }

    /**
     * timed tryReadLock times out if write-locked
     */
    public void testReadTryLock_Timeout() {
        final StampedLock lock = new StampedLock();
        long s = lock.writeLock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                long rs = lock.tryReadLock(timeoutMillis, MILLISECONDS);
                assertEquals(rs, 0L);
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        awaitTermination(t);
        assertTrue(lock.isWriteLocked());
        lock.unlockWrite(s);
    }

    /**
     * writeLockInterruptibly succeeds if unlocked, else is interruptible
     */
    public void testWriteLockInterruptibly() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long s = 0L;
        try {
            s = lock.writeLockInterruptibly();
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                running.countDown();
                lock.writeLockInterruptibly();
            }});

        try {
            running.await();
            waitForThreadToEnterWaitState(t, 100);
            t.interrupt();
            assertTrue(lock.isWriteLocked());
            awaitTermination(t);
            releaseWriteLock(lock, s);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }

    }

    /**
     * readLockInterruptibly succeeds if lock free else is interruptible
     */
    public void testReadLockInterruptibly() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long s = 0L;
        try {
            s = lock.readLockInterruptibly();
            lock.unlockRead(s);
            s = lock.writeLockInterruptibly();
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                running.countDown();
                lock.readLockInterruptibly();
            }});
        try {
            running.await();
            waitForThreadToEnterWaitState(t, 100);
            t.interrupt();
            awaitTermination(t);
            releaseWriteLock(lock, s);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * A serialized lock deserializes as unlocked
     */
    public void testSerialization() {
        StampedLock lock = new StampedLock();
        lock.writeLock();
        StampedLock clone = serialClone(lock);
        assertTrue(lock.isWriteLocked());
        assertFalse(clone.isWriteLocked());
        long s = clone.writeLock();
        assertTrue(clone.isWriteLocked());
        clone.unlockWrite(s);
        assertFalse(clone.isWriteLocked());
    }

    /**
     * toString indicates current lock state
     */
    public void testToString() {
        StampedLock lock = new StampedLock();
        assertTrue(lock.toString().contains("Unlocked"));
        long s = lock.writeLock();
        assertTrue(lock.toString().contains("Write-locked"));
        lock.unlockWrite(s);
        s = lock.readLock();
        assertTrue(lock.toString().contains("Read-locks"));
    }

    /**
     * tryOptimisticRead succeeds and validates if unlocked, fails if locked
     */
    public void testValidateOptimistic() {
        try {
            StampedLock lock = new StampedLock();
            long s, p;
            assertTrue((p = lock.tryOptimisticRead()) != 0L);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.writeLock()) != 0L);
            assertFalse((p = lock.tryOptimisticRead()) != 0L);
            assertTrue(lock.validate(s));
            lock.unlockWrite(s);
            assertTrue((p = lock.tryOptimisticRead()) != 0L);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.readLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryOptimisticRead()) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockRead(s);
            assertTrue((s = lock.tryWriteLock()) != 0L);
            assertTrue(lock.validate(s));
            assertFalse((p = lock.tryOptimisticRead()) != 0L);
            lock.unlockWrite(s);
            assertTrue((s = lock.tryReadLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryOptimisticRead()) != 0L);
            lock.unlockRead(s);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.tryWriteLock(100L, MILLISECONDS)) != 0L);
            assertFalse((p = lock.tryOptimisticRead()) != 0L);
            assertTrue(lock.validate(s));
            lock.unlockWrite(s);
            assertTrue((s = lock.tryReadLock(100L, MILLISECONDS)) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryOptimisticRead()) != 0L);
            lock.unlockRead(s);
            assertTrue((p = lock.tryOptimisticRead()) != 0L);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * tryOptimisticRead stamp does not validate if a write lock intervenes
     */
    public void testValidateOptimisticWriteLocked() {
        StampedLock lock = new StampedLock();
        long s, p;
        assertTrue((p = lock.tryOptimisticRead()) != 0L);
        assertTrue((s = lock.writeLock()) != 0L);
        assertFalse(lock.validate(p));
        assertFalse((p = lock.tryOptimisticRead()) != 0L);
        assertTrue(lock.validate(s));
        lock.unlockWrite(s);
    }

    /**
     * tryOptimisticRead stamp does not validate if a write lock
     * intervenes in another thread
     */
    public void testValidateOptimisticWriteLocked2() {
        final CountDownLatch running = new CountDownLatch(1);
        final StampedLock lock = new StampedLock();
        long s, p;
        assertTrue((p = lock.tryOptimisticRead()) != 0L);
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
                public void realRun() throws InterruptedException {
                    lock.writeLockInterruptibly();
                    running.countDown();
                    lock.writeLockInterruptibly();
                }});
        try {
            running.await();
            assertFalse(lock.validate(p));
            assertFalse((p = lock.tryOptimisticRead()) != 0L);
            t.interrupt();
            awaitTermination(t);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * tryConvertToOptimisticRead succeeds and validates if successfully locked,
     */
    public void testTryConvertToOptimisticRead() {
        try {
            StampedLock lock = new StampedLock();
            long s, p;
            s = 0L;
            assertFalse((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue((s = lock.tryOptimisticRead()) != 0L);
            assertTrue((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue((s = lock.writeLock()) != 0L);
            assertTrue((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.readLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.tryWriteLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.tryReadLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.tryWriteLock(100L, MILLISECONDS)) != 0L);
            assertTrue((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue(lock.validate(p));
            assertTrue((s = lock.tryReadLock(100L, MILLISECONDS)) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToOptimisticRead(s)) != 0L);
            assertTrue(lock.validate(p));
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * tryConvertToReadLock succeeds and validates if successfully locked
     * or lock free;
     */
    public void testTryConvertToReadLock() {
        try {
            StampedLock lock = new StampedLock();
            long s, p;
            s = 0L;
            assertFalse((p = lock.tryConvertToReadLock(s)) != 0L);
            assertTrue((s = lock.tryOptimisticRead()) != 0L);
            assertTrue((p = lock.tryConvertToReadLock(s)) != 0L);
            lock.unlockRead(p);
            assertTrue((s = lock.writeLock()) != 0L);
            assertTrue((p = lock.tryConvertToReadLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockRead(p);
            assertTrue((s = lock.readLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToReadLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockRead(p);
            assertTrue((s = lock.tryWriteLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToReadLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockRead(p);
            assertTrue((s = lock.tryReadLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToReadLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockRead(p);
            assertTrue((s = lock.tryWriteLock(100L, MILLISECONDS)) != 0L);
            assertTrue((p = lock.tryConvertToReadLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockRead(p);
            assertTrue((s = lock.tryReadLock(100L, MILLISECONDS)) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToReadLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockRead(p);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }

    /**
     * tryConvertToWriteLock succeeds and validates if successfully locked
     * or lock free;
     */
    public void testTryConvertToWriteLock() {
        try {
            StampedLock lock = new StampedLock();
            long s, p;
            s = 0L;
            assertFalse((p = lock.tryConvertToWriteLock(s)) != 0L);
            assertTrue((s = lock.tryOptimisticRead()) != 0L);
            assertTrue((p = lock.tryConvertToWriteLock(s)) != 0L);
            lock.unlockWrite(p);
            assertTrue((s = lock.writeLock()) != 0L);
            assertTrue((p = lock.tryConvertToWriteLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockWrite(p);
            assertTrue((s = lock.readLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToWriteLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockWrite(p);
            assertTrue((s = lock.tryWriteLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToWriteLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockWrite(p);
            assertTrue((s = lock.tryReadLock()) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToWriteLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockWrite(p);
            assertTrue((s = lock.tryWriteLock(100L, MILLISECONDS)) != 0L);
            assertTrue((p = lock.tryConvertToWriteLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockWrite(p);
            assertTrue((s = lock.tryReadLock(100L, MILLISECONDS)) != 0L);
            assertTrue(lock.validate(s));
            assertTrue((p = lock.tryConvertToWriteLock(s)) != 0L);
            assertTrue(lock.validate(p));
            lock.unlockWrite(p);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        }
    }
}
