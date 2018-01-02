/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

/**
 * A non continuous, strictly monotonic queue of transaction ids. Equivalently, a queue where the head is always
 * the minimum value.
 *
 * Threads can wait for the minimal value to reach a specific value, upon which event they are woken up and can
 * act. This notification happens only when the current minimal value (head) is removed, so care should be taken
 * to remove it when done.
 *
 * @author Mattias Persson
 */
public interface IdOrderingQueue
{
    /**
     * Adds this id at the tail of the queue. The argument must be larger than all previous arguments
     * passed to this method.
     * @param value The id to add
     */
    void offer( long value );

    /**
     * Waits for the argument to become the head of the queue. This is a blocking operation and as such it may
     * throw InterruptedException.
     * @param value The id to wait for to become the head of the queue
     * @throws InterruptedException
     */
    void waitFor( long value ) throws InterruptedException;

    /**
     * Remove the current minimum value, while ensuring that it the expected value.
     * @param expectedValue The value the minimum value is supposed to be - if the check fails,
     *                      an IllegalStateException will be thrown and the notification of waiting threads will not
     *                      happen.
     */
    void removeChecked( long expectedValue );

    boolean isEmpty();

    IdOrderingQueue BYPASS = new IdOrderingQueue()
    {
        @Override
        public void offer( long value )
        {   // Just ignore, it's fine
        }

        @Override
        public void waitFor( long value )
        {   // Just ignore, it's fine
        }

        @Override
        public void removeChecked( long expectedValue )
        {   // Just ignore, it's fine
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }
    };
}
