/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.concurrent;

/**
 * Manages ownership of a piece of memory between two threads, but only allowing one thread at a time to read and write.
 * In order to separate between the two threads, they are called client and server. The only distinction between them
 * is that the client starts with access to the object, and can then hand over control to the other thread.
 *
 * This is very similar to a basic {@link java.util.concurrent.Semaphore}, but is slightly faster as it explicitly hands
 * the object back and forth, rather than allowing arbitrary acquire/release calls, approximately doubling performance.
 */
public interface FlipFlopMessaging<T>
{
    /** Get the object, independent of who controls it. */
    T message();

    /** Block until the object is available to the client. */
    T clientAwaitMessage();

    /** Hand over ownership to the server. */
    void handoverToServer();

    /** Check if the client currently owns the object. */
    boolean clientHasPendingMessage();

    /** Block until the object is available to the server. */
    T serverAwaitMessage();

    /** Hand over ownership to the client */
    void handOverToClient();

    /** Check if the server currently owns the object */
    boolean serverHasPendingMessage();
}
