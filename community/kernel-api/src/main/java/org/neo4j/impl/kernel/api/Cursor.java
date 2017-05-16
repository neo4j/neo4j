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
package org.neo4j.impl.kernel.api;

/**
 * This interface is package-private because it is not generically useful. All use cases should use the explicit cursor
 * types. It is however useful to define this interface to ensure that the generic usage pattern (as outlined by the
 * example code snippet below) aligns across all cursor interfaces.
 * <p>
 * Generic usage:
 * <code><pre>
 * SomeCursor cursor = allocateCursorOfAppropriateType();
 * store.positionCursor( cursor );
 * while ( cursor.next() )
 * {
 *     do
 *     {
 *         // access data here ...
 *         // also make sure that data read here is considered valid until `shouldRetry()` is false.
 *     }
 *     while ( cursor.shouldRetry() );
 *     // use accessed data here ...
 * }
 * </pre></code>
 */
interface Cursor extends AutoCloseable
{
    boolean next();

    boolean shouldRetry();

    @Override
    void close();
}
