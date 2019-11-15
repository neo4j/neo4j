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
package org.neo4j.internal.kernel.api;

/**
 * Enriches AutoCloseable with isClosed(). This method can be used to query whether a resource was closed or
 * to make sure that it is only closed once.
 * <p>
 * Also provides ability to register a listener for when this is closed.
 */
public interface AutoCloseablePlus extends AutoCloseable
{
    @Override
    void close();

    /**
     * Same as close(), but invoked before the listener has been notified.
     */
    void closeInternal();

    boolean isClosed();

    void setCloseListener( CloseListener closeListener );

    CloseListener getCloseListener();

    /**
     * Assigns a token to the AutoCloseable that can be used to as an index for faster lookups.
     * @param token the token to assign to the AutoCloseable
     */
    void setToken( int token );

    /**
     * Retrieves the token associated with the AutoCloseable
     */
    int getToken();
}
