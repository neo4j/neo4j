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
package org.neo4j.bolt.v1.runtime.internal;

/**
 * Exposes the ability to manipulate the state of a running session in various ways. This is the interface Tank
 * uses to manipulate the session.
 */
public interface SessionState
{
    /**
     * Begin a new explicit transaction, meaning a transaction that will remain open until the client explicitly
     * commits or rolls it back.
     */
    void beginTransaction();

    /**
     * Begin a new implicit transaction, meaning a transaction that will commit automatically once the next result
     * that becomes available has been closed.
     */
    void beginImplicitTransaction();

    /** Check if the session currently has an open transaction. */
    boolean hasTransaction();

    /** Commit the current explicit transaction associated with this session. */
    void commitTransaction();

    /** Rollback the current explicit transaction associated with this session. */
    void rollbackTransaction();

}
