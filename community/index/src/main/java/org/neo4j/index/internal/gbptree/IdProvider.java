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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Provide tree node (page) ids which can be used for storing tree node data.
 * Bytes on returned page ids must be empty (all zeros).
 */
interface IdProvider
{
    /**
     * Acquires a page id, guaranteed to currently not be used. The bytes on the page at this id
     * are all guaranteed to be zero at the point of returning from this method.
     *
     * @param stableGeneration current stable generation.
     * @param unstableGeneration current unstable generation.
     * @return page id guaranteed to current not be used and whose bytes are all zeros.
     * @throws IOException on {@link PageCursor} error.
     */
    long acquireNewId( long stableGeneration, long unstableGeneration ) throws IOException;

    /**
     * Releases a page id which has previously been used, but isn't anymore, effectively allowing
     * it to be reused and returned from {@link #acquireNewId(long, long)}.
     *
     * @param stableGeneration current stable generation.
     * @param unstableGeneration current unstable generation.
     * @param id page id to release.
     * @throws IOException on {@link PageCursor} error.
     */
    void releaseId( long stableGeneration, long unstableGeneration, long id ) throws IOException;
}
