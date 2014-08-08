/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;

public interface PageTable
{
    /**
     * Load a new page into the table. This does not guarantee avoiding duplicate
     * pages loaded into the cache, it is up to the callee to ensure pages do not get
     * duplicated into the table.
     *
     * The page returned is pre-locked with the lock specified in the call.
     */
    PinnablePage load( PageSwapper io, long pageId, int pf_flags ) throws IOException;

    /** Flush all dirty pages. */
    void flush() throws IOException;

    /** Flush all dirty pages backed by the specified io. */
    void flush( PageSwapper io ) throws IOException;

    int pageSize();

    int maxCachedPages();

}
