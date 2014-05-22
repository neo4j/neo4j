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
package org.neo4j.io.pagecache;

import java.io.File;
import java.io.IOException;


public interface PageCache
{
    /**
     * Ask for a handle to a paged file, backed by this page cache.
     *
     * Note that this currently asks for the pageSize to use, which is an artifact or records being
     * of varying size in the stores. This should be consolidated to use a standard page size for the
     * whole cache, with records aligning on those page boundaries.
     */
    PagedFile map( File file, int pageSize ) throws IOException;

    void unmap( File file ) throws IOException;

    PageCursor newCursor();

    void close() throws IOException;

    void flush();
}
