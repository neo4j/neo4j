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

import java.io.IOException;

import org.neo4j.io.pagecache.impl.common.Page;

public interface PageIO
{
    /**
     * Apply the PageIO to the given page by the given pageId, optionally
     * using the arguments io_context and io_flags, which are passed in through
     * the call to
     * {@link org.neo4j.io.pagecache.PagedFile#io(long, int, PageIO, long, long)}
     * @param pageId
     * @param page
     * @param io_context
     * @param io_flags
     */
    void apply( long pageId, Page page, long io_context, long io_flags ) throws IOException;
}
