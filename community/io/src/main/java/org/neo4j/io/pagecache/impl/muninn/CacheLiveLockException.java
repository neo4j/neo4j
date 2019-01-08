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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;

/**
 * Thrown if a live-lock is encountered in the page cache. This should be extremely rare. The case would be that all
 * pages are currently locked by page cursors, and you try to lock one more page. Then the pin would get stuck trying
 * to find a page to evict and then fault. Except this exception will eventually get thrown to escape the infinite loop.
 */
public class CacheLiveLockException extends IOException
{
    public CacheLiveLockException( String msg )
    {
        super( msg );
    }
}
