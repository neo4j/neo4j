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

import org.neo4j.io.pagecache.CursorException;

/**
 * This exception is used to indicate in the stack trace, whether or not
 * {@link MuninnPageCursor#usePreciseCursorErrorStackTraces} is enabled.
 *
 * This is useful because it allows us to clearly see how much we can trust the stack traces for decoding errors.
 */
final class CursorExceptionWithPreciseStackTrace extends CursorException
{
    CursorExceptionWithPreciseStackTrace( String message )
    {
        super( message );
    }
}
