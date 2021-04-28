/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.pagecache.tracing.cursor;

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;

import static java.util.Objects.requireNonNull;

public class CursorContext implements AutoCloseable
{
    public static CursorContext NULL = new CursorContext( PageCursorTracer.NULL, EmptyVersionContext.EMPTY );

    private final PageCursorTracer cursorTracer;
    private final VersionContext versionContext;

    public CursorContext( PageCursorTracer cursorTracer )
    {
        this( cursorTracer, EmptyVersionContext.EMPTY );
    }

    public CursorContext( PageCursorTracer cursorTracer, VersionContext versionContext )
    {
        this.cursorTracer = requireNonNull( cursorTracer );
        this.versionContext = requireNonNull( versionContext );
    }

    public PageCursorTracer getCursorTracer()
    {
        return cursorTracer;
    }

    public VersionContext getVersionContext()
    {
        return versionContext;
    }

    @Override
    public void close()
    {
        cursorTracer.close();
    }
}
