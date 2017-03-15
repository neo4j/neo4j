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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Keeps id and generation of root of the tree. Can move {@link PageCursor} to root id and return its generation,
 * both read atomically.
 */
class Root
{
    /**
     * Current page id which contains the root of the tree.
     */
    private final long rootId;

    /**
     * Generation of current {@link #rootId}.
     */
    private final long rootGen;

    Root( long rootId, long rootGen )
    {
        this.rootId = rootId;
        this.rootGen = rootGen;
    }

    /**
     * Moves the provided {@code cursor} to the current root id and returning the generation where
     * that root id was assigned.
     *
     * @param cursor {@link PageCursor} to place at the current root id.
     * @return the generation where the current root was assigned.
     * @throws IOException on {@link PageCursor} error.
     */
    long goTo( PageCursor cursor ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "root", rootId );
        return rootGen;
    }

    long id()
    {
        return rootId;
    }

    long gen()
    {
        return rootGen;
    }
}
