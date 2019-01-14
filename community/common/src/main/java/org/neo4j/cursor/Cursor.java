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
package org.neo4j.cursor;

/**
 * A cursor is an object that moves to point to different locations in a data structure.
 * The abstraction originally comes from mechanical slide rules, which have a "cursor" which
 * slides to point to different positions on the ruler.
 * <p>
 * Each position a cursor points to is referred to as a "row".
 * <p>
 * Access to the current row is done by subtyping this interface and adding accessor methods. If no call to
 * {@link #next()} has been done, or if it returned false, then such accessor methods throw {@link
 * IllegalStateException}.
 */
public interface Cursor<T> extends RawCursor<T,RuntimeException>
{
    @Override
    boolean next();

    @Override
    void close();
}
