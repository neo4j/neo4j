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
package org.neo4j.cursor;

/**
 * A cursor is an object that moves to point to different locations in a data structure.
 * The abstraction originally comes from mechanical slide rules, which have a "cursor" which
 * slides to point to different positions on the ruler.
 *
 * Each position a cursor points to is referred to as a "row".
 *
 * This cursor does not allow the user to directly interact with the current row, it is
 * assumed that the data structure using this cursor provides some alternative mechanism for that.
 * The cursor is simply a control object that defines a current position and a means to move
 * that position.
 */
public interface Cursor extends AutoCloseable
{
    /**
     * Move the cursor to the next row.
     * Return false if there are no more valid positions, generally indicating that the end of the data structure
     * has been reached. */
    boolean next();

    /** Reset the cursor to its origin position. */
    void reset();

    /** Signal that the cursor is no longer needed. */
    @Override
    void close();
}
