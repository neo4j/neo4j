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
package org.neo4j.graphdb;

/**
 * An input position refers to a specific point in a query string.
 */
public final class InputPosition
{
    private final int offset;
    private final int line;
    private final int column;

    /**
     * The empty position
     */
    public static final InputPosition empty = new InputPosition( -1, -1, -1 );

    /**
     * Creating a position from and offset, line number and a column number.
     * @param offset the offset from the start of the string, starting from 0.
     * @param line the line number, starting from 1.
     * @param column the column number, starting from 1.
     */
    public InputPosition( int offset, int line, int column )
    {
        this.offset = offset;
        this.line = line;
        this.column = column;
    }

    /**
     * The character offset referred to by this position; offset numbers start at 0.
     * @return the offset of this position.
     */
    public int getOffset()
    {
        return offset;
    }

    /**
     * The line number referred to by the position; line numbers start at 1.
     * @return the line number of this position.
     */
    public int getLine()
    {
        return line;
    }

    /**
     * The column number referred to by the position; column numbers start at 1.
     * @return the column number of this position.
     */
    public int getColumn()
    {
        return column;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InputPosition that = (InputPosition) o;

        if ( offset != that.offset )
        {
            return false;
        }
        if ( line != that.line )
        {
            return false;
        }
        return column == that.column;

    }

    @Override
    public int hashCode()
    {
        int result = offset;
        result = 31 * result + line;
        result = 31 * result + column;
        return result;
    }

    @Override
    public String toString()
    {
        return "InputPosition{" + "offset=" + offset + ", line=" + line + ", column=" + column + '}';
    }
}
