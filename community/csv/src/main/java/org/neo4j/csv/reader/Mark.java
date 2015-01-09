/**
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
package org.neo4j.csv.reader;

import static java.lang.String.format;

/**
 * A mutable marker that is changed to hold progress made to a {@link BufferedCharSeeker}.
 * It holds information such as start/end position in the data stream, which character
 * was the match and whether or not this denotes the last value of the line.
 */
public class Mark
{
    public static int END_OF_LINE_CHARACTER = -1;

    private int lineNumber;
    private long startPosition;
    private long position;
    private int character;
    private boolean skippedChars;

    /**
     * @param startPosition
     * @param position
     * @param character use {@code -1} to denote that the matching character was an end-of-line or end-of-file
     * @param skippedChars whether or not the original data had some characters that were skipped, f.ex
     * quotation or escaped characters.
     */
    void set( int lineNumber, long startPosition, long position, int character, boolean skippedChars )
    {
        this.lineNumber = lineNumber;
        this.startPosition = startPosition;
        this.position = position;
        this.character = character;
        this.skippedChars = skippedChars;
    }

    public int character()
    {
        assert !isEndOfLine();
        return character;
    }

    public boolean isEndOfLine()
    {
        return character == -1;
    }

    public int lineNumber()
    {
        return lineNumber;
    }

    public boolean hasSkippedChars()
    {
        return skippedChars;
    }

    long position()
    {
        if ( position == -1 )
        {
            throw new IllegalStateException( "No value to extract here" );
        }
        return position;
    }

    long startPosition()
    {
        if ( startPosition == -1 )
        {
            throw new IllegalStateException( "No value to extract here" );
        }
        return startPosition;
    }

    @Override
    public String toString()
    {
        return format( "Mark[line:%d, from:%d, to:%d, skipped:%b]", lineNumber, startPosition, position, skippedChars );
    }
}
