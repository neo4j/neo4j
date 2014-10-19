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
package org.neo4j.csv.reader;

import java.io.IOException;
import java.util.Arrays;

/**
 * Adds quotation awareness to a {@link CharSeeker}.
 */
public class QuoteAwareCharSeeker implements CharSeeker
{
    private final CharSeeker actual;
    private final int quotationChar;
    private final int[] quotationCharDelimiter;

    // Keep the last delimiter so that if it comes again the next call, don't create a new int[]
    private int[] lastActualDelimiter;
    private int[] lastQuotedDelimiter;

    private QuoteAwareCharSeeker( CharSeeker actual, char quoteChar )
    {
        this.actual = actual;
        this.quotationChar = quoteChar;
        this.quotationCharDelimiter = new int[] {quoteChar};
    }

    public static CharSeeker quoteAware( CharSeeker actual, char quoteChar )
    {
        return new QuoteAwareCharSeeker( actual, quoteChar );
    }

    @Override
    public boolean seek( Mark mark, int[] untilOneOfChars ) throws IOException
    {
        if ( actual.seek( mark, withQuoteChars( untilOneOfChars ) ) )
        {
            long startPosition = mark.startPosition();
            if ( mark.isEndOfLine() || mark.character() != quotationChar )
            {   // Good, we're there. Return the value
                mark.set( mark.lineNumber(), startPosition, mark.position(),
                        mark.isEndOfLine() ? Mark.END_OF_LINE_CHARACTER : mark.character() );
                return true;
            }

            // We hit the beginning of a quote. Step by it
            startPosition++;
            while ( actual.seek( mark, quotationCharDelimiter ) && mark.isEndOfLine() )
            {   // Keep on looking for the end of that quote, since the seeker returns line endings
                // no matter what we give it
            }

            if ( mark.isEndOfLine() )
            {   // We didn't find the other quote
                mark.set( mark.lineNumber(), -1, -1, Mark.END_OF_LINE_CHARACTER );
                return false;
            }

            // We found the quote, now just skip to the next delimiter. Whether or not we found
            // anything else after the quote doesn't matter
            long position = mark.position();
            actual.seek( mark, untilOneOfChars );
            mark.set( mark.lineNumber(), startPosition, position,
                    mark.isEndOfLine() ? Mark.END_OF_LINE_CHARACTER : mark.character() );
            return true;
        }

        mark.set( mark.lineNumber(), -1, -1, Mark.END_OF_LINE_CHARACTER );
        return false;
    }

    private int[] withQuoteChars( int[] delimiter )
    {
        if ( Arrays.equals( delimiter, lastActualDelimiter ) )
        {
            return lastQuotedDelimiter;
        }

        lastQuotedDelimiter = Arrays.copyOf( delimiter, delimiter.length+1 );
        lastQuotedDelimiter[delimiter.length] = quotationChar;
        lastActualDelimiter = delimiter;
        return lastQuotedDelimiter;
    }

    @Override
    public <T> T extract( Mark mark, Extractor<T> extractor )
    {
        return actual.extract( mark, extractor );
    }

    @Override
    public void close() throws IOException
    {
        actual.close();
    }
}
