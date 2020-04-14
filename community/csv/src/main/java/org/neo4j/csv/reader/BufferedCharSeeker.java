/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.csv.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.values.storable.CSVHeaderInformation;

import static java.lang.String.format;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.csv.reader.Mark.END_OF_LINE_CHARACTER;

/**
 * Much like a {@link BufferedReader} for a {@link Reader}.
 */
public class BufferedCharSeeker implements CharSeeker
{
    private static final char EOL_CHAR = '\n';
    private static final char EOL_CHAR_2 = '\r';
    private static final char EOF_CHAR = (char) -1;
    private static final char BACK_SLASH = '\\';

    private char[] buffer;
    private int dataLength;
    private int dataCapacity;

    // index into the buffer character array to read the next time nextChar() is called
    private int bufferPos;
    private int bufferStartPos;
    // last index (effectively length) of characters in use in the buffer
    private int bufferEnd;
    // bufferPos denoting the start of this current line that we're reading
    private int lineStartPos;
    // bufferPos when we started reading the current field
    private int seekStartPos;
    // 1-based value of which logical line we're reading a.t.m.
    private int lineNumber;
    // flag to know if we've read to the end
    private boolean eof;
    // char to recognize as quote start/end
    private final char quoteChar;
    // this absolute position + bufferPos is the current position in the source we're reading
    private long absoluteBufferStartPosition;
    private String sourceDescription;
    private final boolean multilineFields;
    private final boolean legacyStyleQuoting;
    private final Source source;
    private Chunk currentChunk;
    private final boolean trim;

    public BufferedCharSeeker( Source source, Configuration config )
    {
        this.source = source;
        this.quoteChar = config.quotationCharacter();
        this.multilineFields = config.multilineFields();
        this.legacyStyleQuoting = config.legacyStyleQuoting();
        this.trim = getTrimStringIgnoreErrors( config );
    }

    @Override
    public boolean seek( Mark mark, int untilChar ) throws IOException
    {
        if ( eof )
        {   // We're at the end
            return eof( mark );
        }

        // Keep a start position in case we need to further fill the buffer in nextChar, a value can at maximum be the
        // whole buffer, so max one fill per value is supported.
        seekStartPos = bufferPos; // seekStartPos updated in nextChar if buffer flips over, that's why it's a member
        int ch;
        int endOffset = 1;
        int skippedChars = 0;
        int quoteDepth = 0;
        int quoteStartLine = 0;
        boolean isQuoted = false;

        while ( !eof )
        {
            ch = nextChar( skippedChars );
            if ( quoteDepth == 0 )
            {   // In normal mode, i.e. not within quotes
                if ( ch == untilChar )
                {   // We found a delimiter, set marker and return true
                    return setMark( mark, endOffset, skippedChars, ch, isQuoted );
                }
                else if ( trim && isWhitespace( ch ) )
                {   // Only check for left+trim whitespace as long as we haven't found a non-whitespace character
                    if ( seekStartPos == bufferPos - 1/* -1 since we just advanced one */ )
                    {   // We found a whitespace, which is before the first non-whitespace of the value and we've been told to trim that off
                        seekStartPos++;
                    }
                }
                else if ( ch == quoteChar && seekStartPos == bufferPos - 1/* -1 since we just advanced one */ )
                {   // We found a quote, which was the first of the value, skip it and switch mode
                    quoteDepth++;
                    isQuoted = true;
                    seekStartPos++;
                    quoteStartLine = lineNumber;
                }
                else if ( isNewLine( ch ) )
                {   // Encountered newline, done for now
                    if ( bufferPos - 1 == lineStartPos )
                    {   // We're at the start of this read so just skip it
                        seekStartPos++;
                        lineStartPos++;
                        continue;
                    }
                    break;
                }
                else if ( isQuoted )
                {   // This value is quoted, i.e. started with a quote and has also seen a quote
                    throw new DataAfterQuoteException( this,
                            new String( buffer, seekStartPos, bufferPos - seekStartPos ) );
                }
                // else this is a character to include as part of the current value
            }
            else
            {   // In quoted mode, i.e. within quotes
                if ( ch == quoteChar )
                {   // Found a quote within a quote, peek at next char
                    int nextCh = peekChar( skippedChars );
                    if ( nextCh == quoteChar )
                    {   // Found a double quote, skip it and we're going down one more quote depth (quote-in-quote)
                        repositionChar( bufferPos++, ++skippedChars );
                    }
                    else
                    {   // Found an ending quote, skip it and switch mode
                        endOffset++;
                        quoteDepth--;
                    }
                }
                else if ( isNewLine( ch ) )
                {   // Found a new line inside a quotation...
                    if ( !multilineFields )
                    {   // ...but we are configured to disallow it
                        throw new IllegalMultilineFieldException( this );
                    }
                    // ... it's OK, just keep going
                    if ( ch == EOL_CHAR )
                    {
                        lineNumber++;
                    }
                }
                else if ( ch == BACK_SLASH && legacyStyleQuoting )
                {   // Legacy concern, support java style quote encoding
                    int nextCh = peekChar( skippedChars );
                    if ( nextCh == quoteChar || nextCh == BACK_SLASH )
                    {   // Found a slash encoded quote
                        repositionChar( bufferPos++, ++skippedChars );
                    }
                }
                else if ( eof )
                {
                    // We have an open quote but have reached the end of the file, this is a formatting error
                    throw new MissingEndQuoteException( this, quoteStartLine, quoteChar );
                }
            }
        }

        int valueLength = bufferPos - seekStartPos - 1;
        if ( eof && valueLength == 0 && seekStartPos == lineStartPos )
        {   // We didn't find any of the characters sought for
            return eof( mark );
        }

        // We found the last value of the line or stream
        lineNumber++;
        lineStartPos = bufferPos;
        return setMark( mark, endOffset, skippedChars, END_OF_LINE_CHARACTER, isQuoted );
    }

    @Override
    public <EXTRACTOR extends Extractor<?>> EXTRACTOR extract( Mark mark, EXTRACTOR extractor )
    {
        return extract( mark, extractor, null );
    }

    private boolean setMark( Mark mark, int endOffset, int skippedChars, int ch, boolean isQuoted )
    {
        int pos = (trim ? rtrim() : bufferPos) - endOffset - skippedChars;
        mark.set( seekStartPos, pos, ch, isQuoted );
        return true;
    }

    /**
     * Starting from the current position, {@link #bufferPos}, scan backwards as long as whitespace is found.
     * Although it cannot scan further back than the start of this field is, i.e. {@link #seekStartPos}.
     *
     * @return the right index of the value to pass into {@link Mark}. This is only called if {@link Configuration#trimStrings()} is {@code true}.
     */
    private int rtrim()
    {
        int index = bufferPos;
        while ( index - 1 > seekStartPos && isWhitespace( buffer[index - 1 /*bufferPos has advanced*/ - 1 /*don't check the last read char (delim or EOF)*/] ) )
        {
            index--;
        }
        return index;
    }

    private static boolean isWhitespace( int ch )
    {
        return ch == ' ' ||
                ch == Character.SPACE_SEPARATOR ||
                ch == Character.PARAGRAPH_SEPARATOR ||
                ch == '\u00A0' ||
                ch == '\u001C' ||
                ch == '\u001D' ||
                ch == '\u001E' ||
                ch == '\u001F' ||
                ch == '\u2007' ||
                ch == '\u202F' ||
                ch == '\t';

    }

    private void repositionChar( int offset, int stepsBack )
    {
        // We reposition characters because we might have skipped some along the way, double-quotes and what not.
        // We want to take an as little hit as possible for that, so we reposition each character as long as
        // we're still reading the same value. All other values will not have to take any hit of skipped chars
        // for this particular value.
        buffer[offset - stepsBack] = buffer[offset];
    }

    private static boolean isNewLine( int ch )
    {
        return ch == EOL_CHAR || ch == EOL_CHAR_2;
    }

    private int peekChar( int skippedChars ) throws IOException
    {
        int ch = nextChar( skippedChars );
        try
        {
            return ch;
        }
        finally
        {
            if ( ch != EOF_CHAR )
            {
                bufferPos--;
            }
        }
    }

    private static boolean eof( Mark mark )
    {
        mark.set( -1, -1, Mark.END_OF_LINE_CHARACTER, false );
        return false;
    }

    private static boolean getTrimStringIgnoreErrors( Configuration config )
    {
        try
        {
            return config.trimStrings();
        }
        catch ( Throwable t )
        {
            // Cypher compatibility can result in older Cypher 2.3 code being passed here with older implementations of
            // Configuration. So we need to ignore the fact that those implementations do not include trimStrings().
            return COMMAS.trimStrings();
        }
    }

    @Override
    public <EXTRACTOR extends Extractor<?>> EXTRACTOR extract( Mark mark, EXTRACTOR extractor, CSVHeaderInformation optionalData )
    {
        if ( !tryExtract( mark, extractor, optionalData ) )
        {
            throw new IllegalStateException( extractor + " didn't extract value for " + mark +
                    ". For values which are optional please use tryExtract method instead" );
        }
        return extractor;
    }

    @Override
    public boolean tryExtract( Mark mark, Extractor<?> extractor, CSVHeaderInformation optionalData )
    {
        int from = mark.startPosition();
        int to = mark.position();
        return extractor.extract( buffer, from, to - from, mark.isQuoted(), optionalData );
    }

    @Override
    public boolean tryExtract( Mark mark, Extractor<?> extractor )
    {
        return tryExtract( mark, extractor, null );
    }

    private int nextChar( int skippedChars ) throws IOException
    {
        int ch;
        if ( bufferPos < bufferEnd || fillBuffer() )
        {
            ch = buffer[bufferPos];
        }
        else
        {
            ch = EOF_CHAR;
            eof = true;
        }

        if ( skippedChars > 0 )
        {
            repositionChar( bufferPos, skippedChars );
        }
        bufferPos++;
        return ch;
    }

    /**
     * @return {@code true} if something was read, otherwise {@code false} which means that we reached EOF.
     */
    private boolean fillBuffer() throws IOException
    {
        boolean first = currentChunk == null;

        if ( !first )
        {
            if ( bufferPos - seekStartPos >= dataCapacity )
            {
                throw new BufferOverflowException( "Tried to read a field larger than buffer size " +
                        dataLength + ". A common cause of this is that a field has an unterminated " +
                        "quote and so will try to seek until the next quote, which ever line it may be on." +
                        " This should not happen if multi-line fields are disabled, given that the fields contains " +
                        "no new-line characters. This field started at " + sourceDescription() + ":" + lineNumber() );
            }
        }

        absoluteBufferStartPosition += dataLength;

        // Fill the buffer with new characters
        Chunk nextChunk = source.nextChunk( first ? -1 : seekStartPos );
        if ( nextChunk == Source.EMPTY_CHUNK )
        {
            return false;
        }

        buffer = nextChunk.data();
        dataLength = nextChunk.length();
        dataCapacity = nextChunk.maxFieldSize();
        bufferPos = nextChunk.startPosition();
        bufferStartPos = bufferPos;
        bufferEnd = bufferPos + dataLength;
        int shift = seekStartPos - nextChunk.backPosition();
        seekStartPos = nextChunk.backPosition();
        if ( first )
        {
            lineStartPos = seekStartPos;
        }
        else
        {
            lineStartPos -= shift;
        }
        String sourceDescriptionAfterRead = nextChunk.sourceDescription();
        if ( !sourceDescriptionAfterRead.equals( sourceDescription ) )
        {   // We moved over to a new source, reset line number
            lineNumber = 0;
            sourceDescription = sourceDescriptionAfterRead;
        }
        currentChunk = nextChunk;
        return dataLength > 0;
    }

    @Override
    public void close() throws IOException
    {
        source.close();
    }

    @Override
    public long position()
    {
        return absoluteBufferStartPosition + (bufferPos - bufferStartPos);
    }

    @Override
    public String sourceDescription()
    {
        return sourceDescription;
    }

    public long lineNumber()
    {
        return lineNumber;
    }

    @Override
    public String toString()
    {
        return format( "%s[source:%s, position:%d, line:%d]", getClass().getSimpleName(),
                sourceDescription(), position(), lineNumber() );
    }

    public static boolean isEolChar( char c )
    {
        return c == EOL_CHAR || c == EOL_CHAR_2;
    }
}
