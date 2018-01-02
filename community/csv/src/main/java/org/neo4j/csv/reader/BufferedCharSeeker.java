/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import static java.lang.String.format;

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

    private final CharReadable reader;
    private char[] buffer;

    // Wraps the char[] buffer and is only used during reading more data, using f.ex. compact()
    // so that we don't have to duplicate that functionality.
    private SectionedCharBuffer charBuffer;

    // index into the buffer character array to read the next time nextChar() is called
    private int bufferPos;
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

    public BufferedCharSeeker( CharReadable reader, Configuration config )
    {
        this.reader = reader;
        this.charBuffer = new SectionedCharBuffer( config.bufferSize() );
        this.buffer = charBuffer.array();
        this.bufferPos = this.bufferEnd = charBuffer.pivot();
        this.quoteChar = config.quotationCharacter();
        this.lineStartPos = this.bufferPos;
        this.sourceDescription = reader.sourceDescription();
        this.multilineFields = config.multilineFields();
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
                if ( ch == quoteChar && seekStartPos == bufferPos - 1/* -1 since we just advanced one */ )
                {   // We found a quote, which was the first of the value, skip it and switch mode
                    quoteDepth++;
                    seekStartPos++;
                    quoteStartLine = lineNumber;
                    continue;
                }
                else if ( isNewLine( ch ) )
                {   // Encountered newline, done for now
                    if ( bufferPos-1 == lineStartPos )
                    {   // We're at the start of this read so just skip it
                        seekStartPos++;
                        lineStartPos++;
                        continue;
                    }
                    break;
                }
                else if ( ch == untilChar )
                {   // We found a delimiter, set marker and return true
                    mark.set( seekStartPos, bufferPos - endOffset - skippedChars, ch, isQuoted );
                    return true;
                }
            }
            else
            {   // In quoted mode, i.e. within quotes
                isQuoted = true;
                if ( ch == quoteChar )
                {   // Found a quote within a quote, peek at next char
                    int nextCh = peekChar( skippedChars );

                    if ( nextCh == quoteChar )
                    {   // Found a double quote, skip it and we're going down one more quote depth (quote-in-quote)
                        repositionChar( bufferPos++, ++skippedChars );
                    }
                    else if ( nextCh != untilChar && !isNewLine( nextCh ) && nextCh != EOF_CHAR )
                    {   // Found an ending quote of sorts, although the next char isn't a delimiter, newline, or EOF
                        // so it looks like there's data characters after this end quote. We don't really support that.
                        // So circle this back to the user saying there's something wrong with the field.
                        throw new DataAfterQuoteException( this,
                                new String( buffer, seekStartPos, bufferPos-seekStartPos ) );
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
                else if ( ch == BACK_SLASH )
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
        mark.set( seekStartPos, bufferPos - endOffset - skippedChars, END_OF_LINE_CHARACTER, isQuoted );
        return true;
    }

    private void repositionChar( int offset, int stepsBack )
    {
        // We reposition characters because we might have skipped some along the way, double-quotes and what not.
        // We want to take an as little hit as possible for that, so we reposition each character as long as
        // we're still reading the same value. All other values will not have to take any hit of skipped chars
        // for this particular value.
        buffer[offset - stepsBack] = buffer[offset];
    }

    private boolean isNewLine( int ch )
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

    private boolean eof( Mark mark )
    {
        mark.set( -1, -1, Mark.END_OF_LINE_CHARACTER, false );
        return false;
    }

    @Override
    public <EXTRACTOR extends Extractor<?>> EXTRACTOR extract( Mark mark, EXTRACTOR extractor )
    {
        if ( !tryExtract( mark, extractor ) )
        {
            throw new IllegalStateException( extractor + " didn't extract value for " + mark +
                    ". For values which are optional please use tryExtract method instead" );
        }
        return extractor;
    }

    @Override
    public boolean tryExtract( Mark mark, Extractor<?> extractor )
    {
        long from = mark.startPosition();
        long to = mark.position();
        return extractor.extract( buffer, (int)(from), (int)(to-from), mark.isQuoted() );
    }

    private int nextChar( int skippedChars ) throws IOException
    {
        int ch;
        if ( fillBufferIfWeHaveExhaustedIt() )
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
    private boolean fillBufferIfWeHaveExhaustedIt() throws IOException
    {
        if ( bufferPos >= bufferEnd )
        {
            if ( bufferPos - seekStartPos >= charBuffer.pivot() )
            {
                throw new IllegalStateException( "Tried to read a field larger than buffer size " +
                        charBuffer.pivot() + ". A common cause of this is that a field has an unterminated " +
                        "quote and so will try to seek until the next quote, which ever line it may be on." +
                        " This should not happen if multi-line fields are disabled, given that the fields contains " +
                        "no new-line characters. This field started at " + sourceDescription() + ":" + lineNumber() );
            }

            absoluteBufferStartPosition += charBuffer.available();

            // Fill the buffer with new characters
            charBuffer = reader.read( charBuffer, seekStartPos );
            buffer = charBuffer.array();
            bufferPos = charBuffer.pivot();
            bufferEnd = charBuffer.front();
            int shift = seekStartPos-charBuffer.back();
            seekStartPos = charBuffer.back();
            lineStartPos -= shift;
            String sourceDescriptionAfterRead = reader.sourceDescription();
            if ( !sourceDescription.equals( sourceDescriptionAfterRead ) )
            {   // We moved over to a new source, reset line number
                lineNumber = 0;
                sourceDescription = sourceDescriptionAfterRead;
            }
            return charBuffer.hasAvailable();
        }
        return true;
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    @Override
    public long position()
    {
        return absoluteBufferStartPosition + bufferPos;
    }

    @Override
    public String sourceDescription()
    {
        return sourceDescription;
    }

    @Override
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
}
