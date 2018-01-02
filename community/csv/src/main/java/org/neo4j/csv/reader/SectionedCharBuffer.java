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

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;

import static java.lang.Math.min;

/**
 * Has a similar role to a {@link CharBuffer}, but is tailored to how {@link BufferedCharSeeker}
 * works and to be able to take full advantage of {@link ThreadAheadReadable}.
 *
 * First of all this thing wraps a {@code char[]} where the array, while still being one array,
 * is sectioned into two equally sized parts: the back and the front. The flow of things is as follows:
 * <ol>
 * <li>Characters are read from a {@link Reader} using {@link #readFrom(Reader)} into the front section,
 * i.e. starting from the middle of the array and forwards.</li>
 * <li>Consumer of the read characters access the {@link #array()} and starts reading from the {@link #pivot()}
 * point, which is the middle, to finally reach the end, denoted by {@link #front()} (exclusive).
 * During the reading typically characters are read into some sort of fields/values, and so the last characters
 * of the array might represent an incomplete value.</li>
 * <li>For this reason, before reading more values into the array, the characters making up the last incomplete
 * value can be moved to the back section using {@link #compact(SectionedCharBuffer, int)} where
 * after that call {@link #back()} will point to the index in the {@link #array()} containing the first value
 * in the array.</li>
 * <li>Now more characters can be read into the front section using {@link #readFrom(Reader)}.</li>
 * </ol>
 *
 * This divide into back and front section enables a behaviour in {@link ThreadAheadReadable} where the
 * thread that reads ahead reads into the front section of another buffer, a double buffer,
 * and the current buffer that {@link BufferedCharSeeker} is working with can
 * {@link #compact(SectionedCharBuffer, int)} its last incomplete characters into that double buffer
 * and flip so that the {@link BufferedCharSeeker} continues to read from the other buffer, i.e. flips
 * buffer every call to {@link ThreadAheadReadable#read(SectionedCharBuffer, int)}. Without these sections
 * the entire double buffer would have to be copied into the char seekers buffer to get the same behavior.
 */
public class SectionedCharBuffer
{
    private final char[] buffer;
    private final int pivot;
    private int back;
    private int front; // exclusive

    /**
     * @param effectiveBuffserSize Size of each section, i.e. effective buffer size that can be
     * {@link #readFrom(Reader) read} each time.
     */
    public SectionedCharBuffer( int effectiveBuffserSize )
    {
        this.buffer = new char[effectiveBuffserSize*2];
        this.front = this.pivot = effectiveBuffserSize;
    }

    /**
     * @return the underlying array which characters are {@link #readFrom(Reader) read into}.
     * {@link #back()}, {@link #pivot()} and {@link #front()} marks the noteworthy indexes into this array.
     */
    public char[] array()
    {
        return buffer;
    }

    /**
     * Copies characters in the {@link #array()} from (and including) the given {@code from} index of the array
     * and all characters forwards to {@link #front()} (excluding) index. These characters are copied into
     * the {@link #array()} of the given {@code into} buffer, where the character {@code array[from]} will
     * be be copied to {@code into.array[pivot-(front-from)]}, and so on. As an example:
     *
     * <pre>
     * pivot (i.e. effective buffer size) = 16
     * buffer A
     * &lt;------ back ------&gt; &lt;------ front -----&gt;
     * [    .    .    .    |abcd.efgh.ijkl.mnop]
     *                                 ^ = 25
     *
     * A.compactInto( B, 25 )
     *
     * buffer B
     * &lt;------ back ------&gt; &lt;------ front -----&gt;
     * [    .    . jkl.mnop|    .    .    .    ]
     * </pre>
     *
     * @param into which buffer to compact into.
     * @param from the array index to start compacting from.
     */
    public void compact( SectionedCharBuffer into, int from )
    {
        assert buffer.length == into.buffer.length;
        int diff = front-from;
        into.back = pivot-diff;
        System.arraycopy( buffer, from, into.buffer, into.back, diff );
    }

    /**
     * Reads characters from {@code reader} into the front section of this buffer, setting {@link #front()}
     * accordingly afterwards. If no characters were read due to end reached then {@link #hasAvailable()} will
     * return {@code false} after this call, likewise {@link #available()} will return 0.
     *
     * @param reader {@link Reader} to read from.
     * @throws IOException any exception from the {@link Reader}.
     */
    public void readFrom( Reader reader ) throws IOException
    {
        readFrom( reader, pivot );
    }

    /**
     * Like {@link #readFrom(Reader)} but with added {@code max} argument for limiting the number of
     * characters read from the {@link Reader}.
     *
     * @see #readFrom(Reader)
     */
    public void readFrom( Reader reader, int max ) throws IOException
    {
        int read = reader.read( buffer, pivot, min( max, pivot ) );
        if ( read == -1 )
        {   // we reached the end
            front = pivot;
        }
        else
        {   // we did read something
            front = pivot + read;
        }
    }

    /**
     * Puts a character into the front section of the buffer and increments the front index.
     * @param ch
     */
    public void append( char ch )
    {
        buffer[front++] = ch;
    }

    /**
     * @return the pivot point of the {@link #array()}. Before the pivot there are characters saved
     * from a previous {@link #compact(SectionedCharBuffer, int) compaction} and after (and including) this point
     * are characters read from {@link #readFrom(Reader)}.
     */
    public int pivot()
    {
        return pivot;
    }

    /**
     * @return index of first available character, might be before pivot point if there have been
     * characters moved over from a previous compaction.
     */
    public int back()
    {
        return back;
    }

    /**
     * @return index of the last available character plus one.
     */
    public int front()
    {
        return front;
    }

    /**
     * @return whether or not there are characters read into the front section of the buffer.
     */
    public boolean hasAvailable()
    {
        return front > pivot;
    }

    /**
     * @return the number of characters available in the front section of the buffer.
     */
    public int available()
    {
        return front-pivot;
    }
}
