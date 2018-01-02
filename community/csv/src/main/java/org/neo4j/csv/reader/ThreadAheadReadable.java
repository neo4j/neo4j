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

/**
 * Like an ordinary {@link CharReadable}, it's just that the reading happens in a separate thread, so when
 * a consumer wants to {@link #read(SectionedCharBuffer, int)} more data it's already available, merely a memcopy away.
 */
public class ThreadAheadReadable extends ThreadAhead implements CharReadable
{
    private final CharReadable actual;
    private SectionedCharBuffer theOtherBuffer;

    private String sourceDescription;
    // the variable below is read and changed in both the ahead thread and the caller,
    // but doesn't have to be volatile since it piggy-backs off of hasReadAhead.
    private String newSourceDescription;

    private ThreadAheadReadable( CharReadable actual, int bufferSize )
    {
        super( actual );
        this.actual = actual;
        this.theOtherBuffer = new SectionedCharBuffer( bufferSize );
        this.sourceDescription = actual.sourceDescription();
        start();
    }

    /**
     * The one calling read doesn't actually read, since reading is up to the thread in here.
     * Instead the caller just waits for this thread to have fully read the next buffer and
     * flips over to that buffer, returning it.
     */
    @Override
    public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
    {
        waitUntilReadAhead();

        // flip the buffers
        SectionedCharBuffer resultBuffer = theOtherBuffer;
        buffer.compact( resultBuffer, from );
        theOtherBuffer = buffer;

        // make any change in source official
        if ( newSourceDescription != null )
        {
            sourceDescription = newSourceDescription;
            newSourceDescription = null;
        }

        pokeReader();
        return resultBuffer;
    }

    @Override
    protected boolean readAhead() throws IOException
    {
        theOtherBuffer = actual.read( theOtherBuffer, theOtherBuffer.front() );
        String sourceDescriptionAfterRead = actual.sourceDescription();
        if ( !sourceDescription.equals( sourceDescriptionAfterRead ) )
        {
            newSourceDescription = sourceDescriptionAfterRead;
        }

        return theOtherBuffer.hasAvailable();
    }

    @Override
    public long position()
    {
        return actual.position();
    }

    @Override
    public String sourceDescription()
    {   // Returns the source information of where this reader is perceived to be. The fact that this
        // thing reads ahead should be visible in this description.
        return sourceDescription;
    }

    @Override
    public long lineNumber()
    {   // Generally line numbers aren't tracked at this level of the reading process, let's leave that
        // to CharSeeker for the time being.
        return 1;
    }

    public static CharReadable threadAhead( CharReadable actual, int bufferSize )
    {
        return new ThreadAheadReadable( actual, bufferSize );
    }
}
