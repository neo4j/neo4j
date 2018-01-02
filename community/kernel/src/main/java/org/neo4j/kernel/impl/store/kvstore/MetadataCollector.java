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
package org.neo4j.kernel.impl.store.kvstore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

abstract class MetadataCollector extends Metadata implements EntryVisitor<BigEndianByteArrayBuffer>
{
    private static final byte[] NO_DATA = new byte[0];
    private final int entriesPerPage;
    private final HeaderField<?>[] headerFields;
    private final Map<HeaderField<?>, Integer> headerIndexes = new HashMap<>();
    private final Object[] headerValues;
    private int header, data;
    private State state = State.expecting_format_specifier;
    private byte[] catalogue = NO_DATA;

    public MetadataCollector( int entriesPerPage, HeaderField<?>[] headerFields )
    {
        this.entriesPerPage = entriesPerPage;
        this.headerFields = headerFields = headerFields.clone();
        this.headerValues = new Object[headerFields.length];
        for ( int i = 0; i < headerFields.length; i++ )
        {
            headerIndexes.put( requireNonNull( headerFields[i], "header field" ), i );
        }
    }

    @Override
    public String toString()
    {
        return "MetadataCollector[" + state + "]";
    }

    @Override
    public Headers headers()
    {
        return Headers.indexedHeaders( headerIndexes, headerValues.clone() );
    }

    @Override
    public final boolean visit( BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
    {
        return state.visit( this, key, value );
    }

    private void readHeader( int offset, BigEndianByteArrayBuffer value )
    {
        headerValues[offset] = headerFields[offset].read( value );
    }

    private void readData( BigEndianByteArrayBuffer key )
    {
        if ( ((header + data) % entriesPerPage) == 1 || data == 1 )
        { // first entry in (a new) page, extend the catalogue
            int oldLen = catalogue.length;
            catalogue = Arrays.copyOf( catalogue, oldLen + 2 * key.size() );
            key.dataTo( catalogue, oldLen ); // write the first key of the page into the catalogue
        }
        // always update the catalogue with the last entry (seen) for the page
        key.dataTo( catalogue, catalogue.length - key.size() );
    }

    abstract boolean verifyFormatSpecifier( ReadableBuffer value );

    @Override
    byte[] pageCatalogue()
    {
        return catalogue;
    }

    @Override
    int headerEntries()
    {
        return header;
    }

    @Override
    int totalEntries()
    {
        return header + data;
    }

    private enum State
    {
        expecting_format_specifier
        {
            @Override
            boolean visit( MetadataCollector collector, BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
            {
                return readFormatSpecifier( collector, key, value );
            }
        },
        expecting_header
        {
            @Override
            boolean visit( MetadataCollector collector, BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
            {
                if ( !key.allZeroes() )
                {
                    throw new IllegalStateException(
                            "Expecting at least one header after the format specifier." );
                }
                if ( value.allZeroes() )
                {
                    int header = ++collector.header;
                    assert header == 2
                            : "End-of-header markers are always the second header after the format specifier.";
                    if ( collector.headerFields.length > 0 )
                    {
                        throw new IllegalStateException( "Expected " + collector.headerFields.length +
                                                         " header fields, none seen." );
                    }
                    collector.state = reading_data;
                    return true;
                }
                else
                {
                    return (collector.state = reading_header).visit( collector, key, value );
                }
            }
        },
        reading_header
        {
            @Override
            boolean visit( MetadataCollector collector, BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
            {
                if ( key.allZeroes() )
                {
                    if ( value.minusOneAtTheEnd() )
                    {
                        collector.state = done;
                        return false;
                    }
                    if ( collector.header > collector.headerFields.length )
                    {
                        throw new IllegalStateException( "Too many header fields, expected only "
                                                         + collector.headerFields.length );
                    }
                    int header = collector.header - 1;
                    collector.header++;
                    collector.readHeader( header, value );
                    return true;
                }
                else
                {
                    if ( collector.headerFields.length >= collector.header )
                    {
                        throw new IllegalStateException( "Expected " + collector.headerFields.length +
                                                         " header fields, only " + (collector.header - 1) + " seen." );
                    }
                    return (collector.state = reading_data).visit( collector, key, value );
                }
            }
        },
        reading_data
        {
            @Override
            boolean visit( MetadataCollector collector, BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
            {
                if ( key.allZeroes() )
                {
                    long encodedEntries = value.getIntegerFromEnd();
                    long entries = encodedEntries == -1 ? 0 : encodedEntries;
                    if ( entries != collector.data )
                    {
                        collector.state = in_error;
                        throw new IllegalStateException( "Number of data entries does not match. (counted=" +
                                                         collector.data + ", trailer=" + entries + ")" );
                    }
                    collector.state = done;
                    return false;
                }
                else
                {
                    collector.data++;
                    collector.readData( key );
                    return true;
                }
            }
        },
        done
        {
            @Override
            boolean visit( MetadataCollector collector, BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
            {
                throw new IllegalStateException( "Metadata collection has completed." );
            }
        },
        in_error
        {
            @Override
            boolean visit( MetadataCollector collector, BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
            {
                throw new IllegalStateException( "Metadata collection has failed." );
            }
        };

        abstract boolean visit( MetadataCollector collector,
                BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value );

        private static boolean readFormatSpecifier( MetadataCollector collector,
                BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value )
        {
            if ( !key.allZeroes() )
            {
                throw new IllegalStateException( "Expecting a valid format specifier." );
            }
            if ( !collector.verifyFormatSpecifier( value ) )
            {
                collector.state = in_error;
                throw new IllegalStateException( "Format header/trailer has changed." );
            }

            try
            {
                collector.header = 1;
                return true;
            }
            finally
            {
                collector.state = expecting_header;
            }
        }
    }
}
