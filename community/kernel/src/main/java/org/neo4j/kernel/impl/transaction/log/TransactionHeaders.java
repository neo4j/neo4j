/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import static java.lang.String.format;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * Represents information read from the extra header of a transaction Start entry. It is presented as a map from
 * integer identifier to byte[], which has a format specific to the handler. The format of the header, all stored in the
 * byte[] "additionalInformation" field of the Start entry is as follows:
 * <p>
 * <pre>
 * 1 byte for format version
 * 1 byte for number of entries that follow
 * for each entry:
 *     1 byte for identifier
 *     4 bytes for data length
 *     byte[] for the header specific data
 * </pre>
 * <p>
 * The above do not include the prefixed 4 bytes that mark the length of the complete array, and which is read by
 * the Start entry parser. It belongs to a level below this class and is irrelevant to its implementation.
 *
 * The creation of the byte array is encapsulated in the {@link TransactionHeadersArrayBuilder}. The parser of the array
 * is encapsulated in this class. The general usage pattern is that when writing out the header one uses the Builder
 * to set it up as required and then pass the resulting array to the {@link TransactionAppender} via a
 * {@link TransactionRepresentation}. Reading back the information requires getting the header array from the
 * {@link TransactionRepresentation} and feeding it into an instance of this class to get the encoded information.
 *
 * The version information kept in the first byte is for future proofing against format changes. Right now there is
 * only one version, the current one, so it is not used anywhere and defaults always to
 * TransactionHeaders.CURRENT_VERSION
 */
public class TransactionHeaders
{
    public static final byte CURRENT_VERSION = 0;
    private static final byte[] DEFAULT_FOR_NON_EXISTING_ID = new byte[0];

    private final byte version;
    private final Map<Byte, byte[]> entries = new HashMap<>();

    public TransactionHeaders( byte[] bytes )
    {
        this.version = extractVersion( bytes );
        extractEntries( bytes );
    }

    /**
     * @return The number of headers discovered and parsed
     */
    public int size()
    {
        return entries.size();
    }

    /**
     * @return The version this array has been encoded with.
     */
    public byte version()
    {
        return version;
    }

    /**
     * @return The byte array belonging to the specified identifier
     */
    public byte[] forIdentifier( byte identifier )
    {
        return entries.containsKey( identifier ) ?
                entries.get( identifier ) :
                DEFAULT_FOR_NON_EXISTING_ID;
    }

    private byte extractVersion( byte[] bytes )
    {
        /*
         * Gracefully handle empty array, default to current version. This is so we can be permissive with what we
         * receive.
         */
        return bytes.length > 0 ? bytes[0] : CURRENT_VERSION;
    }

    private void extractEntries( byte[] bytes )
    {
        if ( bytes.length < 2 ) // either empty array or just with version
        {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap( bytes );
        buffer.position( 1 ); // skip position 0, it's the version
        byte numberOfEntries = buffer.get();
        for ( byte i = 0; i < numberOfEntries; i++ )
        {
            byte currentIdentifier = buffer.get();
            int currentLength = buffer.getInt();

            byte[] content = new byte[currentLength];
            buffer.get( content );
            entries.put( currentIdentifier, content );
        }
        assert buffer.remaining() == 0; // we should have exhausted the array
    }

    public static class TransactionHeadersArrayBuilder
    {
        private Map<Byte, byte[]> entries = new HashMap<>();
        private byte version;
        private int totalLength = 1 + 1; // version and length at least. We need to keep this to allocate the array

        public byte[] build()
        {
            byte[] result = new byte[totalLength];
            ByteBuffer buffer = ByteBuffer.wrap( result );
            buffer.put( version );
            buffer.put( (byte) entries.size() );
            for ( Map.Entry<Byte, byte[]> byteEntry : entries.entrySet() )
            {
                 buffer.put( byteEntry.getKey() );
                byte[] value = byteEntry.getValue();
                buffer.putInt( value.length );
                buffer.put( value );
            }
            return buffer.array();
        }

        public TransactionHeadersArrayBuilder withHeader( byte identifier, byte[] content )
        {
            this.entries.put( identifier, content );
            totalLength += (1 + 4 + content.length); // update count for identifier, content length, actual content
            return this;
        }

        public TransactionHeadersArrayBuilder withVersion( byte version )
        {
            this.version = version;
            return this;
        }
    }
}
