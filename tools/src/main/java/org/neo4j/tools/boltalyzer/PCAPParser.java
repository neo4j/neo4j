/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.boltalyzer;


import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.function.ThrowingSupplier;

public class PCAPParser
{
    private static final int PCAP_HEADER = 0xA1B2C3D4;

    public static final int IP_VERSION_AND_LENGTH_OFFSET = 0;
    public static final int IP_SRC_OFFSET = 12;
    public static final int IP_DST_OFFSET = 16;

    public static final int TCP_SRC_PORT_OFFSET = 0;
    public static final int TCP_DST_PORT_OFFSET = 2;


    private static final Map<Integer,PhysicalFormat> physicalFormats = new HashMap<>();
    private static final byte[] IP_BUFFER = new byte[4];


    /** tcpdump supports lots of network types. We implement them as a lambda that takes a tcpdump raw frame strips off the physical transport frame */
    interface PhysicalFormat
    {
        /**
         * Given a stream currently pointing to the beginning of a frame (eg. localhost, ethernet or similar), strip off the physical transport and return
         * the inner data.
         */
        byte[] read( LittleEndianStream in, int length ) throws IOException;
    }

    static
    {
        // http://www.tcpdump.org/linktypes.html

        // BSD Loopback
        physicalFormats.put( 0, ( in, length ) -> {
            in.skip( 4 );
            return in.read( length - 4 );
        } );
    }


    public Stream<Dict> parse( InputStream rawStream ) throws IOException
    {
        LittleEndianStream in = new LittleEndianStream( rawStream );
        // Valid PCAP file starts with a 32-bit integer header
        int i = in.readInt();
        if( i != PCAP_HEADER )
        {
            throw new IOException( "Provided file is not a valid PCAP dump, valid dump files should start with 0x" +
                                   Integer.toHexString( PCAP_HEADER ) + "." );
        }

        // Followed by 16 header bytes I didn't care to look up
        in.skip(16);

        // Followed by the network type
        int networkType = in.readInt();
        PhysicalFormat physicalFormat = physicalFormats.get( networkType );
        if( networkType != 0 )
        {
            throw new IOException( "Don't know how do decode packets from " + Integer.toHexString( networkType ) + " network type. You need to add a physical format parser for this format to PCAPParser." );
        }

        return streamFrom( () -> {

            if( !in.hasMore() )
            {
                return null;
            }

            // PCAP Packet Header: [int32 seconds][int32 ms][int32 frame captured size][int32 actual frame size]
            long timestampSeconds = in.readInt();
            long timestampMicroSeconds = in.readInt();
            int packetSize = in.readInt();
            int actualPacketSize = in.readInt();

            long timestamp = timestampSeconds * 1_000_000 + timestampMicroSeconds;
            if( packetSize != actualPacketSize )
            {
                return null;
            }

            if( packetSize == 0 )
            {
                return null;
            }

            // Read the packet, unwrapped from the physical layer wrapping
            byte[] rawPacket = physicalFormat.read( in, packetSize );

            return parsePacket( rawPacket, timestamp );
        });
    }

    private Stream<Dict> streamFrom( ThrowingSupplier<Dict, IOException> supplier ) throws IOException
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( new Iterator<Dict>()
        {
            private Dict next = supplier.get();

            @Override
            public boolean hasNext()
            {
                return next != null;
            }

            @Override
            public Dict next()
            {
                try
                {
                    Dict current = next;
                    next = supplier.get();
                    return current;
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }, Spliterator.IMMUTABLE ), false );
    }

    private static class LittleEndianStream
    {
        private final InputStream stream;
        private final byte[] intBuffer = new byte[4];
        private final ByteBuffer littleEndianBufferView = ByteBuffer.wrap( intBuffer ).order( ByteOrder.LITTLE_ENDIAN );

        public LittleEndianStream( InputStream stream )
        {
            this.stream = stream;
        }

        private int readInt() throws IOException
        {
            // PCAP is litte-endian, so we need our own int reading method
            int read = stream.read( intBuffer );
            if ( read != 4 )
            {
                throw new IOException( "Expected at least 4 bytes left to read an integer, found " + read );
            }
            littleEndianBufferView.clear();
            return littleEndianBufferView.getInt();
        }

        public void skip( int numBytes ) throws IOException
        {
            if( stream.skip( numBytes ) != numBytes )
            {
                throw new IOException( "EOF" );
            }
        }

        public boolean hasMore() throws IOException
        {
            return stream.available() > 0;
        }

        public byte[] read( int size ) throws IOException
        {
            byte[] data = new byte[size];
            if( stream.read(data) != data.length )
            {
                throw new IOException( "EOF" );
            }
            return data;
        }
    }

    private static Dict parsePacket( byte[] raw, long timestamp ) throws UnknownHostException
    {
        if( raw.length > 0 )
        {
            int tcpPacketOffset = (raw[IP_VERSION_AND_LENGTH_OFFSET] & 0xF) * 4;
            int tcpPayloadOffset = tcpPacketOffset + ((raw[tcpPacketOffset + 12] >> 4) & 0xF) * 4;

            InetAddress src = parseInetAddress( IP_SRC_OFFSET, raw );
            int srcPort = readPort( TCP_SRC_PORT_OFFSET + tcpPacketOffset, raw );

            InetAddress dst = parseInetAddress( IP_DST_OFFSET, raw );
            int dstPort = readPort( TCP_DST_PORT_OFFSET + tcpPacketOffset, raw );

            String srcStr = src.toString() + srcPort;
            String dstStr = dst.toString() + dstPort;
            String connectionKey = srcStr.compareTo( dstStr ) > 0 ? srcStr + dstStr : dstStr + srcStr;

            ByteBuffer payload = ByteBuffer.wrap( raw, tcpPayloadOffset, raw.length - tcpPayloadOffset );
            return new Dict()
                    .put( Fields.timestamp, timestamp )
                    .put( Fields.src, src )
                    .put( Fields.srcPort, srcPort )
                    .put( Fields.dst, dst )
                    .put( Fields.dstPort, dstPort )
                    .put( Fields.payload, payload )
                    .put( Fields.connectionKey, connectionKey );
        }
        else
        {
            return new Dict();
        }
    }

    private static int readPort( int offset, byte[] raw )
    {
        return ((raw[offset] & 0xFF) << 8) | (raw[offset + 1] & 0xFF);
    }

    private static InetAddress parseInetAddress(int offset, byte[] raw) throws UnknownHostException
    {
        System.arraycopy( raw, offset, IP_BUFFER, 0, IP_BUFFER.length );
        return InetAddress.getByAddress( IP_BUFFER );
    }
}