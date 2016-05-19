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
package org.neo4j.coreedge.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.state.ByteBufferMarshal;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;

public class CoreMember
{
    private final AdvertisedSocketAddress coreAddress;
    private final AdvertisedSocketAddress raftAddress;
    private final AdvertisedSocketAddress boltAddress;

    public CoreMember( AdvertisedSocketAddress coreAddress, AdvertisedSocketAddress raftAddress,
                       AdvertisedSocketAddress boltAddress)
    {
        this.coreAddress = coreAddress;
        this.raftAddress = raftAddress;
        this.boltAddress = boltAddress;
    }

    @Override
    public String toString()
    {
        return format( "CoreMember{coreAddress=%s, raftAddress=%s, boltAddress=%s}",
                coreAddress, raftAddress, boltAddress );
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
        CoreMember that = (CoreMember) o;
        return Objects.equals( coreAddress, that.coreAddress ) &&
                Objects.equals( raftAddress, that.raftAddress ) &&
                Objects.equals( boltAddress, that.boltAddress );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( coreAddress, raftAddress, boltAddress );
    }

    public AdvertisedSocketAddress getCoreAddress()
    {
        return coreAddress;
    }

    public AdvertisedSocketAddress getRaftAddress()
    {
        return raftAddress;
    }

    public AdvertisedSocketAddress getBoltAddress()
    {
        return boltAddress;
    }

    /**
     * Format:
     * ┌────────────────────────────────────────────┐
     * │core address ┌─────────────────────────────┐│
     * │             │hostnameLength        4 bytes││
     * │             │hostnameBytes        variable││
     * │             │port                  4 bytes││
     * │             └─────────────────────────────┘│
     * │raft address ┌─────────────────────────────┐│
     * │             │hostnameLength        4 bytes││
     * │             │hostnameBytes        variable││
     * │             │port                  4 bytes││
     * │             └─────────────────────────────┘│
     * └────────────────────────────────────────────┘
     * │bolt address ┌─────────────────────────────┐│
     * │             │hostnameLength        4 bytes││
     * │             │hostnameBytes        variable││
     * │             │port                  4 bytes││
     * │             └─────────────────────────────┘│
     * └────────────────────────────────────────────┘
     *
     * <p/>
     * This Marshal implementation can also serialize and deserialize null values. They are encoded as a CoreMember
     * with empty strings in the address fields, so they still adhere to the format displayed above.
     */
    public static class CoreMemberMarshal implements ByteBufferMarshal<CoreMember>,
            ChannelMarshal<CoreMember>,
            ByteBufMarshal<CoreMember>
    {
        private static final AdvertisedSocketAddress NULL_ADDRESS = new AdvertisedSocketAddress( "" );

        final AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal byteBufMarshal =
                new AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal();
        final AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal channelMarshal =
                new AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal();

        public void marshal( CoreMember member, ByteBuffer buffer )
        {
            if ( member == null )
            {
                byteBufMarshal.marshal( NULL_ADDRESS, buffer );
                byteBufMarshal.marshal( NULL_ADDRESS, buffer );
                byteBufMarshal.marshal( NULL_ADDRESS, buffer );
            }
            else
            {
                byteBufMarshal.marshal( member.getCoreAddress(), buffer );
                byteBufMarshal.marshal( member.getRaftAddress(), buffer );
                byteBufMarshal.marshal( member.getBoltAddress(), buffer );
            }
        }

        @Override
        public void marshal( CoreMember member, ByteBuf buffer )
        {
            byteBufMarshal.marshal( member.getCoreAddress(), buffer );
            byteBufMarshal.marshal( member.getRaftAddress(), buffer );
            byteBufMarshal.marshal( member.getBoltAddress(), buffer );
        }

        public CoreMember unmarshal( ByteBuffer buffer )
        {
            AdvertisedSocketAddress coreAddress = byteBufMarshal.unmarshal( buffer );
            AdvertisedSocketAddress raftAddress = byteBufMarshal.unmarshal( buffer );
            AdvertisedSocketAddress boltAddress = byteBufMarshal.unmarshal( buffer );

            return dealWithPossibleNullAddress( coreAddress, raftAddress, boltAddress );
        }

        @Override
        public CoreMember unmarshal( ByteBuf source )
        {
            AdvertisedSocketAddress coreAddress = byteBufMarshal.unmarshal( source );
            AdvertisedSocketAddress raftAddress = byteBufMarshal.unmarshal( source );
            AdvertisedSocketAddress boltAddress = byteBufMarshal.unmarshal( source );
            return dealWithPossibleNullAddress( coreAddress, raftAddress, boltAddress );
        }

        @Override
        public void marshal( CoreMember member, WritableChannel channel ) throws IOException
        {
            if ( member == null )
            {
                channelMarshal.marshal( NULL_ADDRESS, channel );
                channelMarshal.marshal( NULL_ADDRESS, channel );
                channelMarshal.marshal( NULL_ADDRESS, channel );
            }
            else
            {
                channelMarshal.marshal( member.getCoreAddress(), channel );
                channelMarshal.marshal( member.getRaftAddress(), channel );
                channelMarshal.marshal( member.getBoltAddress(), channel );
            }
        }

        @Override
        public CoreMember unmarshal( ReadableChannel source ) throws IOException
        {
            AdvertisedSocketAddress coreAddress = channelMarshal.unmarshal( source );
            AdvertisedSocketAddress raftAddress = channelMarshal.unmarshal( source );
            AdvertisedSocketAddress boltAddress = channelMarshal.unmarshal( source );
            return dealWithPossibleNullAddress( coreAddress, raftAddress, boltAddress );
        }

        private CoreMember dealWithPossibleNullAddress( AdvertisedSocketAddress coreAddress,
                                                        AdvertisedSocketAddress raftAddress,
                                                        AdvertisedSocketAddress boltAddress)
        {
            if ( coreAddress == null ||
                 raftAddress == null ||
                 boltAddress == null ||
                    (coreAddress.equals( NULL_ADDRESS ) &&
                            raftAddress.equals( NULL_ADDRESS ) &&
                            boltAddress.equals( NULL_ADDRESS )) )
            {
                return null;
            }
            else
            {
                return new CoreMember( coreAddress, raftAddress, boltAddress );
            }
        }
    }
}
