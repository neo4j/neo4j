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

import java.nio.ByteBuffer;
import java.util.Objects;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.state.membership.Marshal;

import static java.lang.String.format;

public class CoreMember
{
    private final AdvertisedSocketAddress coreAddress;
    private final AdvertisedSocketAddress raftAddress;

    public CoreMember( AdvertisedSocketAddress coreAddress, AdvertisedSocketAddress raftAddress )
    {
        this.coreAddress = coreAddress;
        this.raftAddress = raftAddress;
    }

    @Override
    public String toString()
    {
        return format( "CoreMember{coreAddress=%s, raftAddress=%s}", coreAddress, raftAddress );
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
                Objects.equals( raftAddress, that.raftAddress );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( coreAddress, raftAddress );
    }

    public AdvertisedSocketAddress getCoreAddress()
    {
        return coreAddress;
    }

    public AdvertisedSocketAddress getRaftAddress()
    {
        return raftAddress;
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
     *
     * This Marshal implementation can also serialize and deserialize null values. They are encoded as a CoreMember
     * with empty strings in the address fields, so they still adhere to the format displayed above.
     */
    public static class CoreMemberMarshal implements Marshal<CoreMember>
    {
        private final static AdvertisedSocketAddress NULL_ADDRESS = new AdvertisedSocketAddress( "" );

        final AdvertisedSocketAddress.AdvertisedSocketAddressMarshal marshal = new AdvertisedSocketAddress
                .AdvertisedSocketAddressMarshal();

        public void marshal( CoreMember member, ByteBuffer buffer )
        {
            if ( member == null )
            {
                marshal.marshal( NULL_ADDRESS, buffer );
                marshal.marshal( NULL_ADDRESS, buffer );
            }
            else
            {
                marshal.marshal( member.getCoreAddress(), buffer );
                marshal.marshal( member.getRaftAddress(), buffer );
            }
        }

        public void marshal( CoreMember member, ByteBuf buffer )
        {
            marshal.marshal( member.getCoreAddress(), buffer );
            marshal.marshal( member.getRaftAddress(), buffer );
        }

        public CoreMember unmarshal( ByteBuffer buffer )
        {
            AdvertisedSocketAddress coreAddress = marshal.unmarshal( buffer );
            AdvertisedSocketAddress raftAddress = marshal.unmarshal( buffer );

            return dealWithPossibleNullAddress( coreAddress, raftAddress );
        }

        public CoreMember unmarshal( ByteBuf buffer )
        {
            AdvertisedSocketAddress coreAddress = marshal.unmarshal( buffer );
            AdvertisedSocketAddress raftAddress = marshal.unmarshal( buffer );
            return dealWithPossibleNullAddress( coreAddress, raftAddress );
        }

        private CoreMember dealWithPossibleNullAddress( AdvertisedSocketAddress coreAddress, AdvertisedSocketAddress
                raftAddress )
        {
            if ( coreAddress == null || raftAddress == null || (coreAddress.equals( NULL_ADDRESS ) && raftAddress.equals( NULL_ADDRESS ) ) )
            {
                return null;
            }
            else
            {
                return new CoreMember( coreAddress, raftAddress );
            }
        }
    }
}

