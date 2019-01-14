/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com;

public final class ProtocolVersion implements Comparable<ProtocolVersion>
{
    public static final byte INTERNAL_PROTOCOL_VERSION = 2;

    private final byte applicationProtocol;
    private final byte internalProtocol;

    public ProtocolVersion( byte applicationProtocol, byte internalProtocol )
    {
        this.applicationProtocol = applicationProtocol;
        this.internalProtocol = internalProtocol;
    }

    public byte getApplicationProtocol()
    {
        return applicationProtocol;
    }

    public byte getInternalProtocol()
    {
        return internalProtocol;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null )
        {
            return false;
        }
        if ( obj.getClass() != ProtocolVersion.class )
        {
            return false;
        }
        ProtocolVersion other = (ProtocolVersion) obj;
        return (other.applicationProtocol == applicationProtocol) && (other.internalProtocol == internalProtocol);
    }

    @Override
    public int hashCode()
    {
        return (31 * applicationProtocol) | internalProtocol;
    }

    @Override
    public int compareTo( ProtocolVersion that )
    {
        return Byte.compare( this.applicationProtocol, that.applicationProtocol );
    }

    @Override
    public String toString()
    {
        return "ProtocolVersion{" +
                "applicationProtocol=" + applicationProtocol +
                ", internalProtocol=" + internalProtocol +
                '}';
    }
}
