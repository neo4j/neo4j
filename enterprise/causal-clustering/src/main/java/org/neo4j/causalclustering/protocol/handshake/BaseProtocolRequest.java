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
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;
import java.util.Set;

public abstract class BaseProtocolRequest<IMPL extends Comparable<IMPL>> implements ServerMessage
{
    private final String protocolName;
    private final Set<IMPL> versions;

    BaseProtocolRequest( String protocolName, Set<IMPL> versions )
    {
        this.protocolName = protocolName;
        this.versions = versions;
    }

    public String protocolName()
    {
        return protocolName;
    }

    public Set<IMPL> versions()
    {
        return versions;
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
        BaseProtocolRequest that = (BaseProtocolRequest) o;
        return Objects.equals( protocolName, that.protocolName ) && Objects.equals( versions, that.versions );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( protocolName, versions );
    }

    @Override
    public String toString()
    {
        return "BaseProtocolRequest{" + "protocolName='" + protocolName + '\'' + ", versions=" + versions + '}';
    }
}
