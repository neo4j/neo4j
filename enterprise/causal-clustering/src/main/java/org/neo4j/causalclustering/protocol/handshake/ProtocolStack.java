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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;

public class ProtocolStack
{
    private final ApplicationProtocol applicationProtocol;
    private final List<ModifierProtocol> modifierProtocols;

    public ProtocolStack( ApplicationProtocol applicationProtocol, List<ModifierProtocol> modifierProtocols )
    {
        this.applicationProtocol = applicationProtocol;
        this.modifierProtocols = Collections.unmodifiableList( modifierProtocols );
    }

    public ApplicationProtocol applicationProtocol()
    {
        return applicationProtocol;
    }

    public List<ModifierProtocol> modifierProtocols()
    {
        return modifierProtocols;
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
        ProtocolStack that = (ProtocolStack) o;
        return Objects.equals( applicationProtocol, that.applicationProtocol ) && Objects.equals( modifierProtocols, that.modifierProtocols );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( applicationProtocol, modifierProtocols );
    }

    @Override
    public String toString()
    {
        return "ProtocolStack{" + "applicationProtocol=" + applicationProtocol + ", modifierProtocols=" + modifierProtocols + '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ApplicationProtocol applicationProtocol;
        private final List<ModifierProtocol> modifierProtocols = new ArrayList<>();

        private Builder()
        {
        }

        public Builder modifier( ModifierProtocol modifierProtocol )
        {
            modifierProtocols.add( modifierProtocol );
            return this;
        }

        public Builder application( ApplicationProtocol applicationProtocol )
        {
            this.applicationProtocol = applicationProtocol;
            return this;
        }

        ProtocolStack build()
        {
            return new ProtocolStack( applicationProtocol, modifierProtocols );
        }

        @Override
        public String toString()
        {
            return "Builder{" + "applicationProtocol=" + applicationProtocol + ", modifierProtocols=" + modifierProtocols + '}';
        }
    }
}
