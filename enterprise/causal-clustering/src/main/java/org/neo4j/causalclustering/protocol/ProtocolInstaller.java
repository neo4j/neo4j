/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.protocol;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;

public interface ProtocolInstaller<O extends ProtocolInstaller.Orientation>
{
    abstract class Factory<O extends ProtocolInstaller.Orientation, I extends ProtocolInstaller<O>>
    {
        private final ApplicationProtocol applicationProtocol;
        private final Function<List<ModifierProtocolInstaller<O>>,I> constructor;

        protected Factory( ApplicationProtocol applicationProtocol, Function<List<ModifierProtocolInstaller<O>>,I> constructor )
        {
            this.applicationProtocol = applicationProtocol;
            this.constructor = constructor;
        }

        I create( List<ModifierProtocolInstaller<O>> modifiers )
        {
            return constructor.apply( modifiers );
        }

        public ApplicationProtocol applicationProtocol()
        {
            return applicationProtocol;
        }
    }

    void install( Channel channel ) throws Exception;

    /**
     * For testing
     */
    ApplicationProtocol applicationProtocol();

    /**
     * For testing
     */
    Collection<Collection<Protocol.ModifierProtocol>> modifiers();

    interface Orientation
    {
        interface Server extends Orientation
        {
            String INBOUND = "inbound";
        }

        interface Client extends Orientation
        {
            String OUTBOUND = "outbound";
        }
    }
}
