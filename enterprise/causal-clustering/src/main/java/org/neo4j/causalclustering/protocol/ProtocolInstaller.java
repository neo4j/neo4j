/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
