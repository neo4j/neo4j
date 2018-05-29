/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;

public class VoidPipelineWrapperFactory implements DuplexPipelineWrapperFactory
{
    public static final PipelineWrapper VOID_WRAPPER = new PipelineWrapper()
    {
        @Override
        public List<ChannelHandler> handlersFor( Channel channel )
        {
            return emptyList();
        }

        @Override
        public String name()
        {
            return "void";
        }
    };

    @Override
    public PipelineWrapper forServer( Config config, Dependencies dependencies, LogProvider logProvider, Setting<String> policyName )
    {
        verifyNoEncryption( config, policyName );
        return VOID_WRAPPER;
    }

    @Override
    public PipelineWrapper forClient( Config config, Dependencies dependencies, LogProvider logProvider, Setting<String> policyName )
    {
        verifyNoEncryption( config, policyName );
        return VOID_WRAPPER;
    }

    private static void verifyNoEncryption( Config config, Setting<String> policyName )
    {
        assert config.get( policyName ) == null : "Unexpected SSL policy " + policyName;
    }
}
