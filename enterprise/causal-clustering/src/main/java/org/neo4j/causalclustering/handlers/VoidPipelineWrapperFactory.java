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
        return VOID_WRAPPER;
    }

    @Override
    public PipelineWrapper forClient( Config config, Dependencies dependencies, LogProvider logProvider, Setting<String> policyName )
    {
        return VOID_WRAPPER;
    }
}
