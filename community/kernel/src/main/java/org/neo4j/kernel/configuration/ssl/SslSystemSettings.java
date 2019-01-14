/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.configuration.ssl;

import io.netty.handler.ssl.SslProvider;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * System-wide settings for SSL.
 */
@Description( "System-wide settings for SSL." )
public class SslSystemSettings implements LoadableConfig
{
    @Description( "Netty SSL provider" )
    public static final Setting<SslProvider> netty_ssl_provider = setting( "dbms.netty.ssl.provider", options( SslProvider.class ), SslProvider.JDK.name() );
}
