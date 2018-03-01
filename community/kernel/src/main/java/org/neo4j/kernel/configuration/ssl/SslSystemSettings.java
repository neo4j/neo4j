/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.List;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * System-wide settings for SSL.
 */
@Description( "System-wide settings for SSL." )
public class SslSystemSettings implements LoadableConfig
{
    static final String TLS_PROTOCOL_DEFAULT_KEY = "org.neo4j.tls.protocol.default";
    private static final String TLS_DEFAULT = "TLSv1.2";

    @Description( "Netty SSL provider" )
    public static final Setting<SslProvider> netty_ssl_provider = setting( "dbms.netty.ssl.provider", options( SslProvider.class ), SslProvider.JDK.name() );

    static List<String> getTlsDefault()
    {
        String protocolProperty = System.getProperty( TLS_PROTOCOL_DEFAULT_KEY, TLS_DEFAULT ).trim();
        return asList( protocolProperty.split( "\\s*,\\s*" ) );
    }
}
