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
package org.neo4j.server.enterprise;

import java.time.Duration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.setting;

@Description( "Settings available in the Enterprise server" )
public class EnterpriseServerSettings implements LoadableConfig
{
    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Configure the Neo4j Browser to time out logged in users after this idle period. " +
                  "Setting this to 0 indicates no limit." )
    public static final Setting<Duration> browser_credentialTimeout = setting( "browser.credential_timeout", DURATION,
            "0" );

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Configure the Neo4j Browser to store or not store user credentials." )
    public static final Setting<Boolean> browser_retainConnectionCredentials =
            setting( "browser.retain_connection_credentials", BOOLEAN, TRUE );

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Configure the policy for outgoing Neo4j Browser connections." )
    public static final Setting<Boolean> browser_allowOutgoingBrowserConnections =
            setting( "browser.allow_outgoing_connections", BOOLEAN, TRUE );
}
