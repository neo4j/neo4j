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
package org.neo4j.server.enterprise;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.setting;

@Description("Settings available in the Enterprise server")
public interface EnterpriseServerSettings
{
    @Description( "Configure the Neo4j Browser to time out logged in users after this idle period. " +
                  "Setting this to 0 indicates no limit." )
    Setting<Long> browser_credentialTimeout = setting( "dbms.browser.credential_timeout", DURATION, "0" );

    @Description( "Configure the Neo4j Browser to store or not store user credentials." )
    Setting<Boolean> browser_storeCredentials = setting( "dbms.browser.store_credentials", BOOLEAN, TRUE );

    @Description( "Configure the operating mode of the database -- 'SINGLE' for stand-alone operation or 'HA' " +
                  "for operating as a member in a cluster." )
    Setting<String> mode = setting( "org.neo4j.server.database.mode", STRING, "SINGLE" );

    @Description( "Whitelist of hosts for the Neo4j Browser to be allowed to fetch content from." )
    Setting<String> browser_remoteContentHostnameWhitelist = setting( "dbms.browser.remote_content_hostname_whitelist", STRING, "http://guides.neo4j.com,https://guides.neo4j.com,http://localhost,https://localhost" );

    @Description( "Configure the policy for outgoing Neo4j Browser connections." )
    Setting<Boolean> browser_allowOutgoingBrowserConnections = setting( "dbms.security.allow_outgoing_browser_connections", BOOLEAN, TRUE );
}
