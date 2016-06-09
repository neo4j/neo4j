/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;

import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for security module
 */
@Description( "Security configuration settings" )
public class SecuritySettings
{
    @SuppressWarnings( "unused" ) // accessed by reflection

    @Description( "Hostname and port of LDAP server to use for authentication and authorization." )
    public static final Setting<HostnamePort> ldap_server =
            setting( "dbms.security.ldap.host", HOSTNAME_PORT, "0.0.0.0:389" );

    @Description( "Authentication mechanism." )
    public static final Setting<String> ldap_auth_mechanism =
            setting( "dbms.security.ldap.auth_mechanism", STRING, "simple" );

    @Description( "Referral" )
    public static final Setting<String> ldap_referral =
            setting( "dbms.security.ldap.referral", STRING, "follow" );


    @Description( "User DN template." )
    public static final Setting<String> ldap_user_dn_template =
            setting( "dbms.security.ldap.user_dn_template", STRING, "uid={0},ou=users,dc=example,dc=com" );

    // Or use "sAMAccountName={0}" as default?

}
