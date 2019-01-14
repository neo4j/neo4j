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
package org.neo4j.shell;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.ANY;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.illegalValueMessage;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.port;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for the shell extension
 */
@Description( "Settings for the remote shell extension" )
public class ShellSettings implements LoadableConfig
{
    @Description( "Enable a remote shell server which Neo4j Shell clients can log in to. Only applicable to `neo4j-shell`." )
    public static final Setting<Boolean> remote_shell_enabled = setting( "dbms.shell.enabled", BOOLEAN, FALSE );

    @Description( "Remote host for shell. By default, the shell server listens only on the loopback interface, " +
            "but you can specify the IP address of any network interface or use `0.0.0.0` for all interfaces. Only applicable to `neo4j-shell`." )
    public static final Setting<String> remote_shell_host = buildSetting( "dbms.shell.host", STRING, "127.0.0.1" ).constraint(
            illegalValueMessage( "must be a valid name", matches( ANY ) ) ).build();

    @Description( "The port the shell will listen on. Only applicable to `neo4j-shell`." )
    public static final Setting<Integer> remote_shell_port =
            buildSetting( "dbms.shell.port", INTEGER, "1337" ).constraint( port ).build();

    @Description( "Read only mode. Will only allow read operations. Only applicable to `neo4j-shell`." )
    public static final Setting<Boolean> remote_shell_read_only = setting( "dbms.shell.read_only", BOOLEAN, FALSE );

    @Description( "The name of the shell. Only applicable to `neo4j-shell`." )
    public static final Setting<String> remote_shell_name = buildSetting( "dbms.shell.rmi_name", STRING, "shell" ).constraint(
            illegalValueMessage( "must be a valid name", matches( ANY ) ) ).build();
}
