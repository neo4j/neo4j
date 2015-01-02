/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.shell;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;

import static org.neo4j.helpers.Settings.ANY;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.FALSE;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.port;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for the shell extension
 */
@Description( "Settings for the remote shell extension" )
public class ShellSettings
{
    @Description("Enable a remote shell server which shell clients can log in to")
    public static final Setting<Boolean> remote_shell_enabled = setting( "remote_shell_enabled", BOOLEAN, FALSE );

    public static final Setting<String> remote_shell_host = setting( "remote_shell_host", STRING, "127.0.0.1",
            illegalValueMessage( "must be a valid name", matches( ANY ) ) );

    public static final Setting<Integer> remote_shell_port = setting( "remote_shell_port", INTEGER, "1337", port );

    public static final Setting<Boolean> remote_shell_read_only = setting( "remote_shell_read_only", BOOLEAN, FALSE );

    public static final Setting<String> remote_shell_name = setting( "remote_shell_name", STRING, "shell",
            illegalValueMessage( "must be a valid name", matches( ANY ) ) );
}
