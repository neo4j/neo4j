/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.*;

/**
 * Settings for the shell extension
 */
public class ShellSettings
{
    @Default( FALSE )
    public static final GraphDatabaseSetting.BooleanSetting remote_shell_enabled = new GraphDatabaseSetting.BooleanSetting( "remote_shell_enabled" );

    @Default( "1337" )
    public static final PortSetting remote_shell_port = new GraphDatabaseSetting.PortSetting( "remote_shell_port" );

    @Default( FALSE )
    public static final GraphDatabaseSetting.BooleanSetting remote_shell_read_only = new GraphDatabaseSetting.BooleanSetting( "remote_shell_read_only" );

    @Default( "shell" )
    public static final GraphDatabaseSetting.StringSetting remote_shell_name = new GraphDatabaseSetting.StringSetting( "remote_shell_name", ANY, "Must be a valid name" );
}
