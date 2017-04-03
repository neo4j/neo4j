/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.commandline.admin;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Neo4jAdminUsageTest
{
    private Usage usageCmd;

    @Before
    public void setup()
    {
        usageCmd = new Usage( AdminTool.scriptName, CommandLocator.fromServiceLocator() );
    }

    @Test
    public void verifyUsageMatchesExpectedCommands() throws Exception
    {
        final StringBuilder sb = new StringBuilder();
        usageCmd.print( s -> sb.append( s ).append( "\n" ) );

        assertEquals("usage: neo4j-admin <command>\n" +
                        "\n" +
                        "Manage your Neo4j instance.\n" +
                        "\n" +
                        "environment variables:\n" +
                        "    NEO4J_CONF    Path to directory which contains neo4j.conf.\n" +
                        "    NEO4J_DEBUG   Set to anything to enable debug output.\n" +
                        "    NEO4J_HOME    Neo4j home directory.\n" +
                        "    HEAP_SIZE     Set size of JVM heap during command execution.\n" +
                        "                  Takes a number and a unit, for example 512m.\n" +
                        "\n" +
                        "available commands:\n" +
                        "\n" +
                        "General\n" +
                        "    check-consistency\n" +
                        "        Check the consistency of a database.\n" +
                        "    import\n" +
                        "        Import from a collection of CSV files or a pre-3.0 database.\n" +
                        "    store-info\n" +
                        "        Prints information about a Neo4j database store.\n" +
                        "\n" +
                        "Authentication\n" +
                        "    set-default-admin\n" +
                        "        Sets the default admin user when no roles are present.\n" +
                        "    set-initial-password\n" +
                        "        Sets the initial password of the initial admin user ('neo4j').\n" +
                        "\n" +
                        "Clustering\n" +
                        "    unbind\n" +
                        "        Removes cluster state data for the specified database.\n" +
                        "\n" +
                        "Offline backup\n" +
                        "    dump\n" +
                        "        Dump a database into a single-file archive.\n" +
                        "    load\n" +
                        "        Load a database from an archive created with the dump command.\n" +
                        "\n" +
                        "Online backup\n" +
                        "    backup\n" +
                        "        Perform an online backup from a running Neo4j enterprise server.\n" +
                        "    restore\n" +
                        "        Restore a backed up database.\n" +
                        "\n" +
                        "Use neo4j-admin help <command> for more details.\n",
                sb.toString() );
    }
}
