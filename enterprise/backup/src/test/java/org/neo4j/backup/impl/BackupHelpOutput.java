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
package org.neo4j.backup.impl;

public class BackupHelpOutput
{
    public static final String BACKUP_OUTPUT = backupOutput();

    private static String backupOutput()
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append( "usage: neo4j-admin backup --backup-dir=<backup-path> --name=<graph.db-backup>\n" );
        stringBuilder.append( "                          [--from=<address>] [--fallback-to-full[=<true|false>]]\n" );
        stringBuilder.append( "                          [--timeout=<timeout>] [--pagecache=<8m>]\n" );
        stringBuilder.append( "                          [--check-consistency[=<true|false>]]\n" );
        stringBuilder.append( "                          [--cc-report-dir=<directory>]\n" );
        stringBuilder.append( "                          [--additional-config=<config-file-path>]\n" );
        stringBuilder.append( "                          [--cc-graph[=<true|false>]]\n" );
        stringBuilder.append( "                          [--cc-indexes[=<true|false>]]\n" );
        stringBuilder.append( "                          [--cc-label-scan-store[=<true|false>]]\n" );
        stringBuilder.append( "                          [--cc-property-owners[=<true|false>]]\n" );
        stringBuilder.append( "\n" );
        stringBuilder.append( "environment variables:\n" );
        stringBuilder.append( "    NEO4J_CONF    Path to directory which contains neo4j.conf.\n" );
        stringBuilder.append( "    NEO4J_DEBUG   Set to anything to enable debug output.\n" );
        stringBuilder.append( "    NEO4J_HOME    Neo4j home directory.\n" );
        stringBuilder.append( "    HEAP_SIZE     Set size of JVM heap during command execution.\n" );
        stringBuilder.append( "                  Takes a number and a unit, for example 512m.\n" );
        stringBuilder.append( "\n" );
        stringBuilder.append( "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup\n" );
        stringBuilder.append( "service must have been configured on the server beforehand.\n" );
        stringBuilder.append( "\n" );
        stringBuilder.append( "All consistency checks except 'cc-graph' can be quite expensive so it may be\n" );
        stringBuilder.append( "useful to turn them off for very large databases. Increasing the heap size can\n" );
        stringBuilder.append( "also be a good idea. See 'neo4j-admin help' for details.\n" );
        stringBuilder.append( "\n" );
        stringBuilder.append( "For more information see:\n" );
        stringBuilder.append( "https://neo4j.com/docs/operations-manual/current/backup/\n" );
        stringBuilder.append( "\n" );
        stringBuilder.append( "options:\n" );
        stringBuilder.append( "  --backup-dir=<backup-path>               Directory to place backup in.\n" );
        stringBuilder.append( "  --name=<graph.db-backup>                 Name of backup. If a backup with this\n" );
        stringBuilder.append( "                                           name already exists an incremental\n" );
        stringBuilder.append( "                                           backup will be attempted.\n" );
        stringBuilder.append( "  --from=<address>                         Host and port of Neo4j.\n" );
        stringBuilder.append( "                                           [default:localhost:6362]\n" );
        stringBuilder.append( "  --fallback-to-full=<true|false>          If an incremental backup fails backup\n" );
        stringBuilder.append( "                                           will move the old backup to\n" );
        stringBuilder.append( "                                           <name>.err.<N> and fallback to a full\n" );
        stringBuilder.append( "                                           backup instead. [default:true]\n" );
        stringBuilder.append( "  --timeout=<timeout>                      Timeout in the form <time>[ms|s|m|h],\n" );
        stringBuilder.append( "                                           where the default unit is seconds.\n" );
        stringBuilder.append( "                                           [default:20m]\n" );
        stringBuilder.append( "  --pagecache=<8m>                         The size of the page cache to use for\n" );
        stringBuilder.append( "                                           the backup process. [default:8m]\n" );
        stringBuilder.append( "  --check-consistency=<true|false>         If a consistency check should be\n" );
        stringBuilder.append( "                                           made. [default:true]\n" );
        stringBuilder.append( "  --cc-report-dir=<directory>              Directory where consistency report\n" );
        stringBuilder.append( "                                           will be written. [default:.]\n" );
        stringBuilder.append( "  --additional-config=<config-file-path>   Configuration file to supply\n" );
        stringBuilder.append( "                                           additional configuration in. This\n" );
        stringBuilder.append( "                                           argument is DEPRECATED. [default:]\n" );
        stringBuilder.append( "  --cc-graph=<true|false>                  Perform consistency checks between\n" );
        stringBuilder.append( "                                           nodes, relationships, properties,\n" );
        stringBuilder.append( "                                           types and tokens. [default:true]\n" );
        stringBuilder.append( "  --cc-indexes=<true|false>                Perform consistency checks on\n" );
        stringBuilder.append( "                                           indexes. [default:true]\n" );
        stringBuilder.append( "  --cc-label-scan-store=<true|false>       Perform consistency checks on the\n" );
        stringBuilder.append( "                                           label scan store. [default:true]\n" );
        stringBuilder.append( "  --cc-property-owners=<true|false>        Perform additional consistency checks\n" );
        stringBuilder.append( "                                           on property ownership. This check is\n" );
        stringBuilder.append( "                                           *very* expensive in time and memory.\n" );
        stringBuilder.append( "                                           [default:false]\n" );
        return stringBuilder.toString();
    }
}

