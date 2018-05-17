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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BackupHelpOutput
{
    public static final List<String> BACKUP_OUTPUT_LINES = backupOutput();

    private static List<String> backupOutput()
    {
        List<String> lines = new ArrayList<>();

        lines.add( "usage: neo4j-admin backup --backup-dir=<backup-path> --name=<graph.db-backup>" );
        lines.add( "                          [--from=<address>] [--protocol=<any|catchup|common>]" );
        lines.add( "                          [--fallback-to-full[=<true|false>]]" );
        lines.add( "                          [--timeout=<timeout>] [--pagecache=<8m>]" );
        lines.add( "                          [--check-consistency[=<true|false>]]" );
        lines.add( "                          [--cc-report-dir=<directory>]" );
        lines.add( "                          [--additional-config=<config-file-path>]" );
        lines.add( "                          [--cc-graph[=<true|false>]]" );
        lines.add( "                          [--cc-indexes[=<true|false>]]" );
        lines.add( "                          [--cc-label-scan-store[=<true|false>]]" );
        lines.add( "                          [--cc-property-owners[=<true|false>]]" );
        lines.add( "" );
        lines.add( "environment variables:" );
        lines.add( "    NEO4J_CONF    Path to directory which contains neo4j.conf." );
        lines.add( "    NEO4J_DEBUG   Set to anything to enable debug output." );
        lines.add( "    NEO4J_HOME    Neo4j home directory." );
        lines.add( "    HEAP_SIZE     Set JVM maximum heap size during command execution." );
        lines.add( "                  Takes a number and a unit, for example 512m." );
        lines.add( "" );
        lines.add( "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup" );
        lines.add( "service must have been configured on the server beforehand." );
        lines.add( "" );
        lines.add( "All consistency checks except 'cc-graph' can be quite expensive so it may be" );
        lines.add( "useful to turn them off for very large databases. Increasing the heap size can" );
        lines.add( "also be a good idea. See 'neo4j-admin help' for details." );
        lines.add( "" );
        lines.add( "For more information see:" );
        lines.add( "https://neo4j.com/docs/operations-manual/current/backup/" );
        lines.add( "" );
        lines.add( "options:" );
        lines.add( "  --backup-dir=<backup-path>               Directory to place backup in." );
        lines.add( "  --name=<graph.db-backup>                 Name of backup. If a backup with this" );
        lines.add( "                                           name already exists an incremental" );
        lines.add( "                                           backup will be attempted." );
        lines.add( "  --from=<address>                         Host and port of Neo4j." );
        lines.add( "                                           [default:localhost:6362]" );
        lines.add( "  --protocol=<any|catchup|common>          Preferred backup protocol" );
        lines.add( "                                           [default:any]" );
        lines.add( "  --fallback-to-full=<true|false>          If an incremental backup fails backup" );
        lines.add( "                                           will move the old backup to" );
        lines.add( "                                           <name>.err.<N> and fallback to a full" );
        lines.add( "                                           backup instead. [default:true]" );
        lines.add( "  --timeout=<timeout>                      Timeout in the form <time>[ms|s|m|h]," );
        lines.add( "                                           where the default unit is seconds." );
        lines.add( "                                           [default:20m]" );
        lines.add( "  --pagecache=<8m>                         The size of the page cache to use for" );
        lines.add( "                                           the backup process. [default:8m]" );
        lines.add( "  --check-consistency=<true|false>         If a consistency check should be" );
        lines.add( "                                           made. [default:true]" );
        lines.add( "  --cc-report-dir=<directory>              Directory where consistency report" );
        lines.add( "                                           will be written. [default:.]" );
        lines.add( "  --additional-config=<config-file-path>   Configuration file to supply" );
        lines.add( "                                           additional configuration in. This" );
        lines.add( "                                           argument is DEPRECATED. [default:]" );
        lines.add( "  --cc-graph=<true|false>                  Perform consistency checks between" );
        lines.add( "                                           nodes, relationships, properties," );
        lines.add( "                                           types and tokens. [default:true]" );
        lines.add( "  --cc-indexes=<true|false>                Perform consistency checks on" );
        lines.add( "                                           indexes. [default:true]" );
        lines.add( "  --cc-label-scan-store=<true|false>       Perform consistency checks on the" );
        lines.add( "                                           label scan store. [default:true]" );
        lines.add( "  --cc-property-owners=<true|false>        Perform additional consistency checks" );
        lines.add( "                                           on property ownership. This check is" );
        lines.add( "                                           *very* expensive in time and memory." );
        lines.add( "                                           [default:false]" );
        String platformNewLine = System.lineSeparator();
        lines = lines.stream().map( line -> line += platformNewLine ).collect( Collectors.toList() );
        return lines;
    }
}

