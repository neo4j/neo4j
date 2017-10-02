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
package org.neo4j.backup;

import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.kernel.configuration.Config;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.util.Converters.toOptionalHostnamePortFromRawAddress;

public class BackupCommandArgumentHandler
{
    private static final String ARG_NAME_BACKUP_DIRECTORY = "backup-dir";
    private static final String ARG_DESC_BACKUP_DIRECTORY = "Directory to place backup in.";

    private static final String ARG_NAME_BACKUP_NAME = "name";
    private static final String ARG_DESC_BACKUP_NAME =
            "Name of backup. If a backup with this name already exists an incremental backup will be attempted.";

    private static final String ARG_NAME_BACKUP_SOURCE = "from";
    private static final String ARG_DESC_BACKUP_SOURCE = "Host and port of Neo4j.";

    private static final String ARG_NAME_TIMEOUT = "timeout";
    private static final String ARG_DESC_TIMEOUT = "Timeout in the form <time>[ms|s|m|h], where the default unit is seconds.";

    private static final String ARG_NAME_REPORT_DIRECTORY = "cc-report-dir";
    private static final String ARG_DESC_REPORT_DIRECTORY = "Directory where consistency report will be written.";

    private static final String ARG_NAME_ADDITIONAL_CONFIG_DIRECTORY = "additional-config";
    private static final String ARG_DESC_ADDITIONAL_CONFIG_DIRECTORY =
            "Configuration file to supply additional configuration in. This argument is DEPRECATED.";

    private static final String ARG_NAME_FALLBACK_FULL = "fallback-to-full";
    private static final String ARG_DESC_FALLBACK_FULL =
            "If an incremental backup fails backup will move the old backup to <name>.err.<N> and fallback to a full backup instead.";

    private static final String ARG_NAME_CHECK_CONSISTENCY = "check-consistency";
    private static final String ARG_DESC_CHECK_CONSISTENCY = "If a consistency check should be made.";

    private static final String ARG_NAME_CHECK_GRAPH = "cc-graph";
    private static final String ARG_DESC_CHECK_GRAPH =
            "Perform consistency checks between nodes, relationships, properties, types and tokens.";

    private static final String ARG_NAME_CHECK_INDEXES = "cc-indexes";
    private static final String ARG_DESC_CHECK_INDEXES = "Perform consistency checks on indexes.";

    private static final String ARG_NAME_CHECK_LABELS = "cc-label-scan-store";
    private static final String ARG_DESC_CHECK_LABELS = "Perform consistency checks on the label scan store.";

    private static final String ARG_NAME_CHECK_OWNERS = "cc-property-owners";
    private static final String ARG_DESC_CHECK_OWNERS =
            "Perform additional consistency checks on property ownership. This check is *very* expensive in time and memory.";

    private static final Arguments arguments = new Arguments()
        .withArgument( new MandatoryCanonicalPath( ARG_NAME_BACKUP_DIRECTORY,
                "backup-path", ARG_DESC_BACKUP_DIRECTORY ) )
        .withArgument( new MandatoryNamedArg( ARG_NAME_BACKUP_NAME,
                "graph.db-backup", ARG_DESC_BACKUP_NAME ) )
        .withArgument( new OptionalNamedArg( ARG_NAME_BACKUP_SOURCE,
                "address", "localhost:6362", ARG_DESC_BACKUP_SOURCE ) )
        .withArgument( new OptionalBooleanArg( ARG_NAME_FALLBACK_FULL,
                true, ARG_DESC_FALLBACK_FULL ) )
        .withArgument( new OptionalNamedArg( ARG_NAME_TIMEOUT,
                "timeout", "20m", ARG_DESC_TIMEOUT ) )
        .withArgument( new OptionalBooleanArg( ARG_NAME_CHECK_CONSISTENCY,
                true, ARG_DESC_CHECK_CONSISTENCY ) )
        .withArgument( new OptionalCanonicalPath( ARG_NAME_REPORT_DIRECTORY,
                "directory", ".", ARG_DESC_REPORT_DIRECTORY ) )
        .withArgument( new OptionalCanonicalPath( ARG_NAME_ADDITIONAL_CONFIG_DIRECTORY,
                "config-file-path", "", ARG_DESC_ADDITIONAL_CONFIG_DIRECTORY ) )
        .withArgument( new OptionalBooleanArg( ARG_NAME_CHECK_GRAPH,
                true, ARG_DESC_CHECK_GRAPH ) )
        .withArgument( new OptionalBooleanArg( ARG_NAME_CHECK_INDEXES,
                true, ARG_DESC_CHECK_INDEXES ) )
        .withArgument( new OptionalBooleanArg( ARG_NAME_CHECK_LABELS,
                true, ARG_DESC_CHECK_LABELS ) )
        .withArgument( new OptionalBooleanArg( ARG_NAME_CHECK_OWNERS,
                false, ARG_DESC_CHECK_OWNERS ) );

    BackupCommandArgumentHandler()
    {
    }

    public static Arguments arguments()
    {
        return arguments;
    }

    ConsistencyFlags readFlagsFromArgumentsOrDefaultToConfig( Config config ) throws IncorrectUsage
    {
        boolean checkGraph;
        boolean checkIndexes;
        boolean checkLabelScanStore;
        boolean checkPropertyOwners;
        try
        {
            // We can remove the loading from config file in 4.0
            if ( arguments.has( ARG_NAME_CHECK_GRAPH ) )
            {
                checkGraph = arguments.getBoolean( ARG_NAME_CHECK_GRAPH );
            }
            else
            {
                checkGraph = ConsistencyCheckSettings.consistency_check_graph.from( config );
            }
            if ( arguments.has( ARG_NAME_CHECK_INDEXES ) )
            {
                checkIndexes = arguments.getBoolean( ARG_NAME_CHECK_INDEXES );
            }
            else
            {
                checkIndexes = ConsistencyCheckSettings.consistency_check_indexes.from( config );
            }
            if ( arguments.has( ARG_NAME_CHECK_LABELS ) )
            {
                checkLabelScanStore = arguments.getBoolean( ARG_NAME_CHECK_LABELS );
            }
            else
            {
                checkLabelScanStore = ConsistencyCheckSettings.consistency_check_label_scan_store.from( config );
            }
            if ( arguments.has( ARG_NAME_CHECK_OWNERS ) )
            {
                checkPropertyOwners = arguments.getBoolean( ARG_NAME_CHECK_OWNERS );
            }
            else
            {
                checkPropertyOwners = ConsistencyCheckSettings.consistency_check_property_owners.from( config );
            }
            return new ConsistencyFlags( checkGraph, checkIndexes, checkLabelScanStore, checkPropertyOwners );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
    }

    OnlineBackupRequiredArguments establishRequiredArguments( String... args ) throws IncorrectUsage
    {
        try
        {
            arguments.parse( args ); // This is stateful, see implementation
            OptionalHostnamePort address = toOptionalHostnamePortFromRawAddress( arguments.get( ARG_NAME_BACKUP_SOURCE ) );
            Path folder = arguments.getMandatoryPath( ARG_NAME_BACKUP_DIRECTORY );
            String name = arguments.get( ARG_NAME_BACKUP_NAME );
            boolean fallbackToFull = arguments.getBoolean( ARG_NAME_FALLBACK_FULL );
            boolean doConsistencyCheck = arguments.getBoolean( ARG_NAME_CHECK_CONSISTENCY );
            long timeout = arguments.get( ARG_NAME_TIMEOUT, TimeUtil.parseTimeMillis );
            Optional<Path> additionalConfig = arguments.getOptionalPath( ARG_NAME_ADDITIONAL_CONFIG_DIRECTORY );
            Path reportDir = arguments.getOptionalPath( ARG_NAME_REPORT_DIRECTORY ).orElseThrow(
                    () -> new IllegalArgumentException( format( "%s must be a path", ARG_NAME_REPORT_DIRECTORY ) ) );

            return new OnlineBackupRequiredArguments( address, folder, name, fallbackToFull, doConsistencyCheck, timeout, additionalConfig, reportDir );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
    }
}
