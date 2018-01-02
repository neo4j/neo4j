/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.legacy.consistency;

import java.io.File;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.legacy.consistency.checking.full.TaskExecutionOrder;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for consistency checker
 */
@Description( "Consistency check configuration settings" )
public class ConsistencyCheckSettings
{
    @Description("Perform optional additional checking on property ownership. " +
            "This can detect a theoretical inconsistency where a property could be owned by multiple entities. " +
            "However, the check is very expensive in time and memory, so it is skipped by default.")
    public static final Setting<Boolean> consistency_check_property_owners = setting( "consistency_check_property_owners", BOOLEAN, FALSE );

    @Description("Perform checks on the label scan store. Checking this store is more expensive than " +
            "checking the native stores, so it may be useful to turn off this check for very large databases.")
    public static final Setting<Boolean> consistency_check_label_scan_store = setting( "consistency_check_label_scan_store", BOOLEAN, TRUE );

    @Description("Perform checks on indexes. Checking indexes is more expensive than " +
            "checking the native stores, so it may be useful to turn off this check for very large databases.")
    public static final Setting<Boolean> consistency_check_indexes = setting( "consistency_check_indexes", BOOLEAN, TRUE );

    @Description( "Perform checks between nodes, relationships, properties, types and tokens." )
    public static final Setting<Boolean> consistency_check_graph = setting( "consistency_check_graph", BOOLEAN, TRUE );

    @Description( "Execution order of store cross-checks to be used when running consistency check" )
    public static final Setting<TaskExecutionOrder> consistency_check_execution_order =
            setting( "consistency_check_execution_order", options( TaskExecutionOrder.class ), TaskExecutionOrder.MULTI_PASS.name() );

    @SuppressWarnings("unchecked")
    @Description("File name for inconsistencies log file. If not specified, logs to a file in the store directory.")
    public static final
    Setting<File> consistency_check_report_file = setting( "consistency_check_report_file", PATH, NO_DEFAULT );
}
