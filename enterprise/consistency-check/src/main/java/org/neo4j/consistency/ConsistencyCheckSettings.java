/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency;

import java.io.File;

import org.neo4j.consistency.checking.full.TaskExecutionOrder;
import org.neo4j.consistency.store.windowpool.WindowPoolImplementation;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.FALSE;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.basePath;
import static org.neo4j.helpers.Settings.options;
import static org.neo4j.helpers.Settings.osIsWindows;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for consistency checker
 */
public class ConsistencyCheckSettings
{
    @Description("Perform optional additional checking on property ownership. " +
            "This can detect a theoretical inconsistency where a property could be owned by multiple entities. " +
            "However, but the check is very expensive in time and memory, so it is skipped by default.")
    public static final Setting<Boolean> consistency_check_property_owners = setting( "consistency_check_property_owners", BOOLEAN, FALSE );

    @Description("Perform checks on the label scan store. Checking this store is more expensive than " +
            "checking the native stores, so it may be useful to turn off this check for very large databases.")
    public static final Setting<Boolean> consistency_check_label_scan_store = setting( "consistency_check_label_scan_store", BOOLEAN, TRUE );

    @Description("Perform checks on indexes. Checking indexes is more expensive than " +
            "checking the native stores, so it may be useful to turn off this check for very large databases.")
    public static final Setting<Boolean> consistency_check_indexes = setting( "consistency_check_indexes", BOOLEAN, TRUE );

    @Description("Window pool implementation to be used when running consistency check")
    public static final Setting<TaskExecutionOrder> consistency_check_execution_order =
            setting( "consistency_check_execution_order", options( TaskExecutionOrder.class ), TaskExecutionOrder.MULTI_PASS.name() );

    // On Windows there are problems with memory (un)mapping files, involving
    // relying on GC for unmapping which is error prone. So default back to
    // the a window pool that can switch off memory mapping.
    @Description("Window pool implementation to be used when running consistency check")
    public static final Setting<WindowPoolImplementation> consistency_check_window_pool_implementation =
            setting( "consistency_check_window_pool_implementation", options( WindowPoolImplementation.class ), osIsWindows() ? WindowPoolImplementation.MOST_FREQUENTLY_USED.name() : WindowPoolImplementation.SCAN_RESISTANT.name());

    @SuppressWarnings("unchecked")
    @Description("File name for inconsistencies log file. If not specified, logs to a file in the store directory.")
    public static final
    Setting<File> consistency_check_report_file = setting( "consistency_check_report_file", PATH, NO_DEFAULT, basePath( GraphDatabaseSettings.store_dir ));
}
