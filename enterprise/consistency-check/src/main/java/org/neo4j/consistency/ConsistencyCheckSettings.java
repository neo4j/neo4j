/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.ANY;

import org.neo4j.consistency.checking.full.TaskExecutionOrder;
import org.neo4j.consistency.store.windowpool.WindowPoolImplementation;
import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;

/**
 * Settings for online backup
 */
public class ConsistencyCheckSettings
{
    @Description("Perform optional additional checking on property ownership. " +
            "This can detect a theoretical inconsistency where a property could be owned by multiple entities. " +
            "However, but the check is very expensive in time and memory, so it is skipped by default.")
    /** Defaults to false due to the way Boolean.parseBoolean(null) works. */
    public static final GraphDatabaseSetting<Boolean> consistency_check_property_owners =
            new GraphDatabaseSetting.BooleanSetting( "consistency_check_property_owners" );

    @Description("Window pool implementation to be used when running consistency check")
    @Default( "MULTI_PASS" )
    public static final
    GraphDatabaseSetting.EnumerableSetting<TaskExecutionOrder> consistency_check_execution_order =
            new GraphDatabaseSetting.EnumerableSetting<TaskExecutionOrder>(
                    "consistency_check_execution_order", TaskExecutionOrder.class );

    @Description("Window pool implementation to be used when running consistency check")
    @Default( "SCAN_RESISTANT" )
    public static final
    GraphDatabaseSetting.EnumerableSetting<WindowPoolImplementation> consistency_check_window_pool_implementation =
            new GraphDatabaseSetting.EnumerableSetting<WindowPoolImplementation>(
                    "consistency_check_window_pool_implementation", WindowPoolImplementation.class );

    @Description("File name for inconsistencies log file. If not specified, logs to a file in the store directory.")
    public static final
    GraphDatabaseSetting<String> consistency_check_report_file =
            new GraphDatabaseSetting.StringSetting("consistency_check_report_file", ANY, "Must me a valid file name");
}
