/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.FALSE;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.options;
import static org.neo4j.helpers.Settings.setting;

import org.neo4j.consistency.checking.full.TaskExecutionOrder;
import org.neo4j.consistency.store.windowpool.WindowPoolImplementation;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;

/**
 * Settings for consistency checker
 */
public class ConsistencyCheckSettings
{
    @Description("Perform optional additional checking on property ownership. " +
            "This can detect a theoretical inconsistency where a property could be owned by multiple entities. " +
            "However, but the check is very expensive in time and memory, so it is skipped by default.")
    public static final Setting<Boolean> consistency_check_property_owners = setting(
            "consistency_check_property_owners", BOOLEAN, FALSE );

    @Description("Window pool implementation to be used when running consistency check")
    public static final Setting<TaskExecutionOrder> consistency_check_execution_order =
            setting( "consistency_check_execution_order", options( TaskExecutionOrder.class ),
                    TaskExecutionOrder.MULTI_PASS.name() );

    @Description("Window pool implementation to be used when running consistency check")
    public static final Setting<WindowPoolImplementation> consistency_check_window_pool_implementation =
            setting( "consistency_check_window_pool_implementation", options( WindowPoolImplementation.class ),
                    WindowPoolImplementation.SCAN_RESISTANT.name() );
}
