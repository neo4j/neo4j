/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.consistency;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for consistency checker
 */
@Description( "Consistency check configuration settings" )
public class ConsistencyCheckSettings implements LoadableConfig
{
    @Description( "This setting is deprecated. See commandline arguments for neoj4-admin check-consistency " +
            "instead. Perform optional additional checking on property ownership. " +
            "This can detect a theoretical inconsistency where a property could be owned by multiple entities. " +
            "However, the check is very expensive in time and memory, so it is skipped by default." )
    @Deprecated
    public static final Setting<Boolean> consistency_check_property_owners =
            setting( "tools.consistency_checker.check_property_owners", BOOLEAN, FALSE );

    @Description( "This setting is deprecated. See commandline arguments for neoj4-admin check-consistency " +
            "instead. Perform checks on the label scan store. Checking this store is more expensive than " +
            "checking the native stores, so it may be useful to turn off this check for very large databases." )
    @Deprecated
    public static final Setting<Boolean> consistency_check_label_scan_store =
            setting( "tools.consistency_checker.check_label_scan_store", BOOLEAN, TRUE );

    @Description( "This setting is deprecated. See commandline arguments for neoj4-admin check-consistency " +
            "instead. Perform checks on indexes. Checking indexes is more expensive than " +
            "checking the native stores, so it may be useful to turn off this check for very large databases." )
    @Deprecated
    public static final Setting<Boolean> consistency_check_indexes =
            setting( "tools.consistency_checker.check_indexes", BOOLEAN, TRUE );

    @Description( "This setting is deprecated. See commandline arguments for neoj4-admin check-consistency " +
            "instead. Perform checks between nodes, relationships, properties, types and tokens." )
    @Deprecated
    public static final Setting<Boolean> consistency_check_graph =
            setting( "tools.consistency_checker.check_graph", BOOLEAN, TRUE );
}
