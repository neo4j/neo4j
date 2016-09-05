/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.enterprise.configuration;

import java.io.File;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.max;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.optionsIgnoreCase;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;

/**
 * Enterprise edition specific settings
 */
public class EnterpriseEditionSettings
{
    @Description( "Specified names of id types (comma separated) that should be reused. " +
                  "Currently only 'node' and 'relationship' types are supported. " )
    public static Setting<List<IdType>> idTypesToReuse = setting(
            "dbms.ids.reuse.types.override", list( ",", optionsIgnoreCase( NODE, RELATIONSHIP ) ),
            String.join( ",", IdType.RELATIONSHIP.name(), IdType.NODE.name() ) );

    @Description( "File name for the security log." )
    public static final Setting<File> security_log_filename = derivedSetting("dbms.security.log_path",
            GraphDatabaseSettings.logs_directory,
            ( logs ) -> new File( logs, "security.log" ),
            PATH );

    @Description( "Set to log successful authentication events." )
    public static final Setting<Boolean> security_log_successful_authentication =
            setting("dbms.security.log_successful_authentication", BOOLEAN, "true" );

    @Description( "Threshold for rotation of the security log." )
    public static final Setting<Long> store_security_log_rotation_threshold =
            setting("dbms.logs.security.rotation.size", BYTES, "20m", min(0L), max( Long.MAX_VALUE ) );

    @Description( "Minimum time interval after last rotation of the security log before it may be rotated again." )
    public static final Setting<Long> store_security_log_rotation_delay =
            setting("dbms.logs.security.rotation.delay", DURATION, "300s" );

    @Description( "Maximum number of history files for the security log." )
    public static final Setting<Integer> store_security_log_max_archives =
            setting("dbms.logs.security.rotation.keep_number", INTEGER, "7", min(1) );

}
