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
package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

enum QueryLogEntryContent
{
    LOG_PARAMETERS( GraphDatabaseSettings.log_queries_parameter_logging_enabled ),
    LOG_DETAILED_TIME( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled ),
    LOG_ALLOCATED_BYTES( GraphDatabaseSettings.log_queries_allocation_logging_enabled ),
    LOG_PAGE_DETAILS( GraphDatabaseSettings.log_queries_page_detail_logging_enabled );
    private final Setting<Boolean> setting;

    QueryLogEntryContent( Setting<Boolean> setting )
    {
        this.setting = setting;
    }

    boolean enabledIn( Config config )
    {
        return config.get( setting );
    }
}
