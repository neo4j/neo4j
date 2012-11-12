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
package org.neo4j.com;

import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;

/**
 * Settings for high availability mode
 */
public class ComSettings
{
    @Description( "Max size of the data chunks that flows between master and slaves in HA. Bigger size may increase throughput," +
            "but may be more sensitive to variations in bandwidth, whereas lower size increases tolerance for bandwidth variations. " +
            "Examples: 500k or 3M. Must be within 1k-16M" )
    @Default( "2M" )
    public static final GraphDatabaseSetting<Integer> com_chunk_size =
            new GraphDatabaseSetting.IntegerRangeNumberOfBytesSetting( "ha.com_chunk_size", 1 * 1024 );
}