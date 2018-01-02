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
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = HighAvailability.NAME )
@Description( "Information about an instance participating in a HA cluster" )
public interface HighAvailability
{
    final String NAME = "High Availability";

    @Description( "The identifier used to identify this server in the HA cluster" )
    String getInstanceId();

    @Description( "Whether this instance is available or not" )
    boolean isAvailable();
    
    @Description( "Whether this instance is alive or not" )
    boolean isAlive();
    
    @Description( "The role this instance has in the cluster" )
    String getRole();

    @Description( "The time when the data on this instance was last updated from the master" )
    String getLastUpdateTime();

    @Description( "The latest transaction id present in this instance's store" )
    long getLastCommittedTxId();

    @Description( "Information about all instances in this cluster" )
    ClusterMemberInfo[] getInstancesInCluster();

    @Description( "(If this is a slave) Update the database on this "
                  + "instance with the latest transactions from the master" )
    String update();
}
