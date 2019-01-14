/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = HighAvailability.NAME )
@Description( "Information about an instance participating in a HA cluster" )
public interface HighAvailability
{
    String NAME = "High Availability";

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
