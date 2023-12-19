/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = CausalClustering.NAME )
@Description( "Information about an instance participating in a causal cluster" )
public interface CausalClustering
{
    String NAME = "Causal Clustering";

    @Description( "The current role this member has in the cluster" )
    String getRole();

    @Description( "The total amount of disk space used by the raft log, in bytes" )
    long getRaftLogSize();

    @Description( "The total amount of disk space used by the replicated states, in bytes" )
    long getReplicatedStateSize();
}
