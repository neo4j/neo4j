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
