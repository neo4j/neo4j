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
package org.neo4j.jmx;

@ManagementInterface( name = StoreSize.NAME )
@Description( "Information about the disk space used by different parts of the Neo4j graph store" )
public interface StoreSize
{
    String NAME = "Store sizes";

    @Description( "Disk space used by the transaction logs, in bytes." )
    long getTransactionLogsSize();

    @Description( "Disk space used to store nodes, in bytes." )
    long getNodeStoreSize();

    @Description( "Disk space used to store relationships, in bytes." )
    long getRelationshipStoreSize();

    @Description( "Disk space used to store properties (excluding string values and array values), in bytes." )
    long getPropertyStoreSize();

    @Description( "Disk space used to store string properties, in bytes." )
    long getStringStoreSize();

    @Description( "Disk space used to store array properties, in bytes." )
    long getArrayStoreSize();

    @Description( "Disk space used to store labels, in bytes" )
    long getLabelStoreSize();

    @Description( "Disk space used to store counters, in bytes" )
    long getCountStoreSize();

    @Description( "Disk space used to store schemas (index and constrain declarations), in bytes" )
    long getSchemaStoreSize();

    @Description( "Disk space used to store all indices, in bytes" )
    long getIndexStoreSize();

    @Description( "Disk space used by whole store, in bytes." )
    long getTotalStoreSize();
}
