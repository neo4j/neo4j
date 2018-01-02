/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

@ManagementInterface( name = StoreFile.NAME )
@Description( "Information about the sizes of the different parts of the Neo4j graph store" )
public interface StoreFile
{
    final String NAME = "Store file sizes";

    @Description( "The amount of disk space used by the current Neo4j logical log, in bytes." )
    long getLogicalLogSize();

    @Description( "The total disk space used by this Neo4j instance, in bytes." )
    long getTotalStoreSize();

    @Description( "The amount of disk space used to store nodes, in bytes." )
    long getNodeStoreSize();

    @Description( "The amount of disk space used to store relationships, in bytes." )
    long getRelationshipStoreSize();

    @Description( "The amount of disk space used to store properties "
                  + "(excluding string values and array values), in bytes." )
    long getPropertyStoreSize();

    @Description( "The amount of disk space used to store string properties, in bytes." )
    long getStringStoreSize();

    @Description( "The amount of disk space used to store array properties, in bytes." )
    long getArrayStoreSize();
}
