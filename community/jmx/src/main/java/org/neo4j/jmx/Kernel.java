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

import java.util.Date;

import javax.management.ObjectName;

@ManagementInterface( name = Kernel.NAME )
@Description( "Information about the Neo4j kernel" )
public interface Kernel
{
    String NAME = "Kernel";

    @Description( "An ObjectName that can be used as a query for getting all management "
                  + "beans for this Neo4j instance." )
    ObjectName getMBeanQuery();

    @Description( "The location where the Neo4j store is located" )
    String getStoreDirectory();

    @Description( "The version of Neo4j" )
    String getKernelVersion();

    @Description( "The time from which this Neo4j instance was in operational mode." )
    Date getKernelStartTime();

    @Description( "The time when this Neo4j graph store was created." )
    Date getStoreCreationDate();

    @Description( "An identifier that, together with store creation time, uniquely identifies this Neo4j graph store." )
    String getStoreId();

    @Description( "The current version of the Neo4j store logical log." )
    long getStoreLogVersion();

    @Description( "Whether this is a read only instance" )
    boolean isReadOnly();
}
