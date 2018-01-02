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

@ManagementInterface( name = Primitives.NAME )
@Description( "Estimates of the numbers of different kinds of Neo4j primitives" )
public interface Primitives
{
    final String NAME = "Primitive count";

    @Description( "An estimation of the number of nodes used in this Neo4j instance" )
    long getNumberOfNodeIdsInUse();

    @Description( "An estimation of the number of relationships used in this Neo4j instance" )
    long getNumberOfRelationshipIdsInUse();

    @Description( "The number of relationship types used in this Neo4j instance" )
    long getNumberOfRelationshipTypeIdsInUse();

    @Description( "An estimation of the number of properties used in this Neo4j instance" )
    long getNumberOfPropertyIdsInUse();
}
