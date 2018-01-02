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

import javax.management.MBeanOperationInfo;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = Cache.NAME )
@Description( "Information about the caching in Neo4j" )
public interface Cache
{
    String NAME = "Cache";

    @Description( "The type of cache used by Neo4j" )
    String getCacheType();

    @Description( "The size of this cache (nr of entities or total size in bytes)" )
    long getCacheSize();

    @Description( "The number of times a cache query returned a result" )
    long getHitCount();

    @Description( "The number of times a cache query did not return a result" )
    long getMissCount();

    @Description( value = "Clears the Neo4j caches", impact = MBeanOperationInfo.ACTION )
    void clear();
}
