/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

@ManagementInterface( name = PageCache.NAME )
@Description( "Information about the Neo4j page cache" )
public interface PageCache
{
    final String NAME = "Page cache";

    @Description( "Number of page faults" )
    int getFaults();

    @Description( "Number of page evictions" )
    int getEvictions();

    @Description( "Number of page pins" )
    int getPins();

    @Description( "Number of page unpins" )
    int getUnpins();

    @Description( "Number of taken exclusive page locks" )
    int getTakenExclusiveLocks();

    @Description( "Number of taken shared page locks" )
    int getTakenSharedLocks();

    @Description( "Number of released exclusive page locks" )
    int getReleasedExclusiveLocks();

    @Description( "Number of released shared page locks" )
    int getReleasedSharedLocks();

    @Description( "Number of page flushes" )
    int getFlushes();
}
