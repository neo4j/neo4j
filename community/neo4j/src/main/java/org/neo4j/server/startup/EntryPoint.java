/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.startup;

import org.neo4j.annotations.service.Service;
import org.neo4j.service.Services;

@Service
public interface EntryPoint extends Comparable<EntryPoint>
{
    enum Priority
    {
        HIGH, //Reserved for testing
        MEDIUM, //Enterprise
        LOW; //Community
    }

    @Override
    default int compareTo( EntryPoint o )
    {
        return getPriority().compareTo( o.getPriority() );
    }

    /**
     * The priority order of this Entrypoint. Will select the entrypoint with the lowest order if multiple are available
     * @return the priority order
     */
    Priority getPriority();

    static Class<? extends EntryPoint> serviceloadEntryPoint()
    {
        return Services.loadAll( EntryPoint.class ).stream()
                .sorted()
                .findFirst()
                .orElseThrow()
                .getClass();
    }
}
