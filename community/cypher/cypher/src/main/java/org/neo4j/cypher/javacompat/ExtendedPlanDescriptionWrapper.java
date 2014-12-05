/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.ProfilerStatisticsNotReadyException;

public class ExtendedPlanDescriptionWrapper implements ExtendedPlanDescription
{
    private final PlanDescription inner;

    public ExtendedPlanDescriptionWrapper( PlanDescription inner )
    {
        this.inner = inner;
    }

    @Override
    public String getName()
    {
        return inner.getName();
    }

    @Override
    public Map<String,Object> getArguments()
    {
        return inner.getArguments();
    }

    @Override
    public Set<String> getIdentifiers()
    {
        return Collections.emptySet();
    }

    @Override
    public List<PlanDescription> getChildren()
    {
        return inner.getChildren();
    }

    @Override
    public List<ExtendedPlanDescription> getExtendedChildren()
    {
        List<PlanDescription> children = getChildren();
        List<ExtendedPlanDescription> result = new ArrayList<>( children.size() );
        for ( PlanDescription child : children )
        {
            result.add( new ExtendedPlanDescriptionWrapper( child ) );
        }
        return result;
    }

    @Override
    public boolean hasProfilerStatistics()
    {
        return inner.hasProfilerStatistics();
    }

    @Override
    public ProfilerStatistics getProfilerStatistics() throws ProfilerStatisticsNotReadyException
    {
        return inner.getProfilerStatistics();
    }

    @Override
    public String toString()
    {
        return inner.toString();
    }
}
