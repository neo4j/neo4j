/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.stream.summary;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;

/**
 * An empty plan, just to make Bolt server not throwing NPE.
 */
public class EmptyExecutionPlanDescription implements ExecutionPlanDescription
{
    @Override
    public String getName()
    {
        return "";
    }

    @Override
    public List<ExecutionPlanDescription> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public Map<String,Object> getArguments()
    {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> getIdentifiers()
    {
        return Collections.emptySet();
    }

    @Override
    public boolean hasProfilerStatistics()
    {
        return false;
    }

    @Override
    public ProfilerStatistics getProfilerStatistics()
    {
        return null;
    }
}
