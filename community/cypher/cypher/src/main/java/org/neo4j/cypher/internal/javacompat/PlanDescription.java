/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;


import java.util.List;
import java.util.Map;

import org.neo4j.cypher.ProfilerStatisticsNotReadyException;


/**
 * Instances describe single execution steps in a Cypher query execution plan
 *
 * Execution plans form a tree of execution steps.  Each step is described by a PlanDescription object.
 */
public interface PlanDescription
{
    /**
     * @return descriptive name for this kind of execution step
     */
    String getName();

    /**
     * Retrieve argument map for the associated execution step
     *
     * Valid arguments are all Java primitive values, Strings, Arrays of those, and Maps from Strings to
     * valid arguments.  Results are guaranteed to be trees (i.e. there are no cyclic dependencies among values)
     *
     * @return a map containing arguments that describe this execution step in more detail
     */
    Map<String, Object> getArguments();

    /**
     * @return list of previous (child) execution step descriptions
     */
    List<PlanDescription> getChildren();

    /**
     * @return true, if ProfilerStatistics are available for this execution step
     */
    boolean hasProfilerStatistics();

    /**
     * @return profiler statistics for this execution step iff available
     * @throws ProfilerStatisticsNotReadyException iff profiler statistics are not available
     */
    ProfilerStatistics getProfilerStatistics() throws ProfilerStatisticsNotReadyException;
}
