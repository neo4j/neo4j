/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

/**
 * Profiler statistics for a single execution step of a Cypher query execution plan
 */
public interface ProfilerStatistics
{
    /**
     * @return PlanDescription for which these ProfilerStatistics have been collected
     */
    PlanDescription getPlanDescription();

    /**
     * @return number of rows processed by the associated execution step
     */
    long getRows();

    /**
     * @return number of database hits (potential disk accesses) caused by executing the associated execution step
     */
    long getDbHits();
}
