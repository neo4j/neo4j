/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.config

import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherReplanAlgorithm

/**
 *
 * @param algorithm the replan algorithm.
 *                  See[[org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_replan_algorithm]]
 * @param initialThreshold The initial threshold for statistics above which a plan is considered stale.
 *                         See [[org.neo4j.configuration.GraphDatabaseSettings.query_statistics_divergence_threshold]]
 * @param targetThreshold The target threshold for statistics above which a plan is considered stale.
 *                        See [[org.neo4j.configuration.GraphDatabaseInternalSettings.query_statistics_divergence_target]]
 * @param minReplanInterval The minimum time between possible cypher query replanning events.
 *                          See [[org.neo4j.configuration.GraphDatabaseSettings.cypher_min_replan_interval]]
 * @param targetReplanInterval The minimum time between two replanning events will move towards this value over time.
 *                             See [[org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_replan_interval_target]]
 */
case class StatsDivergenceCalculatorConfig(
  algorithm: CypherReplanAlgorithm,
  initialThreshold: Double,
  targetThreshold: Double,
  minReplanInterval: Long,
  targetReplanInterval: Long
)
