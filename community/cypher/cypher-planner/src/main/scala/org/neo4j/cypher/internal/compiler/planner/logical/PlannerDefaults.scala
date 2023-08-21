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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Selectivity

object PlannerDefaults {

  /** Default selectivity for `n.prop IS NOT NULL` && `n.prop OP <range>` */
  val DEFAULT_RANGE_SELECTIVITY = Selectivity(0.3)
  val DEFAULT_PREDICATE_SELECTIVITY = Selectivity(0.75)

  /** Default selectivity for `n.prop IS NOT NULL` */
  val DEFAULT_PROPERTY_SELECTIVITY = Selectivity(0.5)

  /** Default selectivity for `n.prop IS NOT NULL` && `n.prop == ...` */
  val DEFAULT_EQUALITY_SELECTIVITY = Selectivity(0.1)
  val DEFAULT_TYPE_SELECTIVITY = Selectivity(0.9)
  val DEFAULT_NUMBER_OF_ID_LOOKUPS = Cardinality(25)
  val DEFAULT_LIST_CARDINALITY = Cardinality(25)
  val DEFAULT_LIMIT_ROW_COUNT = 75
  val DEFAULT_LIMIT_CARDINALITY = Cardinality(DEFAULT_LIMIT_ROW_COUNT)
  val DEFAULT_REL_UNIQUENESS_SELECTIVITY = Selectivity(0.99)

  /** Default selectivity for `n.prop OP <range> | n.prop IS NOT NULL` */
  val DEFAULT_RANGE_SEEK_FACTOR = 0.03
  val DEFAULT_STRING_LENGTH = 6
  val DEFAULT_DISTINCT_SELECTIVITY = Selectivity(0.95)
  val DEFAULT_MULTIPLIER = Multiplier(10)
  val DEFAULT_SKIP_ROW_COUNT = 1
}
