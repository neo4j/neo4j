/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.ir.v3_4.PatternLength
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen
import org.neo4j.cypher.internal.v3_4.expressions.RelTypeName

/**
  * For every source row, consider the path described by the relationships in 'rel'
  *
  *   If rel == NO_VALUE or rel does not match the specified 'types', do nothing
  *   If directed, produce one row containing source and the start and end nodes of rel
  *   If not directed, produce two rows:
  *     one like the directed case
  *     one like the directed case, but with start and end node swapped, and rel = reverse(rel)
  */
case class ProjectEndpoints(source: LogicalPlan,
                            rel: String,
                            start: String,
                            startInScope: Boolean,
                            end: String,
                            endInScope: Boolean,
                            types: Option[Seq[RelTypeName]],
                            directed: Boolean,
                            length: PatternLength)(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {

  val lhs = Some(source)
  def rhs = None

  val availableSymbols: Set[String] = source.availableSymbols + rel + start + end
}
