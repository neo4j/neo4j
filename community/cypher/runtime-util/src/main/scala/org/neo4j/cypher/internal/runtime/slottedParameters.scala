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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

/**
  * Rewrites a logical plan so that parameter access is done by offset into an array instead of accessing
  * a hash map.
  */
case object slottedParameters {

  def apply(input: LogicalPlan): (LogicalPlan, Map[String, Int]) = {
    val mapping: Map[String, Int] = input.treeFold(Set.empty[String]) {
      case Parameter(name, _) => acc => (acc + name, Some(identity))
    }.toArray.sorted.zipWithIndex.toMap

    val rewriter = bottomUp(Rewriter.lift {
      case Parameter(name, typ) => ParameterFromSlot(mapping(name), name, typ)
    })

    (input.endoRewrite(rewriter), mapping)
  }
}
