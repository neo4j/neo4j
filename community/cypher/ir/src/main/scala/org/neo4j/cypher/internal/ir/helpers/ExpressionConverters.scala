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
package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

object ExpressionConverters {

  implicit class PredicateConverter(val predicate: Expression) extends AnyVal {

    def asPredicates: ListSet[Predicate] = {
      asPredicates(ListSet.empty)
    }

    def asPredicates(outerScope: Set[LogicalVariable]): ListSet[Predicate] = {
      val builder = ListSet.newBuilder[Predicate]
      predicate.folder.treeFold(()) {
        // n:Label
        case p @ HasLabels(v: Variable, labels) =>
          builder ++= labels.map { label =>
            Predicate(Set(v), p.copy(labels = Seq(label))(p.position))
          }
          SkipChildren(_)
        // r:T
        case p @ HasTypes(v: Variable, types) =>
          builder ++= types.map { typ =>
            Predicate(Set(v), p.copy(types = Seq(typ))(p.position))
          }
          SkipChildren(_)
        // and
        case _: Ands | _: And =>
          TraverseChildren(_)
        case p: Expression =>
          builder += Predicate(p.dependencies -- outerScope, p)
          SkipChildren(_)
      }
      builder.result()
    }
  }
}
