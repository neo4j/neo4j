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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

/**
 * If we have a query like `MATCH (n) CALL { WITH n DELETE DETACH n } IN TRANSACTIONS`, we know that deleting n will not interfere with the matching of nodes.
 *
 * We therefore remove all `Eager` operators if we recognize such a pattern.
 */
case object truncateDatabaseDeeagerizer extends Rewriter {

  override def apply(value: AnyRef): AnyRef = {
    val eagerlessPlan = removeEager(value)
    if (isTruncatePattern(eagerlessPlan)) eagerlessPlan
    else value
  }

  private def removeEager(value: AnyRef): AnyRef = {
    value.endoRewrite(topDown(Rewriter.lift {
      case Eager(source, _) => source
    }))
  }

  private def isTruncatePattern(plan: Any): Boolean = plan match {
    case ProduceResult(
        EmptyResult(
          UnitTruncate(_)
        ),
        _,
        _
      ) => true
    case ProduceResult(
        ReturningTruncate(_),
        _,
        _
      ) => true
    case ProduceResult(
        Projection(
          ReturningTruncate(_),
          _
        ),
        _,
        _
      ) => true
    case _ => false
  }

  case object UnitTruncate {

    def unapply(v: Any): Option[LogicalVariable] = v match {
      case TransactionForeach(
          NodeLeafPlan(n),
          DeletePlan(m),
          _,
          _,
          _,
          _
        ) if n == m => Some(n)
      case SubqueryForeach(
          NodeLeafPlan(n),
          DeletePlan(m)
        ) if n == m => Some(n)
      case _ => None
    }
  }

  case object ReturningTruncate {

    def unapply(v: Any): Option[LogicalVariable] = v match {
      case TransactionApply(
          NodeLeafPlan(n),
          Projection(
            DeletePlan(m),
            _
          ),
          _,
          _,
          _,
          _
        ) if n == m => Some(n)
      case Apply(
          NodeLeafPlan(n),
          Projection(
            DeletePlan(m),
            _
          )
        ) if n == m => Some(n)
      case TransactionApply(
          NodeLeafPlan(n),
          DeletePlan(m),
          _,
          _,
          _,
          _
        ) if n == m => Some(n)
      case Apply(
          NodeLeafPlan(n),
          DeletePlan(m)
        ) if n == m => Some(n)
      case _ => None
    }
  }

  case object NodeLeafPlan {

    def unapply(v: Any): Option[LogicalVariable] = v match {
      case plan: NodeLogicalLeafPlan     => Some(plan.idName)
      case Selection(_, NodeLeafPlan(n)) => Some(n)
      case _                             => None
    }
  }

  case object DeletePlan {

    def unapply(v: Any): Option[LogicalVariable] = v match {
      case DetachDeleteNode(
          _: Argument,
          n: LogicalVariable
        ) => Some(n)
      case DeleteNode(
          _: Argument,
          n: LogicalVariable
        ) => Some(n)
      case _ => None
    }
  }

}
