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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.ast
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription._
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._

object LogicalPlan2PlanDescription extends ((LogicalPlan, Map[LogicalPlan, Id]) => InternalPlanDescription) {
  import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._

  override def apply(plan: LogicalPlan, idMap: Map[LogicalPlan, Id]): InternalPlanDescription = {
    val symbols = plan.availableSymbols.map(_.name)
    val planDescription = plan match {

      case AllNodesScan(IdName(id), arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "AllNodesScan", NoChildren, Seq.empty, symbols)

      case NodeByLabelScan(IdName(id), label, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeByLabelScan", NoChildren, Seq(LabelName(label.name)), symbols)

      case NodeByIdSeek(IdName(id), nodeIds, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeByIdSeek", NoChildren, Seq(), symbols)

      case NodeIndexSeek(IdName(id), label, propKey, value, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeIndexSeek", NoChildren, Seq(Index(label.name, propKey.name)), symbols)

      case NodeIndexUniqueSeek(IdName(id), label, propKey, value, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeIndexUniqueSeek", NoChildren, Seq(Index(label.name, propKey.name)), symbols)

      case ProduceResult(nodes, rels, _, inner) =>
        PlanDescriptionImpl(id = idMap(plan), "Results", SingleChild(apply(inner, idMap)), Seq(), symbols)

      case Expand(inner, IdName(fromName), dir, typeNames, IdName(toName), IdName(relName), mode) =>
        val expression = ExpandExpression( fromName, relName, typeNames.map( _.name ), toName, dir )
        val modeText = mode match {
          case ExpandAll => "Expand(All)"
          case ExpandInto => "Expand(Into)"
        }
        PlanDescriptionImpl(id = idMap(plan), modeText, SingleChild(apply(inner, idMap)), Seq(expression), symbols)

      case NodeHashJoin(nodes, lhs, rhs) =>
        val children = TwoChildren(apply(lhs, idMap), apply(rhs, idMap))
        PlanDescriptionImpl(id = idMap(plan), "NodeHashJoin", children, Seq(KeyNames(nodes.toSeq.map(_.name))), symbols)

      case Projection(lhs, expr) =>
        PlanDescriptionImpl(id = idMap(plan), "Projection", SingleChild(apply(lhs, idMap)), expr.values.toSeq.map(e => LegacyExpression(e.asCommandExpression)), symbols )

      case x => throw new CantCompileQueryException(x.getClass.getSimpleName)
    }
    planDescription.addArgument(EstimatedRows(plan.solved.estimatedCardinality.amount))
  }
}
