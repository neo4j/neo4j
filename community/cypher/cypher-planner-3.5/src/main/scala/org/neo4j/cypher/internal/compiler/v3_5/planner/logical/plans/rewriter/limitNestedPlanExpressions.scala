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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.v3_5.expressions.functions.Head
import org.neo4j.cypher.internal.v3_5.expressions.{Add, ContainerIndex, FunctionInvocation, FunctionName, ListSlice, Namespace, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_5.logical.plans.{DoNotIncludeTies, Limit, NestedPlanExpression}
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen
import org.neo4j.cypher.internal.v3_5.util.{Rewriter, bottomUp}

/**
  * Places a Limit inside of NestenPlanExpressions, if the NestenPlanExpressions is inside an expression that does not need the whole list as a result.
  * These expressions are `head`, `ContainerIndex`, and `ListSlice`.
  */
case class limitNestedPlanExpressions(logicalPlanIdGen: IdGen) extends Rewriter {
  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case fi@FunctionInvocation(Namespace(List()), FunctionName(Head.name), _, Seq(npe@NestedPlanExpression(plan, _)), _) if !plan.isInstanceOf[Limit] =>
      fi.copy(args = IndexedSeq(npe.copy(
        Limit(plan, SignedDecimalIntegerLiteral("1")(npe.position), DoNotIncludeTies)(logicalPlanIdGen)
      )(npe.position)))(fi.position)

    case ci@ContainerIndex(npe@NestedPlanExpression(plan, _), index) if !plan.isInstanceOf[Limit] =>
      ci.copy(expr = npe.copy(
        Limit(plan, Add(SignedDecimalIntegerLiteral("1")(npe.position), index)(npe.position), DoNotIncludeTies)(logicalPlanIdGen)
      )(npe.position))(ci.position)

    case ls@ListSlice(npe@NestedPlanExpression(plan, _), _, Some(to)) if !plan.isInstanceOf[Limit] =>
      ls.copy(list = npe.copy(
        Limit(plan, Add(SignedDecimalIntegerLiteral("1")(npe.position), to)(npe.position), DoNotIncludeTies)(logicalPlanIdGen)
      )(npe.position))(ls.position)
  })
}
