/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.steps.getDegreeRewriter.isEligible
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.LabelExpression.containsGpmSpecificRelType
import org.neo4j.cypher.internal.expressions.LabelExpression.getRelTypes
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

case object getDegreeRewriter extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val instance = bottomUp(
    rewriter,
    // Do not traverse into NestedPlanExpressions as they have been optimized already by an earlier call to plan
    _.isInstanceOf[NestedPlanExpression]
  )

  private def rewriter = Rewriter.lift {

    // GetDegree > limit
    case e @ GreaterThan(GetDegree(node, typ, dir), limit) =>
      HasDegreeGreaterThan(node, typ, dir, limit)(e.position)
    // limit > GetDegree
    case e @ GreaterThan(limit, GetDegree(node, typ, dir)) =>
      HasDegreeLessThan(node, typ, dir, limit)(e.position)
    // GetDegree >= limit
    case e @ GreaterThanOrEqual(GetDegree(node, typ, dir), limit) =>
      HasDegreeGreaterThanOrEqual(node, typ, dir, limit)(e.position)
    // limit >= GetDegree
    case e @ GreaterThanOrEqual(limit, GetDegree(node, typ, dir)) =>
      HasDegreeLessThanOrEqual(node, typ, dir, limit)(e.position)
    // GetDegree < limit
    case e @ LessThan(GetDegree(node, typ, dir), limit) =>
      HasDegreeLessThan(node, typ, dir, limit)(e.position)
    // limit < GreaterThan
    case e @ LessThan(limit, GetDegree(node, typ, dir)) =>
      HasDegreeGreaterThan(node, typ, dir, limit)(e.position)
    // GetDegree <= limit
    case e @ LessThanOrEqual(GetDegree(node, typ, dir), limit) =>
      HasDegreeLessThanOrEqual(node, typ, dir, limit)(e.position)
    // limit <= GreaterThan
    case e @ LessThanOrEqual(limit, GetDegree(node, typ, dir)) =>
      HasDegreeGreaterThanOrEqual(node, typ, dir, limit)(e.position)
    // GetDegree = limit
    case e @ Equals(GetDegree(node, typ, dir), value) =>
      HasDegree(node, typ, dir, value)(e.position)
    // limit = GreaterThan
    case e @ Equals(value, GetDegree(node, typ, dir)) =>
      HasDegree(node, typ, dir, value)(e.position)

    // SIZE( (a)-[]->() )
    case SizeFunctionOfPatternComprehension(node, types, dir) =>
      calculateUsingGetDegree(node, types, dir)

    // EXISTS( (a)-[]->() ) rewritten to GetDegree( (a)-[]->() ) > 0
    case ExistsFunctionOfPatternExpressions(node, types, dir) =>
      existsToUsingHasDegreeGreaterThan(node, types, dir)
  }

  private def calculateUsingGetDegree(
    node: LogicalVariable,
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ): Expression = {
    types
      .map(typ => GetDegree(node.copyId, Some(typ), dir)(typ.position))
      .reduceOption[Expression](Add(_, _)(InputPosition.NONE))
      .getOrElse(GetDegree(node.copyId, None, dir)(InputPosition.NONE))
  }

  private def existsToUsingHasDegreeGreaterThan(
    node: LogicalVariable,
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ): Expression = {
    val all = types.map(typ =>
      HasDegreeGreaterThan(node.copyId, Some(typ), dir, SignedDecimalIntegerLiteral("0")(InputPosition.NONE))(
        typ.position
      )
    )
    if (all.isEmpty) {
      HasDegreeGreaterThan(node.copyId, None, dir, SignedDecimalIntegerLiteral("0")(InputPosition.NONE))(
        InputPosition.NONE
      )
    } else if (all.size == 1) {
      all.head
    } else {
      Ors(all)(InputPosition.NONE)
    }
  }

  def isEligible(
    pe: Expression,
    node: LogicalVariable,
    rel: LogicalVariable,
    otherNode: LogicalVariable,
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ): Option[(LogicalVariable, Seq[RelTypeName], SemanticDirection)] = {
    val peDeps = pe.dependencies
    if (peDeps.contains(node) && !peDeps.contains(rel) && !peDeps.contains(otherNode)) {
      Some((node, types, dir))
    } else if (!peDeps.contains(node) && !peDeps.contains(rel) && peDeps.contains(otherNode)) {
      Some((otherNode, types, dir.reversed))
    } else {
      None
    }
  }
}

object RelationshipsPatternSolvableByGetDegree {

  def unapply(arg: Any)
    : Option[(LogicalVariable, LogicalVariable, LogicalVariable, Seq[RelTypeName], SemanticDirection)] = arg match {
    // (a)-[r:X|Y]->(b)
    case RelationshipsPattern(RelationshipChain(
        NodePattern(Some(node), None, None, None),
        RelationshipPattern(Some(rel), labelExpression, None, None, None, dir),
        NodePattern(Some(otherNode), None, None, None)
      )) if !containsGpmSpecificRelType(labelExpression) =>
      Some((node, rel, otherNode, getRelTypes(labelExpression), dir))

    case _ => None
  }
}

object ExistsFunctionOfPatternExpressions {

  def unapply(arg: Any): Option[(LogicalVariable, Seq[RelTypeName], SemanticDirection)] = arg match {
    case Exists(
        pe @ PatternExpression(RelationshipsPatternSolvableByGetDegree(
          node,
          rel,
          otherNode,
          types,
          dir
        ))
      ) =>
      isEligible(pe, node, rel, otherNode, types, dir)

    case _ => None
  }
}

object SizeFunctionOfPatternComprehension {

  def unapply(arg: Any): Option[(LogicalVariable, Seq[RelTypeName], SemanticDirection)] = arg match {
    case Size(
        pe @ PatternComprehension(
          _,
          RelationshipsPatternSolvableByGetDegree(
            node,
            rel,
            otherNode,
            types,
            dir
          ),
          None,
          _
        )
      ) =>
      isEligible(pe, node, rel, otherNode, types, dir)

    case _ => None
  }
}
