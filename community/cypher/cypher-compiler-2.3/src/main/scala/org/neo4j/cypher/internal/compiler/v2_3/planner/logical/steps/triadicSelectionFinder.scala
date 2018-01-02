/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CandidateGenerator, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression

object triadicSelectionFinder extends CandidateGenerator[LogicalPlan] {

  override def apply(in: LogicalPlan, qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] =
    unsolvedPredicates(in, qg).collect {
      // WHERE NOT (a)-[:X]->(c)
      case predicate@Not(patternExpr: PatternExpression) => findMatchingRelationshipPattern(positivePredicate = false, predicate, patternExpr, in, qg)
      // WHERE (a)-[:X]->(c)
      case patternExpr: PatternExpression => findMatchingRelationshipPattern(positivePredicate = true, patternExpr, patternExpr, in, qg)
    }.flatten

  def unsolvedPredicates(in: LogicalPlan, qg: QueryGraph) = {
    val patternPredicates: Seq[Expression] = qg.selections.patternPredicatesGiven(in.availableSymbols)
    val solvedPredicates: Seq[Expression] = in.solved.lastQueryGraph.selections.flatPredicates
    patternPredicates.filter { patternPredicate =>
      !(solvedPredicates contains patternPredicate)
    }
  }

  private def findMatchingRelationshipPattern(positivePredicate: Boolean, triadicPredicate: Expression,
                                              patternExpression: PatternExpression, in: LogicalPlan, qg: QueryGraph)
                                              (implicit context: LogicalPlanningContext): Seq[LogicalPlan] = in match {

    // MATCH (a)-[:X]->(b)-[:X]->(c) WHERE (predicate involving (a)-[:X]->(c))
    case Selection(predicates,exp:Expand) => findMatchingOuterExpand(positivePredicate, triadicPredicate, patternExpression, predicates, exp, qg)

    // MATCH (a)-[:X]->(b)-[:Y]->(c) WHERE (predicate involving (a)-[:X]->(c))
    case exp:Expand => findMatchingOuterExpand(positivePredicate, triadicPredicate, patternExpression, Seq.empty, exp, qg)

    case _ => Seq.empty
  }

  private def findMatchingOuterExpand(positivePredicate: Boolean, triadicPredicate: Expression,
                                              patternExpression: PatternExpression, incomingPredicates: Seq[Expression], expand: Expand, qg: QueryGraph)
                                             (implicit context: LogicalPlanningContext): Seq[LogicalPlan] = expand match {
    case exp2@Expand(exp1: Expand, _, _, _, _, _, ExpandAll) =>
      findMatchingInnerExpand(positivePredicate, triadicPredicate, patternExpression, incomingPredicates, Seq.empty, exp1, exp2, qg)

    case exp2@Expand(Selection(innerPredicates, exp1: Expand), _, _, _, _, _, ExpandAll) =>
      findMatchingInnerExpand(positivePredicate, triadicPredicate, patternExpression, incomingPredicates, innerPredicates, exp1, exp2, qg)

    case _ => Seq.empty
  }

  private def findMatchingInnerExpand(positivePredicate: Boolean, triadicPredicate: Expression,
                                      patternExpression: PatternExpression, incomingPredicates: Seq[Expression],
                                      leftPredicates: Seq[Expression], exp1: Expand, exp2: Expand, qg: QueryGraph)
                                             (implicit context: LogicalPlanningContext): Seq[LogicalPlan] =
    if (exp1.mode == ExpandAll && exp1.to == exp2.from &&
      matchingLabels(positivePredicate, exp1.to, exp2.to, qg) &&
      leftPredicatesAcceptable(exp1.to, leftPredicates) &&
      matchingRelationshipPattern(patternExpression, exp1.from.name, exp2.to.name, exp1.types, exp1.dir)) {

      val left = if (leftPredicates.nonEmpty)
        context.logicalPlanProducer.planSelection(leftPredicates, exp1)
      else
        exp1

      val argument = context.logicalPlanProducer.planArgumentRowFrom(left)
      val newExpand2 = Expand(argument, exp2.from, exp2.dir, exp2.types, exp2.to, exp2.relName, ExpandAll)(exp2.solved)
      val right = if (incomingPredicates.nonEmpty)
        context.logicalPlanProducer.planSelection(incomingPredicates, newExpand2)
      else
        newExpand2

      Seq(context.logicalPlanProducer.planTriadicSelection(positivePredicate, left, exp1.from, exp2.from, exp2.to, right, triadicPredicate))
    }
    else
      Seq.empty

  private def leftPredicatesAcceptable(leftId: IdName, leftPredicates: Seq[Expression]) = leftPredicates.forall {
    case HasLabels(Identifier(id),List(_)) if(id == leftId.name) => true
    case a => false
  }

  private def matchingLabels(positivePredicate: Boolean, node1: IdName, node2: IdName, qg: QueryGraph): Boolean = {
    val labels1 = qg.selections.labelsOnNode(node1)
    val labels2 = qg.selections.labelsOnNode(node2)
    if (positivePredicate)
      labels1 == labels2
    else
      labels1.isEmpty || labels2.nonEmpty && (labels2 subsetOf labels1)
  }

  private def matchingRelationshipPattern(pattern: PatternExpression, from: String, to: String,
                                          types: Seq[RelTypeName], dir: SemanticDirection): Boolean = pattern match {
    // (a)-[:X]->(c)
    case p@PatternExpression(
      RelationshipsPattern(
        RelationshipChain(
          NodePattern(Some(Identifier(predicateFrom)), List(), None, false),
          RelationshipPattern(None, false, predicateTypes, None, None, predicateDir),
          NodePattern(Some(Identifier(predicateTo)), List(), None, false))))
      if predicateFrom == from && predicateTo == to && predicateTypes == types && predicateDir == dir => true
    case _ => false
  }
}