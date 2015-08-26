/*
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CandidateGenerator, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.graphdb.Direction

object triadicSelectionFinder extends CandidateGenerator[LogicalPlan] {
  override def apply(in: LogicalPlan, qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = in match {
    // MATCH (a)-[:X]->(b)-[:X]->(c) WHERE (predicate involving (a)-[:X]->(c))
    case sel@Selection(predicates,
           exp2@Expand(
             exp1@Expand(lhs, from1, dir1, types1, to1, rel1, ExpandAll),
                              from2, dir2, types2, to2, rel2, ExpandAll))
      if to1 == from2 =>

      val newPlan = matchingPredicateExists(qg, in.availableSymbols, from1.name, to2.name, types1, dir1) map {
        case (positivePredicate, predicate) =>
          val argument = context.logicalPlanProducer.planArgumentRowFrom(exp1)
          val newExpand2 = Expand(argument, from2, dir2, types2, to2, rel2, ExpandAll)(exp2.solved)
          val newSelection = context.logicalPlanProducer.planSelection(sel.predicates, newExpand2)
          context.logicalPlanProducer.planTriadicSelection(positivePredicate, exp1, from1, to1, to2, newSelection, predicate)
      }

      newPlan.toSeq

    case _ => Seq.empty
  }

  private def matchingPredicateExists(qg: QueryGraph, availableSymbols: Set[IdName], from: String, to: String, types: Seq[RelTypeName], dir: Direction): Option[(Boolean,Expression)] =
    qg.selections.patternPredicatesGiven(availableSymbols).collectFirst {
      // WHERE NOT (a)-[:X]->(c)
      case predicate@Not(patternExpr: PatternExpression) if matchingRelationshipPattern(patternExpr, from, to, types, dir) => (false, predicate)
      // WHERE (a)-[:X]->(c)
      case patternExpr: PatternExpression if matchingRelationshipPattern(patternExpr, from, to, types, dir) => (true, patternExpr)
    }

  private def matchingRelationshipPattern(pattern: PatternExpression, from: String, to: String, types: Seq[RelTypeName], dir: Direction): Boolean = pattern match {
    // (a)-[:X]->(c)
    case p@PatternExpression(
        RelationshipsPattern(
        RelationshipChain(
        NodePattern(Some(Identifier(predicateFrom)), List(), None, false),
        RelationshipPattern(None, false, predicateTypes, None, None, predicateDir),
        NodePattern(Some(Identifier(predicateTo)), List(), None, false))))
          if predicateFrom == from && predicateTo == to && predicateTypes == types && toGraphDb(predicateDir) == dir => true
    case _ => false
  }
}
