/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.PlanBuilder
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier

class PredicateRewriter extends PlanBuilder {


  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean = {
    // We can process this if there are any SingleNodes in the patterns, that have labels on them
    findNodeWithLabels(plan.query.patterns).nonEmpty
  }

  case class LabelExtraction(patternWithLabels: Unsolved[Pattern],
                             labels: Seq[KeyToken],
                             patternWithoutLabels: Unsolved[Pattern],
                             name: String)

  def findNodeWithLabels(tokens: Seq[QueryToken[Pattern]]): Option[LabelExtraction] =
    tokens.collectFirst {
      case node@Unsolved(pattern@SingleNode(name, labels, false)) if labels.nonEmpty                                        =>
        LabelExtraction(node, labels, Unsolved(pattern.copy(labels = Seq())), name)

      case relationship@Unsolved(RelationshipPattern(pattern, left@SingleNode(name, labels, false), _)) if labels.nonEmpty =>
        LabelExtraction(relationship, labels, Unsolved(pattern.changeEnds(left = left.copy(labels = Seq.empty))), name)

      case relationship@Unsolved(RelationshipPattern(pattern, _, right@SingleNode(name, labels, false))) if labels.nonEmpty =>
        LabelExtraction(relationship, labels, Unsolved(pattern.changeEnds(right = right.copy(labels = Seq.empty))), name)
    }

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    val nodeWithLabel: LabelExtraction = findNodeWithLabels(plan.query.patterns).get
    val predicates = mapLabelsToPredicates(nodeWithLabel.labels, nodeWithLabel.name)
    plan.copy(query = plan.query.copy(
      where = plan.query.where ++ predicates,
      patterns = plan.query.patterns.replace(nodeWithLabel.patternWithLabels, nodeWithLabel.patternWithoutLabels)))
  }

  def mapLabelsToPredicates(tokens: Seq[KeyToken], name: String): Seq[Unsolved[Predicate]] = tokens.map {
    token => Unsolved(HasLabel(Identifier(name), token).asInstanceOf[Predicate])
  }

  def priority = PlanBuilder.QueryRewriting
}
