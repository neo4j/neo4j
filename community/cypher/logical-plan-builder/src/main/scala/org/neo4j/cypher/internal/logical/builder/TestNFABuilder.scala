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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder.NodePredicate
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFABuilder
import org.neo4j.cypher.internal.logical.plans.NFABuilder.State
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.inSequence

import scala.collection.immutable.ListSet

object TestNFABuilder {

  object NodePredicate {

    def unapply(node: NodePattern): Option[(String, Option[Expand.VariablePredicate])] = node match {
      case NodePattern(Some(nodeVariable @ LogicalVariable(name)), labelExpression, None, nodePredicate) =>
        val labelExpressionRewriter = LabelExpressionNormalizer(nodeVariable, Some(NODE_TYPE))
        val normalizedLabelExpression = labelExpression.map(labelExpressionRewriter(_).asInstanceOf[Expression])

        val nodeJointPredicate = (nodePredicate, normalizedLabelExpression) match {
          case (None, None)         => None
          case (Some(p1), None)     => Some(p1)
          case (None, Some(p2))     => Some(p2)
          case (Some(p1), Some(p2)) => Some(Ands(ListSet(p1, p2))(InputPosition.NONE))
        }

        val rewrittenNodeJointPredicate =
          nodeJointPredicate.endoRewrite(inSequence(flattenBooleanOperators, combineHasLabels))

        Some((name, rewrittenNodeJointPredicate.map(Expand.VariablePredicate(nodeVariable, _))))
      case _ => None
    }
  }
}

/**
 * Builder used to build NFAs in test code together with an [[AbstractLogicalPlanBuilder]].
 */
class TestNFABuilder(startStateId: Int, startStateName: String, groupVar: Boolean = false)
    extends NFABuilder(startStateId, startStateName, groupVar) {

  def addTransition(fromId: Int, toId: Int, pattern: String, groupVars: Set[String] = Set.empty): TestNFABuilder = {

    def assertFromNameMatchesFromId(actualState: State, specifiedName: String): Unit = {
      if (actualState.name.name != specifiedName) {
        throw new IllegalArgumentException(
          s"For id $fromId in pattern '$pattern': expected '${actualState.name.name}' but was '$specifiedName'"
        )
      }
    }

    val parsedPattern = Parser.parsePathPattern(pattern).element
    parsedPattern match {
      case RelationshipChain(
          NodePattern(Some(LogicalVariable(fromName)), None, None, None),
          RelationshipPattern(
            Some(rel @ LogicalVariable(relName)),
            relTypeExpression,
            None,
            None,
            relPredicate,
            direction
          ),
          NodePredicate(toName, toNodePredicate)
        ) =>
        val types = LabelExpression.getRelTypes(relTypeExpression)
        val relVariablePredicate = relPredicate.map(Expand.VariablePredicate(rel, _))

        val nfaPredicate = RelationshipExpansionPredicate(
          NFABuilder.asVarName(relName, groupVars.contains(relName)),
          relVariablePredicate,
          types,
          direction,
          toNodePredicate
        )

        val fromState = getOrCreateState(fromId, fromName, groupVars.contains(fromName))
        assertFromNameMatchesFromId(fromState, fromName)
        val toState = getOrCreateState(toId, toName, groupVars.contains(toName))

        addTransition(fromState, toState, nfaPredicate)

      case PathConcatenation(Seq(
          NodePattern(Some(LogicalVariable(fromName)), None, None, None),
          NodePredicate(toName, toNodePredicate)
        )) =>
        val fromState = getOrCreateState(fromId, fromName, groupVars.contains(fromName))
        assertFromNameMatchesFromId(fromState, fromName)
        val toState = getOrCreateState(toId, toName, groupVars.contains(toName))
        addTransition(fromState, toState, NodeJuxtapositionPredicate(toNodePredicate))

      case _ => throw new IllegalArgumentException(s"Expected path pattern or two juxtaposed nodes but was: $pattern")
    }
    this
  }

  def addFinalState(id: Int): TestNFABuilder = {
    val state = getState(id)
    addFinalState(state)
    this
  }
}
