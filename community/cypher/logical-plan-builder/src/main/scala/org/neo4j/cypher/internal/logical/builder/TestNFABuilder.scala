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
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFABuilder
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.inSequence

import scala.util.control.NonFatal

object TestNFABuilder {

  object NodePredicate {

    def unapply(node: NodePattern): Option[(LogicalVariable, Option[Expand.VariablePredicate])] = node match {
      case NodePattern(Some(nodeVariable: LogicalVariable), labelExpression, None, nodePredicate) =>
        val labelExpressionRewriter = LabelExpressionNormalizer(nodeVariable, Some(NODE_TYPE))
        val normalizedLabelExpression = labelExpression.map(labelExpressionRewriter(_).asInstanceOf[Expression])

        val nodeJointPredicate = (nodePredicate, normalizedLabelExpression) match {
          case (None, None)         => None
          case (Some(p1), None)     => Some(p1)
          case (None, Some(p2))     => Some(p2)
          case (Some(p1), Some(p2)) => Some(Ands(ListSet(p1, p2))(InputPosition.NONE))
        }

        val rewrittenNodeJointPredicate =
          nodeJointPredicate.endoRewrite(inSequence(
            flattenBooleanOperators.instance(CancellationChecker.NeverCancelled),
            combineHasLabels
          ))

        Some((nodeVariable, rewrittenNodeJointPredicate.map(Expand.VariablePredicate(nodeVariable, _))))
      case _ => None
    }
  }
}

/**
 * Builder used to build NFAs in test code together with an [[AbstractLogicalPlanBuilder]].
 */
class TestNFABuilder(startStateId: Int, startStateName: String) extends NFABuilder(startStateId, startStateName) {

  def addTransition(
    fromId: Int,
    toId: Int,
    pattern: String,
    maybeRelPredicate: Option[VariablePredicate] = None,
    maybeToPredicate: Option[VariablePredicate] = None
  ): TestNFABuilder = {

    def assertFromNameMatchesFromId(actualState: State, specifiedName: String): Unit = {
      if (actualState.variable.name != specifiedName) {
        throw new IllegalArgumentException(
          s"For id $fromId in pattern '$pattern': expected '${actualState.variable.name}' but was '$specifiedName'"
        )
      }
    }

    val parsedPattern =
      try {
        Parser.parsePatternElement(pattern)
      } catch {
        case NonFatal(e) =>
          println("Error parsing pattern: " + pattern)
          throw e
      }
    parsedPattern match {
      case RelationshipChain(
          NodePattern(Some(from: LogicalVariable), None, None, None),
          RelationshipPattern(
            Some(rel: LogicalVariable),
            relTypeExpression,
            None,
            None,
            relPredicate,
            direction
          ),
          NodePredicate(toName, toNodePredicateFromRel)
        ) =>
        val types = LabelExpression.getRelTypes(relTypeExpression)
        val relVariablePredicate = maybeRelPredicate match {
          case somePred @ Some(_) => somePred
          case None               => relPredicate.map(Expand.VariablePredicate(rel, _))
        }

        val nfaPredicate = RelationshipExpansionPredicate(
          rel,
          relVariablePredicate,
          types,
          direction
        )
        val transition = RelationshipExpansionTransition(nfaPredicate, toId)
        val toNodePredicate = maybeToPredicate match {
          case somePred @ Some(_) => somePred
          case None               => toNodePredicateFromRel
        }
        val fromState = getOrCreateState(fromId, from)
        assertFromNameMatchesFromId(fromState, from.name)
        getOrCreateState(toId, toName, toNodePredicate)
        addTransition(fromState, transition)

      case PathConcatenation(Seq(
          NodePattern(Some(from: LogicalVariable), None, None, None),
          NodePredicate(to, toNodePredicateFromPattern)
        )) =>
        val fromState = getOrCreateState(fromId, from)
        assertFromNameMatchesFromId(fromState, from.name)
        val transition = NodeJuxtapositionTransition(toId)
        val toNodePredicate = maybeToPredicate match {
          case somePred @ Some(_) => somePred
          case None               => toNodePredicateFromPattern
        }
        getOrCreateState(toId, to, toNodePredicate)
        addTransition(fromState, transition)

      case _ => throw new IllegalArgumentException(s"Expected path pattern or two juxtaposed nodes but was: $pattern")
    }
    this
  }

  def setFinalState(id: Int): TestNFABuilder = {
    val state = getState(id)
    setFinalState(state)
    this
  }
}
