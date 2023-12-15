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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Rewritable

object NFA {

  object State {
    private[plans] def expressionStringifier: ExpressionStringifier = ExpressionStringifier(_.asCanonicalStringVal)
  }

  /**
   * A State is associated with a node (variable) and an ID that must be unique per NFA.
   */
  case class State(id: Int, variable: LogicalVariable) {
    def toDotString: String = s"<($id, ${variable.name})>"
  }

  /**
   * A predicate that dictates whether a transition may be applied.
   */
  sealed trait Predicate {
    def variables: Seq[LogicalVariable]

    def toDotString: String
  }

  /**
   * This predicate is used when two nodes are juxtaposed in a pattern, e.g.
   * {{{(a) (b)}}}
   * The optional variablePredicate contains any predicates on the node of the state we're transitioning into.
   * E.g., if this predicate is part of a transition from state a to state b, and the pattern looks like this:
   * {{{(a) ( (b:B)-->() )*}}}
   * then the variablePredicate should contain `b:B`
   */
  case class NodeJuxtapositionPredicate(variablePredicate: Option[VariablePredicate]) extends Predicate {
    override def variables: Seq[LogicalVariable] = variablePredicate.toSeq.map(_.variable)

    override def toDotString: String =
      variablePredicate.map(vp => State.expressionStringifier(vp.predicate)).getOrElse("")
  }

  /**
   * This predicate is used for a relationship in a pattern.
   * There are optional variablePredicates for both the relationship and the node of the end state of this transition.
   *
   * Below, we use this pattern as an example, assuming we're transitioning from a to b.
   * {{{(a)-[r:R|Q WHERE r.prop = 5]->(b:B)}}}
   *
   * @param relationshipVariable the relationship. `r` in the example.
   * @param relPred the predicate for the relationship `r.prop = 5` in the example.
   * @param types the allowed types of the relationship. This is not inlined into relPred because this predicate can be
   *              executed more efficiently. `Seq(R,Q)` in the example.
   * @param dir the direction of the relationship, from the perspective of the start of the transition.
   *            `OUTGOING` in the example.
   * @param nodePred the predicate for the node of the end state of the transition. `b:B` in the example.
   */
  case class RelationshipExpansionPredicate(
    relationshipVariable: LogicalVariable,
    relPred: Option[VariablePredicate],
    types: Seq[RelTypeName],
    dir: SemanticDirection,
    nodePred: Option[VariablePredicate]
  ) extends Predicate {

    override def variables: Seq[LogicalVariable] =
      relationshipVariable +: Seq(relPred, nodePred).flatten.map(_.variable)

    override def toDotString: String = {
      val (dirStrA, dirStrB) = LogicalPlanToPlanBuilderString.arrows(dir)
      val typeStr = LogicalPlanToPlanBuilderString.relTypeStr(types)
      val relWhere = relPred.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      val nodeWhere = nodePred.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      val nodeName = nodePred.map(_.variable.name).getOrElse("")
      s"()$dirStrA[${relationshipVariable.name}$typeStr$relWhere]$dirStrB($nodeName$nodeWhere)"
    }
  }

  /**
   * A conditional transition. The condition is expressed in the predicate.
   * The start state is omitted, since it is the map key in the [[NFA.transitions]] map.
   *
   * @param predicate the condition under which the transition may be applied.
   * @param end the end state of this transition.
   */
  case class Transition(predicate: Predicate, end: State)
}

/**
 * A non finite automaton, used to solve SHORTEST.
 *
 * @param states the states of the automaton
 * @param transitions a map that maps each state to its outgoing transitions.
 * @param startState the start state. Must be an element of states.
 * @param finalState the final state. Must be an element of states.
 */
case class NFA(
  states: Set[State],
  transitions: Map[State, Set[Transition]],
  startState: State,
  finalState: State
) extends Rewritable with Foldable {

  def predicateVariables: Set[LogicalVariable] = {
    transitions.values.flatten.toSeq.flatMap(_.predicate.variables).toSet
  }

  def nodes: Set[LogicalVariable] =
    states.map(_.variable) ++ transitionPredicates.flatMap {
      case r: RelationshipExpansionPredicate => r.nodePred
      case n: NodeJuxtapositionPredicate     => n.variablePredicate
    }.map(_.variable)

  def relationships: Set[LogicalVariable] =
    transitionPredicates
      .collect { case r: RelationshipExpansionPredicate => r }
      .flatMap(r => Iterator(Some(r.relationshipVariable), r.relPred.map(_.variable)).flatten)
      .toSet

  private def transitionPredicates = transitions.iterator.flatMap(_._2).map(_.predicate)

  override def toString: String = {
    val states = this.states.toList.sortBy(_.id)
    val transitions = this.transitions.view.mapValues(
      _.toList.sortBy(_.end.id).mkString("[\n  ", "\n  ", "\n]")
    ).toList.sortBy(_._1.id).mkString("\n")
    s"NFA($states,\n$transitions\n, $startState, $finalState)"
  }

  /**
   * @return a DOT String to generate a graphviz. For example use
   *         https://dreampuf.github.io/GraphvizOnline/
   */
  def toDotString: String = {
    val nodes = states
      .toSeq
      .sortBy(_.id)
      .map { node =>
        val style = if (node == startState) " style=filled" else ""
        val shape = if (node == finalState) " shape=doublecircle" else ""
        s"""  ${node.id} [label=${node.toDotString}$style$shape];"""
      }.mkString("\n")
    val edges =
      transitions.toSeq
        .flatMap { case (start, transitions) => transitions.map(start -> _) }
        .sortBy {
          case (start, Transition(_, end)) => (start.id, end.id)
        }
        .map {
          case (start, Transition(p, end)) => s"""  ${start.id} -> ${end.id} [label="${p.toDotString}"];"""
        }
        .mkString("\n")
    s"digraph G {\n$nodes\n$edges\n}"
  }

  // noinspection ZeroIndexToHead
  def dup(children: Seq[AnyRef]): this.type = NFA(
    children(0).asInstanceOf[Set[State]],
    children(1).asInstanceOf[Map[State, Set[Transition]]],
    children(2).asInstanceOf[State],
    children(3).asInstanceOf[State]
  ).asInstanceOf[this.type]
}
