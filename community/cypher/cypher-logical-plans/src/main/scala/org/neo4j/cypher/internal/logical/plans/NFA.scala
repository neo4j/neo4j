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
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Rewritable

import scala.collection.immutable.ArraySeq

object NFA {

  object State {
    private[plans] def expressionStringifier: ExpressionStringifier = ExpressionStringifier(_.asCanonicalStringVal)
  }

  /**
   * A State is associated with a node (variable) and an ID that must be unique per NFA.
   *
   * It may have a predicate on the node which must be satisfied to enter the state.
   * E.g., if this predicate is part of a transition from state a to state b, and the pattern looks like this:
   * {{{(a) ( (b:B)-->() )*}}}
   * then the variablePredicate should contain `b:B`
   */
  case class State(id: Int, variable: LogicalVariable, predicate: Option[VariablePredicate]) {

    def toDotString: String = {
      val nodeWhere = predicate.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      s"\"($id, ${variable.name}$nodeWhere)\""
    }
  }

  /**
   * A predicate that dictates whether a transition may be applied.
   */
  sealed trait Predicate {
    def variable: Option[LogicalVariable]
    def toDotString: String
  }

  /**
   * This predicate is used when two nodes are juxtaposed in a pattern, e.g.
   * {{{(a) (b)}}}
   */
  object NodeJuxtapositionPredicate extends Predicate {
    override def variable: Option[LogicalVariable] = None
    override def toDotString: String = ""
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
   */
  case class RelationshipExpansionPredicate(
    relationshipVariable: LogicalVariable,
    relPred: Option[VariablePredicate],
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ) extends Predicate {

    override def variable: Option[LogicalVariable] =
      relPred.map(_.variable)

    override def toDotString: String = {
      val (dirStrA, dirStrB) = LogicalPlanToPlanBuilderString.arrows(dir)
      val typeStr = LogicalPlanToPlanBuilderString.relTypeStr(types)
      val relWhere = relPred.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      s"()$dirStrA[${relationshipVariable.name}$typeStr$relWhere]$dirStrB()"
    }

  }

  /**
   * A conditional transition. The condition is expressed in the predicate.
   * The start state is omitted, since it is the map key in the [[NFA.transitions]] map.
   *
   * @param predicate the condition under which the transition may be applied.
   * @param endId the end state of this transition.
   */
  case class Transition(
    predicate: Predicate,
    endId: Int
  )
}

/**
 * A non-deterministic finite automaton, used to solve SHORTEST.
 *
 * @param states the states of the automaton
 * @param transitions a map that maps each state id to its outgoing transitions.
 * @param startId the start state id. Must be an element of states.
 * @param finalId the final state id. Must be an element of states.
 */
case class NFA(
  states: ArraySeq[State],
  transitions: Map[Int, Set[Transition]],
  startId: Int,
  finalId: Int
) extends Rewritable with Foldable {
  def startState: State = states(startId)
  def finalState: State = states(finalId)

  def predicateVariables: Set[LogicalVariable] =
    (states.iterator.flatMap(_.predicate).map(_.variable)
      ++ transitions.values.flatten.iterator.flatMap(_.predicate.variable)).toSet

  def nodes: Set[LogicalVariable] =
    states.map(_.variable).toSet ++ states.flatMap(_.predicate).map(_.variable)

  def relationships: Set[LogicalVariable] =
    transitions.iterator
      .flatMap(_._2)
      .map(_.predicate)
      .collect { case r: RelationshipExpansionPredicate => r }
      .flatMap(r => Iterator(Some(r.relationshipVariable), r.relPred.map(_.variable)).flatten)
      .toSet

  override def toString: String = {
    val states = this.states.toList.sortBy(_.id)
    val transitions = this.transitions.view.mapValues(
      _.toList.sortBy(_.endId).mkString("[\n  ", "\n  ", "\n]")
    ).toList.sortBy(_._1).mkString("\n")
    s"NFA($states,\n$transitions\n, $startState, $finalState)"
  }

  /**
   * @return a DOT String to generate a graphviz. For example use
   *         https://dreampuf.github.io/GraphvizOnline/
   */
  def toDotString: String = {
    val nodes = states
      .sortBy(_.id)
      .map { node =>
        val style = if (node.id == startState.id) " style=filled" else ""
        val shape = if (node.id == finalState.id) " shape=doublecircle" else ""
        s"""  ${node.id} [label=${node.toDotString}$style$shape];"""
      }.mkString("\n")
    val edges =
      transitions.toSeq
        .flatMap { case (start, transitions) => transitions.map(start -> _) }
        .sortBy {
          case (start, Transition(_, endId)) => (start, endId)
        }
        .map {
          case (start, Transition(p, endId)) =>
            s"""  ${start} -> ${endId} [label="${p.toDotString}"];"""
        }
        .mkString("\n")
    s"digraph G {\n$nodes\n$edges\n}"
  }

  // noinspection ZeroIndexToHead
  def dup(children: Seq[AnyRef]): this.type = NFA(
    children(0).asInstanceOf[ArraySeq[State]],
    children(1).asInstanceOf[Map[Int, Set[Transition]]],
    children(2).asInstanceOf[Int],
    children(3).asInstanceOf[Int]
  ).asInstanceOf[this.type]

  private def assertSortedStates: Boolean = {
    !states.zipWithIndex.exists {
      case (state, index) => state.id != index
      case _              => false
    }
  }

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    assertSortedStates,
    "NFA States should be sorted by internal State Id"
  )
}
