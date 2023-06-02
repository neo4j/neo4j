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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.State.VarName
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.logical.plans.NFA.Transitions

object NFA {

  object State {
    private[plans] def expressionStringifier: ExpressionStringifier = ExpressionStringifier(_.asCanonicalStringVal)

    sealed trait VarName {
      val name: LogicalVariable

      def toDotString: String
    }

    object VarName {

      case class SingletonVarName(name: LogicalVariable) extends VarName {
        override def toDotString: String = name.name
      }

      case class GroupVarName(name: LogicalVariable) extends VarName {
        override def toDotString: String = s"<i>${name.name}</i>"
      }
    }
  }

  /**
   * A State is associated with a node (varName) and an ID that must be unique per NFA.
   */
  case class State(id: Int, varName: VarName) {
    def toDotString: String = s"<($id, ${varName.toDotString})>"
  }

  /**
   * A predicate that dictates whether a transition may be applied.
   */
  sealed trait Predicate {
    def variablePredicates: Seq[VariablePredicate]

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
    override def variablePredicates: Seq[VariablePredicate] = variablePredicate.toSeq

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
   * @param relVarName the name of the relationship. `r` in the example.
   * @param relPred the predicate for the relationship `r.prop = 5` in the example.
   * @param types the allowed types of the relationship. This is not inlined into relPred because this predicate can be
   *              executed more efficiently. `Seq(R,Q)` in the example.
   * @param dir the direction of the relationship, from the perspective of the start of the transition.
   *            `OUTGOING` in the example.
   * @param nodePred the predicate for the node of the end state of the transition. `b:B` in the example.
   */
  case class RelationshipExpansionPredicate(
    relVarName: VarName,
    relPred: Option[VariablePredicate],
    types: Seq[RelTypeName],
    dir: SemanticDirection,
    nodePred: Option[VariablePredicate]
  ) extends Predicate {

    override def variablePredicates: Seq[VariablePredicate] = Seq(relPred, nodePred).flatten

    override def toDotString: String = {
      val (dirStrA, dirStrB) = LogicalPlanToPlanBuilderString.arrows(dir)
      val typeStr = LogicalPlanToPlanBuilderString.relTypeStr(types)
      val relWhere = relPred.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      val nodeWhere = nodePred.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      val nodeName = nodePred.map(_.variable.name).getOrElse("")
      s"()$dirStrA[${relVarName.name}$typeStr$relWhere]$dirStrB($nodeName$nodeWhere)"
    }
  }

  /**
   * A conditional transition. The condition is expressed in the predicate.
   * The start state is omitted, since it is the map key in the [[NFA.transitions]] map.
   *
   * @param predicate the condition under which the transition may be applied.
   * @param end the end state of this transition.
   */
  final case class Transition[+P <: Predicate](predicate: P, end: State)

  /**
   * The outgoing transitions of a state can either all have NodeJuxtapositionPredicates
   * or all have RelationshipExpansionPredicates. This trait is here to guarantee this in a type-safe way.
   */
  sealed trait Transitions {
    def transitions: Set[_ <: Transition[Predicate]]
  }

  object Transitions {
    def unapply(v: Transitions): Some[Set[_ <: Transition[Predicate]]] = Some(v.transitions)
  }

  case class NodeJuxtapositionTransitions(transitions: Set[Transition[NodeJuxtapositionPredicate]])
      extends Transitions

  case class RelationshipExpansionTransitions(transitions: Set[Transition[RelationshipExpansionPredicate]])
      extends Transitions
}

/**
 * A non finite automaton, used to solve SHORTEST.
 *
 * @param states the states of the automaton
 * @param transitions a map that maps each state to its outgoing transitions.
 * @param startState the start state. Must be an element of states.
 * @param finalStates all final states. Must be a subset of states.
 */
case class NFA(
  states: Set[State],
  transitions: Map[State, Transitions],
  startState: State,
  finalStates: Set[State]
) {

  def nodeNames: Set[LogicalVariable] = states.map(_.varName.name)

  def relationshipNames: Set[LogicalVariable] =
    transitions.flatMap(_._2.transitions).map(_.predicate).collect {
      case RelationshipExpansionPredicate(relVarName, _, _, _, _) =>
        relVarName.name
    }.toSet

  def availableSymbols: Set[LogicalVariable] = nodeNames ++ relationshipNames

  /**
   * @return a DOT String to generate a graphviz. For example use
   *         https://dreampuf.github.io/GraphvizOnline/
   */
  def toDotString: String = {
    val nodes = states
      .toSeq
      .map { node =>
        val style = if (node == startState) " style=filled" else ""
        val shape = if (finalStates.contains(node)) " shape=doublecircle" else ""
        s"""  ${node.id} [label=${node.toDotString}$style$shape];"""
      }.mkString("\n")
    val edges =
      transitions.toSeq
        .flatMap { case (start, Transitions(transitions)) => transitions.map(start -> _) }
        .map {
          case (start, Transition(p, end)) => s"""  ${start.id} -> ${end.id} [label="${p.toDotString}"];"""
        }
        .mkString("\n")
    s"digraph G {\n$nodes\n$edges\n}"
  }
}
