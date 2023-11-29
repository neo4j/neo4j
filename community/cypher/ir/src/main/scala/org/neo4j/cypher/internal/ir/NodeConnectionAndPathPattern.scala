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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewritable

sealed trait PathVariable {
  val variable: LogicalVariable
}

case class NodePathVariable(variable: LogicalVariable) extends PathVariable
case class RelationshipPathVariable(variable: LogicalVariable) extends PathVariable

/**
 * Part of a pattern that is connecting nodes (as in "connected components").
 * This is a generalisation of relationships.
 *
 * The implicit contract holds that
 *
 * boundaryNodes == (left, right)
 * boundaryNodesSet == Set(left, right)
 */
sealed trait NodeConnection {

  val left: LogicalVariable
  val right: LogicalVariable
  val nodes: Set[LogicalVariable]
  val relationships: Set[LogicalVariable]

  /**
   * The nodes connected by this node connection. That is, the outer-most nodes in this part of the pattern.
   */
  val boundaryNodes: (LogicalVariable, LogicalVariable)

  lazy val boundaryNodesSet: Set[LogicalVariable] = Set(left, right)

  def withLeft(left: LogicalVariable): NodeConnection
  def withRight(right: LogicalVariable): NodeConnection

  /**
   * All node/relationship/group variables along the path of this node connection, from left to right.
   */
  def pathVariables: Seq[PathVariable]

  /**
   * Same as [[pathVariables]], but as a Set and without PathVariable wrapper class
   */
  final lazy val coveredIds: Set[LogicalVariable] = pathVariables.map(_.variable).toSet

  def otherSide(node: LogicalVariable): LogicalVariable =
    if (node == left) {
      right
    } else if (node == right) {
      left
    } else {
      throw new IllegalArgumentException(
        s"Did not provide either side as an argument to otherSide. Rel: $this, argument: $node"
      )
    }

  /**
   * @return A Cypher representation of this node connection
   */
  def solvedString: String
}

/**
 * This is a node connection that is not restricted by a selector.
 */
sealed trait ExhaustiveNodeConnection extends NodeConnection {

  /**
   * @return same as solvedString, but omitting the left node of the node connection
   */
  def solvedStringSuffix: String

  override def withLeft(left: LogicalVariable): ExhaustiveNodeConnection
  override def withRight(right: LogicalVariable): ExhaustiveNodeConnection
}

object ExhaustiveNodeConnection {

  /**
   * A Cypher String from the given node connections forming a path pattern, left to right.
   */
  def solvedString(ncs: Seq[ExhaustiveNodeConnection]): String = {
    (ncs.head.solvedString +: ncs.tail.map(_.solvedStringSuffix)).mkString("")
  }
}

final case class PatternRelationship(
  variable: LogicalVariable,
  boundaryNodes: (LogicalVariable, LogicalVariable),
  dir: SemanticDirection,
  types: Seq[RelTypeName],
  length: PatternLength
) extends ExhaustiveNodeConnection {

  def directionRelativeTo(node: LogicalVariable): SemanticDirection = if (node == left) dir else dir.reversed

  override def pathVariables: Seq[PathVariable] =
    Seq(NodePathVariable(left), RelationshipPathVariable(variable), NodePathVariable(right))

  override val left: LogicalVariable = boundaryNodes._1
  override val right: LogicalVariable = boundaryNodes._2
  override val nodes: Set[LogicalVariable] = Set(left, right)
  override val relationships: Set[LogicalVariable] = Set(variable)

  override def withLeft(left: LogicalVariable): PatternRelationship = copy(boundaryNodes = (left, right))

  override def withRight(right: LogicalVariable): PatternRelationship = copy(boundaryNodes = (left, right))

  def inOrder: (LogicalVariable, LogicalVariable) = dir match {
    case SemanticDirection.INCOMING => (right, left)
    case _                          => (left, right)
  }

  override def toString: String = solvedString

  override def solvedString: String =
    s"(${boundaryNodes._1.name})$solvedStringSuffix"

  override def solvedStringSuffix: String = {
    val lArrow = if (dir == SemanticDirection.INCOMING) "<" else ""
    val rArrow = if (dir == SemanticDirection.OUTGOING) ">" else ""
    val typesStr =
      if (types.isEmpty) {
        ""
      } else {
        types.map(_.name).mkString(":", "|", "")
      }
    val lengthStr = length match {
      case SimplePatternLength              => ""
      case VarPatternLength(1, None)        => "*"
      case VarPatternLength(x, None)        => s"*$x.."
      case VarPatternLength(min, Some(max)) => s"*$min..$max"
    }
    s"$lArrow-[${variable.name}$typesStr$lengthStr]-$rArrow(${boundaryNodes._2.name})"
  }
}

object PatternRelationship {

  implicit val byName: Ordering[PatternRelationship] = Ordering.by { (patternRel: PatternRelationship) =>
    patternRel.variable.name
  }
}

sealed trait PatternLength {
  def isSimple: Boolean

  def intersect(patternLength: PatternLength): PatternLength
}

case object SimplePatternLength extends PatternLength {
  def isSimple = true

  override def intersect(patternLength: PatternLength): PatternLength = SimplePatternLength
}

final case class VarPatternLength(min: Int, max: Option[Int]) extends PatternLength {
  def isSimple = false

  override def intersect(patternLength: PatternLength): PatternLength = patternLength match {
    case VarPatternLength(otherMin, otherMax) =>
      val newMax = Seq(max, otherMax).flatten.reduceOption(_ min _)
      VarPatternLength(min.max(otherMin), newMax)
    case _ => throw new IllegalArgumentException("VarPatternLength may only be intersected with VarPatternLength")
  }
}

object VarPatternLength {
  def unlimited: VarPatternLength = VarPatternLength(1, None)

  def fixed(length: Int): VarPatternLength = VarPatternLength(length, Some(length))
}

/**
 * Describes the connection between two juxtaposed nodes - one inside of a [[QuantifiedPathPattern]]
 * and the other one outside.
 */
case class NodeBinding(inner: LogicalVariable, outer: LogicalVariable) {
  override def toString: String = s"(inner=$inner, outer=$outer)"
}

/**
 * Describes a variable that is exposed from a [[org.neo4j.cypher.internal.expressions.QuantifiedPath]].
 *
 * @param singletonName the name of the singleton variable inside the QuantifiedPath.
 * @param groupName     the name of the group variable exposed outside of the QuantifiedPath.
 */
case class VariableGrouping(singletonName: LogicalVariable, groupName: LogicalVariable) {
  override def toString: String = s"(singletonName=$singletonName, groupName=$groupName)"
}

object VariableGrouping {

  def singletonToGroup(groupings: Set[VariableGrouping], singletonName: LogicalVariable): Option[LogicalVariable] = {
    groupings.collectFirst {
      case VariableGrouping(`singletonName`, groupName) => groupName
    }
  }
}

final case class QuantifiedPathPattern(
  leftBinding: NodeBinding,
  rightBinding: NodeBinding,
  patternRelationships: NonEmptyList[PatternRelationship],
  argumentIds: Set[LogicalVariable] = Set.empty,
  selections: Selections = Selections(),
  repetition: Repetition,
  nodeVariableGroupings: Set[VariableGrouping],
  relationshipVariableGroupings: Set[VariableGrouping]
) extends ExhaustiveNodeConnection {

  // all pattern nodes are part of a relationship because a QPP has always at least one relationship and is linear in shape
  val patternNodes: Set[LogicalVariable] = patternRelationships.iterator.flatMap(_.boundaryNodesSet).toSet

  // all variables are meant as singletons except those in the groupings
  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    patternRelationships.head.left == leftBinding.inner,
    s"${leftBinding.inner} is not the left node of the first relationship ${patternRelationships.head.left}"
  )

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    patternRelationships.last.right == rightBinding.inner,
    s"${rightBinding.inner} is not the right node of the last relationship ${patternRelationships.last.right}"
  )

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    nodeVariableGroupings.forall(grouping => patternNodes.contains(grouping.singletonName)),
    s"Not all singleton node variables ${nodeVariableGroupings.map(_.singletonName)} were pattern nodes"
  )

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    relationshipVariableGroupings.forall(grouping =>
      patternRelationships.map(_.variable).contains(grouping.singletonName)
    ),
    s"Not all singleton relationship variables ${relationshipVariableGroupings.map(_.singletonName)} were relationship names"
  )

  override val left: LogicalVariable = leftBinding.outer
  override val right: LogicalVariable = rightBinding.outer
  override val nodes: Set[LogicalVariable] = Set(left, right) ++ nodeVariableGroupings.map(_.groupName)
  override val relationships: Set[LogicalVariable] = relationshipVariableGroupings.map(_.groupName)
  override val boundaryNodes: (LogicalVariable, LogicalVariable) = (left, right)
  val variableGroupings: Set[VariableGrouping] = nodeVariableGroupings ++ relationshipVariableGroupings
  val variableGroupNames: Set[LogicalVariable] = variableGroupings.map(_.groupName)

  override def withLeft(left: LogicalVariable): QuantifiedPathPattern =
    copy(leftBinding = leftBinding.copy(outer = left))

  override def withRight(right: LogicalVariable): QuantifiedPathPattern =
    copy(rightBinding = rightBinding.copy(outer = right))

  override def pathVariables: Seq[PathVariable] = {
    val rightTail: Seq[PathVariable] =
      VariableGrouping.singletonToGroup(nodeVariableGroupings, patternRelationships.last.right)
        .map(NodePathVariable) ++: Seq(NodePathVariable(right))

    NodePathVariable(left) +: patternRelationships.iterator.foldRight(rightTail) {
      case (rel, acc) =>
        VariableGrouping.singletonToGroup(nodeVariableGroupings, rel.left)
          .map(NodePathVariable) ++:
          VariableGrouping.singletonToGroup(relationshipVariableGroupings, rel.variable)
            .map(RelationshipPathVariable) ++:
          acc
    }
  }

  override def toString: String =
    s"QPP($leftBinding, $rightBinding, $asQueryGraph, $repetition, $nodeVariableGroupings, $relationshipVariableGroupings)"

  override def solvedStringSuffix: String = {
    val where =
      if (selections.isEmpty) ""
      else selections.flatPredicates.map(QueryGraph.stringifier(_)).mkString(" WHERE ", " AND ", "")
    s" (${ExhaustiveNodeConnection.solvedString(patternRelationships.toIndexedSeq)}$where)${repetition.solvedString} (${rightBinding.outer.name})"
  }

  override def solvedString: String =
    s"(${leftBinding.outer.name})$solvedStringSuffix"

  val dependencies: Set[LogicalVariable] = selections.predicates.flatMap(_.dependencies) ++ argumentIds

  /**
   * Creates a QueryGraph representation of the Quantified Path Pattern and collects all dependent selections eg.
   * MATCH (start) ((a:L)-[r]->(b)-[s]->(c))+ (end) =>
   * MATCH (a)-[r]->(b), (b)-[s]->(c) WHERE a:L
   */
  lazy val asQueryGraph: QueryGraph =
    QueryGraph
      .empty
      .addPatternRelationships(patternRelationships.toSet)
      .addPatternNodes(patternNodes.toList.map(_.name): _*)
      .addArgumentIds(argumentIds.toList.map(_.name))
      .addSelections(selections)
}

sealed trait PathPattern {

  /**
   * @return all quantified sub-path patterns contained in this path pattern
   */
  def allQuantifiedPathPatterns: Set[QuantifiedPathPattern]

  /**
   * @return all node connection sub-path patterns contained in this path pattern
   */
  def allNodeConnections: Set[NodeConnection]
}

/**
 * List of path patterns making up a graph pattern.
 */
case class PathPatterns(pathPatterns: List[PathPattern]) extends AnyVal {

  /**
   * @return all quantified sub-path patterns contained in these path patterns
   */
  def allQuantifiedPathPatterns: Set[QuantifiedPathPattern] =
    pathPatterns.view.flatMap(_.allQuantifiedPathPatterns).toSet

  /**
   * @return all node connections in these path patterns
   */
  def allNodeConnections: Set[NodeConnection] =
    pathPatterns.view.flatMap(_.allNodeConnections).toSet
}

/**
 * A path pattern made of either a single node or a list of node connections.
 * It is exhaustive in that it represents all the paths matching this pattern.
 * Node connections are stored as a list, preserving the order of the pattern as expressed in the query.
 * Each connection contains a reference to the nodes it connects, effectively allowing us to reconstruct the alternating sequence of node and relationship patterns from this type.
 *
 * @tparam A In most cases, should be [[ExhaustiveNodeConnection]], but can be used to narrow down the type of node connections to [[PatternRelationship]] only.
 */
sealed trait ExhaustivePathPattern[+A <: ExhaustiveNodeConnection] extends PathPattern

object ExhaustivePathPattern {

  /**
   * A path pattern of length 0, made of a single node.
   *
   * @param variable the variable bound to the node pattern
   */
  final case class SingleNode[A <: ExhaustiveNodeConnection](variable: LogicalVariable)
      extends ExhaustivePathPattern[A] {
    override def allQuantifiedPathPatterns: Set[QuantifiedPathPattern] = Set.empty
    override def allNodeConnections: Set[NodeConnection] = Set.empty
  }

  /**
   * A path pattern of length 1 or more, made of at least one node connection.
   *
   * @param connections the connections making up the path pattern, in the order in which they appear in the original query.
   * @tparam A In most cases, should be [[ExhaustiveNodeConnection]], but can be used to narrow down the type of node connections to [[PatternRelationship]] only.
   */
  final case class NodeConnections[+A <: ExhaustiveNodeConnection](connections: NonEmptyList[A])
      extends ExhaustivePathPattern[A] {

    override def allQuantifiedPathPatterns: Set[QuantifiedPathPattern] = {
      connections.toSet[ExhaustiveNodeConnection].collect {
        case qpp: QuantifiedPathPattern => qpp
      }
    }

    override def allNodeConnections: Set[NodeConnection] =
      connections.toSet
  }
}

/**
 * A path pattern, its predicates, and a selector limiting the number of paths to find.
 *
 * @param pathPattern path pattern for which we want to find solutions
 * @param selections  so-called "pre-filters", predicates that are applied to the path pattern as part of the path finding algorithm
 * @param selector    path selector such as ANY k, SHORTEST k, or SHORTEST k GROUPS, defining the type of path finding algorithm as well as the number paths to find
 */
final case class SelectivePathPattern(
  pathPattern: NodeConnections[ExhaustiveNodeConnection],
  selections: Selections,
  selector: SelectivePathPattern.Selector
) extends PathPattern with NodeConnection {
  override def allQuantifiedPathPatterns: Set[QuantifiedPathPattern] = pathPattern.allQuantifiedPathPatterns

  override def allNodeConnections: Set[NodeConnection] = pathPattern.allNodeConnections

  override val left: LogicalVariable = pathPattern.connections.head.left
  override val right: LogicalVariable = pathPattern.connections.last.right
  override val nodes: Set[LogicalVariable] = pathPattern.connections.map(_.nodes).toSet.flatten
  override val relationships: Set[LogicalVariable] = pathPattern.connections.map(_.relationships).toSet.flatten
  override val boundaryNodes: (LogicalVariable, LogicalVariable) = (left, right)

  override def withLeft(left: LogicalVariable): SelectivePathPattern = copy(
    pathPattern = pathPattern.copy(
      connections = pathPattern.connections.head.withLeft(left) +: pathPattern.connections.tailOption
    )
  )

  override def withRight(right: LogicalVariable): SelectivePathPattern = copy(
    pathPattern = pathPattern.copy(
      connections = pathPattern.connections.initOption :+ pathPattern.connections.last.withRight(right)
    )
  )

  override def pathVariables: Seq[PathVariable] =
    pathPattern.connections.foldLeft(Seq[PathVariable](NodePathVariable(left))) {
      case (acc, nc) => acc ++ nc.pathVariables.tail
    }

  val dependencies: Set[LogicalVariable] = selections.predicates.flatMap(_.dependencies)

  def solvedString: String = {
    val where =
      if (selections.isEmpty) ""
      else selections.flatPredicates.map(QueryGraph.stringifier(_)).sorted.mkString(" WHERE ", " AND ", "")

    s"${selector.solvedString} (${ExhaustiveNodeConnection.solvedString(pathPattern.connections.toIndexedSeq)}$where)"
  }

  /**
   * Creates a QueryGraph representation of the Selective Path Pattern without the QPPs outer nodes and collects all dependent selections eg.
   * MATCH SHORTEST (foo)-[x]->(start) ((a:L)-[r]->(b)-[s]->(c))+ (end)=>
   * MATCH (foo)-[x]->(start), (a)-[r]->(b), (b)-[s]->(c) WHERE a:L
   */
  lazy val asQueryGraph: QueryGraph =
    pathPattern.connections.foldLeft(QueryGraph
      .empty) { (acc, nodeCon) =>
      nodeCon match {
        case patternRelationship: PatternRelationship =>
          acc.addPatternRelationship(patternRelationship)
        case innerQpp: QuantifiedPathPattern =>
          val innerQppAsQueryGraph = innerQpp.asQueryGraph

          // We do not need to take the outer nodes into consideration here
          acc.addPatternRelationships(innerQppAsQueryGraph.patternRelationships)
            // since they are added in the pattern relationship part of the selective path pattern or are considered for analysis in the outer QueryGraph
            .addPatternNodes(innerQppAsQueryGraph.patternNodes.diff(boundaryNodesSet.map(_.name)).toList: _*)
            .addSelections(innerQppAsQueryGraph.selections)
            .addArgumentIds(innerQppAsQueryGraph.argumentIds.toSeq)
      }
    }.addSelections(selections)

  lazy val varLengthRelationships: Seq[LogicalVariable] = pathPattern.connections.toIndexedSeq.collect {
    case PatternRelationship(name, _, _, _, _: VarPatternLength) => name
  }
}

object SelectivePathPattern {

  /**
   * Defines the paths to find for each combination of start and end nodes.
   */
  sealed trait Selector {

    /**
     * @return A Cypher representation of this selector
     */
    def solvedString: String
  }

  object Selector {

    /**
     * Finds up to k paths arbitrarily.
     */
    case class Any(k: Long) extends Selector {
      override def solvedString: String = s"ANY $k"
    }

    /**
     * Returns the shortest, second-shortest, etc. up to k paths.
     * If there are multiple paths of same length, picks arbitrarily.
     */
    case class Shortest(k: Long) extends Selector {
      override def solvedString: String = s"SHORTEST $k"
    }

    /**
     * Finds all shortest paths, all second shortest paths, etc. up to all Kth shortest paths.
     */
    case class ShortestGroups(k: Long) extends Selector {
      override def solvedString: String = s"SHORTEST $k GROUPS"
    }
  }
}

//noinspection ZeroIndexToHead
final case class ShortestRelationshipPattern(
  maybePathVar: Option[LogicalVariable],
  rel: PatternRelationship,
  single: Boolean
)(
  val expr: ShortestPathsPatternPart
) extends PathPattern with Rewritable {

  def dup(children: Seq[AnyRef]): this.type =
    copy(
      children(0).asInstanceOf[Option[LogicalVariable]],
      children(1).asInstanceOf[PatternRelationship],
      children(2).asInstanceOf[Boolean]
    )(expr).asInstanceOf[this.type]

  def isFindableFrom(symbols: Set[LogicalVariable]): Boolean = symbols.contains(rel.left) && symbols.contains(rel.right)

  def availableSymbols: Set[LogicalVariable] = maybePathVar.toSet ++ rel.coveredIds

  override def allQuantifiedPathPatterns: Set[QuantifiedPathPattern] = Set.empty

  override def allNodeConnections: Set[NodeConnection] = Set.empty
}

object ShortestRelationshipPattern {

  implicit val byRelName: Ordering[ShortestRelationshipPattern] = Ordering.by { (sp: ShortestRelationshipPattern) =>
    sp.rel
  }
}
