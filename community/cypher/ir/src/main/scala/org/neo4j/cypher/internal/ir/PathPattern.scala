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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.util.Rewritable

sealed trait PathPattern {

  /**
   * @return all quantified sub-path patterns contained in this path pattern
   */
  def allQuantifiedPathPatterns: List[QuantifiedPathPattern]
}

/**
 * List of path patterns making up a graph pattern.
 */
case class PathPatterns(pathPatterns: List[PathPattern]) extends AnyVal {

  /**
   * @return all quantified sub-path patterns contained in these path patterns
   */
  def allQuantifiedPathPatterns: List[QuantifiedPathPattern] = pathPatterns.flatMap(_.allQuantifiedPathPatterns)
}

/**
 * A path pattern made of either a single node or a list of node connections.
 * It is exhaustive in that it represents all the paths matching this pattern.
 * Node connections are stored as a list, preserving the order of the pattern as expressed in the query.
 * Each connection contains a reference to the nodes it connects, effectively allowing us to reconstruct the alternating sequence of node and relationship patterns from this type.
 * @tparam A In most cases, should be [[NodeConnection]], but can be used to narrow down the type of node connections to [[PatternRelationship]] only.
 */
sealed trait ExhaustivePathPattern[+A <: NodeConnection] extends PathPattern

object ExhaustivePathPattern {

  /**
   * A path pattern of length 0, made of a single node.
   * @param name name of the variable bound to the node pattern
   */
  final case class SingleNode[A <: NodeConnection](name: String) extends ExhaustivePathPattern[A] {
    override def allQuantifiedPathPatterns: List[QuantifiedPathPattern] = Nil
  }

  /**
   * A path pattern of length 1 or more, made of at least one node connection.
   * @param connections the connections making up the path pattern, in the order in which they appear in the original query.
   * @tparam A In most cases, should be [[NodeConnection]], but can be used to narrow down the type of node connections to [[PatternRelationship]] only.
   */
  final case class NodeConnections[A <: NodeConnection](connections: List[A]) extends ExhaustivePathPattern[A] {

    override def allQuantifiedPathPatterns: List[QuantifiedPathPattern] =
      connections.collect {
        case qpp: QuantifiedPathPattern => qpp
      }
  }
}

/**
   * A path pattern, its predicates, and a selector limiting the number of paths to find.
   * @param pathPattern path pattern for which we want to find solutions
   * @param selections so-called "pre-filters", predicates that are applied to the path pattern as part of the path finding algorithm
   * @param selector path selector such as ANY k, SHORTEST k, or SHORTEST k GROUPS, defining the type of path finding algorithm as well as the number paths to find
   */
final case class SelectivePathPattern(
  pathPattern: ExhaustivePathPattern[NodeConnection],
  selections: Selections,
  selector: SelectivePathPattern.Selector
) extends PathPattern {
  override def allQuantifiedPathPatterns: List[QuantifiedPathPattern] = pathPattern.allQuantifiedPathPatterns
}

object SelectivePathPattern {

  /**
   * Defines the paths to find for each combination of start and end nodes.
   */
  sealed trait Selector

  object Selector {

    /**
     * Finds up to k paths arbitrarily.
     */
    case class Any(k: Long) extends Selector

    /**
     * Returns the shortest, second-shortest, etc. up to k paths.
     * If there are multiple paths of same length, picks arbitrarily.
     */
    case class Shortest(k: Long) extends Selector

    /**
     * Finds all shortest paths, all second shortest paths, etc. up to all Kth shortest paths.
     */
    case class ShortestGroups(k: Long) extends Selector
  }
}

//noinspection ZeroIndexToHead
final case class ShortestRelationshipPattern(name: Option[String], rel: PatternRelationship, single: Boolean)(
  val expr: ShortestPathsPatternPart
) extends PathPattern with Rewritable {

  def dup(children: Seq[AnyRef]): this.type =
    copy(
      children(0).asInstanceOf[Option[String]],
      children(1).asInstanceOf[PatternRelationship],
      children(2).asInstanceOf[Boolean]
    )(expr).asInstanceOf[this.type]

  def isFindableFrom(symbols: Set[String]): Boolean = symbols.contains(rel.left) && symbols.contains(rel.right)

  def availableSymbols: Set[String] = name.toSet ++ rel.coveredIds

  override def allQuantifiedPathPatterns: List[QuantifiedPathPattern] = Nil
}

object ShortestRelationshipPattern {

  implicit val byRelName: Ordering[ShortestRelationshipPattern] = Ordering.by { sp: ShortestRelationshipPattern =>
    sp.rel
  }
}
