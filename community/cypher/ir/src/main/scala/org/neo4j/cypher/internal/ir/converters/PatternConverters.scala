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
package org.neo4j.cypher.internal.ir.converters

import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.ir.ExhaustiveNodeConnection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.SingleNode
import org.neo4j.cypher.internal.ir.PathPattern
import org.neo4j.cypher.internal.ir.PathPatterns
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern.Selector
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.NonEmptyList

/**
 * @param anonymousVariableNameGenerator generator used for anonymous shortest var-length relationship patterns
 */
class PatternConverters(anonymousVariableNameGenerator: AnonymousVariableNameGenerator) {

  def convertPattern(pattern: Pattern.ForMatch): PathPatterns =
    PathPatterns(pattern.patternParts.toList.map(convertPatternPart))

  private def convertPatternPart(patternPart: PatternPartWithSelector): PathPattern =
    patternPart.part match {
      case NamedPatternPart(variable, anonymousPatternPart) =>
        convertAnonymousPatternPart(patternPart.selector, Some(variable), anonymousPatternPart)
      case anonymousPatternPart: AnonymousPatternPart =>
        convertAnonymousPatternPart(patternPart.selector, None, anonymousPatternPart)
    }

  private def convertAnonymousPatternPart(
    selector: PatternPart.Selector,
    pathName: Option[LogicalVariable],
    anonymousPatternPart: AnonymousPatternPart
  ): PathPattern =
    anonymousPatternPart match {
      case PathPatternPart(element) =>
        selector match {
          case PatternPart.AllPaths() =>
            convertPatternElement(element)
          case PatternPart.AnyPath(count) =>
            convertShortestPathPatternElement(element, Selector.Any(count.value))
          case PatternPart.AnyShortestPath(count) =>
            convertShortestPathPatternElement(element, Selector.Shortest(count.value))
          case PatternPart.AllShortestPaths() =>
            convertShortestPathPatternElement(element, Selector.ShortestGroups(1))
          case PatternPart.ShortestGroups(count) =>
            convertShortestPathPatternElement(element, Selector.ShortestGroups(count.value))
        }
      case part @ ShortestPathsPatternPart(element, single) =>
        element match {
          case RelationshipChain(leftNode: NodePattern, relationship, rightNode) =>
            ShortestRelationshipPattern(
              maybePathVar = Some(pathName.getOrElse(varFor(anonymousVariableNameGenerator.nextName))),
              rel = SimplePatternConverters.makePatternRelationship(leftNode, relationship, rightNode),
              single = single
            )(expr = part)
          case other =>
            throw new IllegalArgumentException(
              s"${part.name}() must contain a single relationship, it cannot contain a ${other.productPrefix}"
            )
        }
    }

  private def convertShortestPathPatternElement(
    patternElement: PatternElement,
    selector: Selector
  ): SelectivePathPattern =
    patternElement match {
      case ParenthesizedPath(part, predicate) =>
        convertShortestPathPatternPart(part, predicate, selector)
      case other =>
        convertUnwrappedShortestPathPatternElement(other, Selections.empty, selector)

    }

  private def convertShortestPathPatternPart(
    part: NonPrefixedPatternPart,
    predicate: Option[Expression],
    selector: Selector
  ): SelectivePathPattern = {
    part match {
      case PathPatternPart(element) =>
        convertUnwrappedShortestPathPatternElement(element, Selections.from(predicate), selector)

      case shortest: ShortestPathsPatternPart =>
        throw new IllegalArgumentException(
          s"${shortest.name}() is not allowed inside of a parenthesised path pattern"
        )
      case _: NamedPatternPart =>
        throw new IllegalArgumentException("Sub-path assignment is currently not supported")
    }
  }

  /**
   * Convert the given pattern element that we know to be a node connection. That is, parentheses have been unwrapped before.
   */
  private def convertUnwrappedShortestPathPatternElement(
    element: PatternElement,
    selections: Selections,
    selector: Selector
  ): SelectivePathPattern = {
    convertPatternElement(element) match {
      case connections: NodeConnections[ExhaustiveNodeConnection] =>
        SelectivePathPattern(connections, selections, selector)
      case _: SingleNode[_] =>
        throw new IllegalStateException(
          "Shortest paths over a single node should have been rewritten by FixedLengthShortestToAllRewriter"
        )
    }
  }

  private def convertPatternElement(patternElement: PatternElement): ExhaustivePathPattern[ExhaustiveNodeConnection] =
    patternElement match {
      case pattern: SimplePattern     => SimplePatternConverters.convertSimplePattern(pattern)
      case PathConcatenation(factors) => convertPathFactors(factors.toList)
      case _: QuantifiedPath =>
        throw new IllegalArgumentException("Quantified path patterns must be concatenated with outer node patterns")
      case _: ParenthesizedPath =>
        throw new IllegalArgumentException(
          "Parenthesised path patterns are only supported at the top level with a selective path selector"
        )
    }

  private def convertPathFactors(factors: List[PathFactor]): ExhaustivePathPattern[ExhaustiveNodeConnection] =
    factors match {
      case (pattern: SimplePattern) :: tail =>
        buildPathPattern(pattern, unfoldPathFactorsTail(tail))
      case Nil =>
        throw new IllegalArgumentException("Cannot concatenate an empty list of path factors")
      case other :: _ =>
        throw new IllegalArgumentException(
          s"Concatenated path factors must start with a simple pattern, not a ${other.productPrefix}"
        )
    }

  private def unfoldPathFactorsTail(factors: List[PathFactor]): LazyList[(QuantifiedPath, SimplePattern)] =
    LazyList.unfold(factors) {
      case Nil => None
      case (quantifiedPath: QuantifiedPath) :: tail =>
        tail match {
          case (simplePattern: SimplePattern) :: tail2 => Some(((quantifiedPath, simplePattern), tail2))
          case Nil =>
            throw new IllegalArgumentException(
              "A quantified path pattern must be concatenated with a simple path pattern"
            )
          case other :: _ =>
            throw new IllegalArgumentException(
              s"A quantified path pattern must be concatenated with a simple path pattern, not with a ${other.productPrefix}"
            )
        }
      case other :: _ =>
        throw new IllegalArgumentException(
          s"A simple path pattern may only be concatenated with a quantified path pattern, not with a ${other.productPrefix}"
        )
    }

  private def buildPathPattern(
    head: SimplePattern,
    tail: LazyList[(QuantifiedPath, SimplePattern)]
  ): ExhaustivePathPattern[ExhaustiveNodeConnection] =
    tail.foldLeft[ExhaustivePathPattern[ExhaustiveNodeConnection]](SimplePatternConverters.convertSimplePattern(head)) {
      case (pathPattern, (quantifiedPath, simplePattern)) =>
        val (previousRightMostNode, previousConnections) = pathPattern match {
          case ExhaustivePathPattern.SingleNode(name)             => (name, Nil)
          case ExhaustivePathPattern.NodeConnections(connections) => (connections.last.right, connections.toIterable)
        }

        val (nextLeftMostNode, nextConnections) = SimplePatternConverters.convertSimplePattern(simplePattern) match {
          case ExhaustivePathPattern.SingleNode(name) => (name, Nil)
          case ExhaustivePathPattern.NodeConnections(relationships) =>
            (relationships.head.left, relationships.toIterable)
        }

        val quantifiedPathPattern =
          QuantifiedPathPatternConverters.convertQuantifiedPath(
            outerLeft = previousRightMostNode,
            quantifiedPath = quantifiedPath,
            outerRight = nextLeftMostNode
          )

        ExhaustivePathPattern.NodeConnections(
          previousConnections ++: NonEmptyList(quantifiedPathPattern) :++ nextConnections
        )
    }
}
