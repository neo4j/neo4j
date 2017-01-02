/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.{IdentityMap, Rewriter, topDown}

object PatternExpressionPatternElementNamer {

  def apply(expr: PatternExpression): (PatternExpression, Map[PatternElement, Variable]) = {
    val unnamedMap = nameUnnamedPatternElements(expr)
    val namedPattern = expr.pattern.endoRewrite(namePatternElementsFromMap(unnamedMap))
    val namedExpr = expr.copy(pattern = namedPattern)
    (namedExpr, unnamedMap)
  }

  private def nameUnnamedPatternElements(expr: PatternExpression): Map[PatternElement, Variable] = {
    val unnamedElements = findPatternElements(expr.pattern).filter(_.variable.isEmpty)
    IdentityMap(unnamedElements.map {
      case elem: NodePattern =>
        elem -> Variable(UnNamedNameGenerator.name(elem.position.bumped()))(elem.position)
      case elem@RelationshipChain(_, relPattern, _) =>
        elem -> Variable(UnNamedNameGenerator.name(relPattern.position.bumped()))(relPattern.position)
    }: _*)
  }

  private case object findPatternElements {
    def apply(astNode: ASTNode): Seq[PatternElement] = astNode.treeFold(Seq.empty[PatternElement]) {
      case patternElement: PatternElement =>
        acc => (acc :+ patternElement, Some(identity))

      case patternExpr: PatternExpression =>
        acc => (acc, None)
    }
  }

  private case class namePatternElementsFromMap(map: Map[PatternElement, Variable]) extends Rewriter {
    override def apply(that: AnyRef): AnyRef = instance.apply(that)

    private val instance: Rewriter = topDown(Rewriter.lift {
      case pattern: NodePattern if map.contains(pattern) =>
        pattern.copy(variable = Some(map(pattern)))(pattern.position)
      case pattern: RelationshipChain if map.contains(pattern) =>
        val rel = pattern.relationship
        pattern.copy(relationship = rel.copy(variable = Some(map(pattern)))(rel.position))(pattern.position)
    })
  }
}

