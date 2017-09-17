/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.apa.v3_4.{ASTNode, Rewriter, topDown}
import org.neo4j.cypher.internal.frontend.v3_4.IdentityMap
import org.neo4j.cypher.internal.frontend.v3_4.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.v3_4.expressions._

object PatternExpressionPatternElementNamer {

  def apply(expr: PatternExpression): (PatternExpression, Map[PatternElement, Variable]) = {
    val unnamedMap = nameUnnamedPatternElements(expr.pattern)
    val namedPattern = expr.pattern.endoRewrite(namePatternElementsFromMap(unnamedMap))
    val namedExpr = expr.copy(pattern = namedPattern)
    (namedExpr, unnamedMap)
  }

  def apply(expr: PatternComprehension): (PatternComprehension, Map[PatternElement, Variable]) = {
    val unnamedMap = nameUnnamedPatternElements(expr.pattern)
    val namedPattern = expr.pattern.endoRewrite(namePatternElementsFromMap(unnamedMap))
    val namedExpr = expr.copy(pattern = namedPattern)(expr.position)
    (namedExpr, unnamedMap)
  }

  private def nameUnnamedPatternElements(pattern: RelationshipsPattern): Map[PatternElement, Variable] = {
    val unnamedElements = findPatternElements(pattern).filter(_.variable.isEmpty)
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
