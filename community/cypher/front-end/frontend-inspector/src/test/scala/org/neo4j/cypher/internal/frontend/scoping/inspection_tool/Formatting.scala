/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.scoping.inspection_tool

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

import java.util.Locale

object Formatting {

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  def prettify(astNode: ASTNode): String =
    astNode match {
      case statement: Statement                => prettifier.asString(statement)
      case definition: LocalCallableDefinition => prettifier.asString(definition)
      case clause: Clause                      => prettifier.asString(SingleQuery(Seq(clause))(InputPosition.NONE))
      case groupBy: GroupBy                    => prettifier.asString(groupBy)
      case search: Search                      => prettifier.asString(search)
      case expression: Expression              => prettifier.expr(expression)
      case cypherPattern: Pattern              => prettifier.expr.patterns(cypherPattern)
      case patternPart: PatternPart            => prettifier.expr.patterns(patternPart)
      case patternElement: PatternElement      => prettifier.expr.patterns(patternElement)
      case relationship: RelationshipPattern   => prettifier.expr.patterns(relationship)
      case labelExpression: LabelExpression    => prettifier.expr.stringifyLabelExpression(labelExpression)
      case conditionalBranch @ ConditionalQueryBranch(Some(_), _) =>
        prettifier.asString(ConditionalQueryWhen(Seq(conditionalBranch), None)(InputPosition.NONE))
      case conditionalBranch @ ConditionalQueryBranch(None, _) =>
        prettifier.asString(ConditionalQueryWhen(Seq.empty, Some(conditionalBranch))(InputPosition.NONE))
      case other => other.toString
    }

  def getClassNameWithoutTrailingDollarSign(x: Any): String = {
    val className = x.getClass.getSimpleName
    if (className.endsWith("$")) className.init else className
  }

  def camelCaseToLowerCaseWithSpaces(value: String): String =
    value
      .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
      .replaceAll("([A-Z])([A-Z][a-z])", "$1 $2")
      .toLowerCase(Locale.ROOT)
}
