/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.rewriting.rewriters

import org.neo4j.cypher.internal.v3_5.ast
import org.neo4j.cypher.internal.v3_5.expressions.{Variable, _}
import org.neo4j.cypher.internal.v3_5.util.Foldable._

object inliningContextCreator extends (ast.Statement => InliningContext) {

  def apply(input: ast.Statement): InliningContext = {
    input.treeFold(InliningContext()) {
      // We cannot inline expressions in a DISTINCT with clause, projecting the result of the expression
      // would change the result of the distinctification
      case withClause: ast.With if !withClause.distinct =>
        context =>
          (context.enterQueryPart(aliasedReturnItems(withClause.returnItems.items)), Some(identity))

      // When just passing a variable through a WITH, do not count the variable as used. This case shortcuts the
      // tree folding so the variables are not tracked.
      case ast.AliasedReturnItem(Variable(n1), alias@Variable(n2)) if n1 == n2 =>
        context => (context, None)

      case variable: Variable =>
        context =>
          (context.trackUsageOfVariable(variable), Some(identity))

      // When a variable is used in ORDER BY, it should never be inlined
      case sortItem: ast.SortItem =>
        context =>
          (context.spoilVariable(sortItem.expression.asInstanceOf[Variable]), Some(identity))

      // Do not inline pattern variables, unless they are clean aliases of previous variables
      case NodePattern(Some(variable), _, _, _) =>
        context =>
          (spoilVariableIfNotAliased(variable, context), Some(identity))

      case RelationshipPattern(Some(variable), _, _, _, _, _, _) =>
        context =>
          (spoilVariableIfNotAliased(variable, context), Some(identity))
    }
  }

  private def spoilVariableIfNotAliased(variable: LogicalVariable, context: InliningContext): InliningContext =
    if (context.isAliasedVarible(variable)) context
    else context.spoilVariable(variable)

  private def aliasedReturnItems(items: Seq[ast.ReturnItem]): Map[LogicalVariable, Expression] =
    items.collect { case ast.AliasedReturnItem(expr, ident) => ident -> expr }.toMap
}
