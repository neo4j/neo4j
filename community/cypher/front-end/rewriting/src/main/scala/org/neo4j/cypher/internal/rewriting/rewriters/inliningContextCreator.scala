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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren

object inliningContextCreator extends (ast.Statement => InliningContext) {

  def apply(input: ast.Statement): InliningContext = {
    input.folder.treeFold(InliningContext()) {
      // We cannot inline expressions in a DISTINCT with clause, projecting the result of the expression
      // would change the result of the distinctification
      case withClause: ast.With if !withClause.distinct =>
        context =>
          TraverseChildren(context.enterQueryPart(aliasedReturnItems(withClause.returnItems.items)))

        // When just passing a variable through a WITH, do not count the variable as used. This case shortcuts the
      // tree folding so the variables are not tracked.
      case ast.AliasedReturnItem(Variable(n1), alias @ Variable(n2)) if n1 == n2 =>
        context => SkipChildren(context)

      case variable: Variable =>
        context =>
          TraverseChildren(context.trackUsageOfVariable(variable))

      // When a variable is used in ORDER BY, it should never be inlined
      case sortItem: ast.SortItem =>
        context =>
          TraverseChildren(context.spoilVariable(sortItem.expression.asInstanceOf[Variable]))

      // Do not inline pattern variables, unless they are clean aliases of previous variables
      case NodePattern(Some(variable), _, _, _) =>
        context =>
          TraverseChildren(spoilVariableIfNotAliased(variable, context))

      case RelationshipPattern(Some(variable), _, _, _, _, _) =>
        context =>
          TraverseChildren(spoilVariableIfNotAliased(variable, context))
    }
  }

  private def spoilVariableIfNotAliased(variable: LogicalVariable, context: InliningContext): InliningContext =
    if (context.isAliasedVariable(variable)) context
    else context.spoilVariable(variable)

  private def aliasedReturnItems(items: Seq[ast.ReturnItem]): Map[LogicalVariable, Expression] =
    items.collect { case ast.AliasedReturnItem(expr, ident) => ident -> expr }.toMap
}
