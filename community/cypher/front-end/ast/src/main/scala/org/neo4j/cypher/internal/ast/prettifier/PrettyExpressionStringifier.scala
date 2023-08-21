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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter.lift
import org.neo4j.cypher.internal.util.helpers.LineBreakRemover.removeLineBreaks
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.eraseGeneratedNamesOnTree
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParams
import org.neo4j.cypher.internal.util.topDown

/**
 * Generates pretty strings from expressions.
 */
private class PrettyExpressionStringifier(inner: ExpressionStringifier) extends ExpressionStringifier {

  private val simplify = topDown {
    lift {
      case string: String => removeLineBreaks(removeGeneratedNamesAndParams(string))
      case pattern: PatternExpression =>
        eraseGeneratedNamesOnTree(pattern) // In patterns it's safe to erase auto generated names
    }
  }

  private val simplifyPattern = topDown {
    lift {
      case s: String => removeLineBreaks(removeGeneratedNamesAndParams(eraseGeneratedNamesOnTree(s)))
    }
  }

  override def apply(expression: Expression): String = inner.apply(expression.endoRewrite(simplify))

  override def apply(name: SymbolicName): String = inner.apply(name.endoRewrite(simplify))

  override def apply(namespace: Namespace): String = inner.apply(namespace.endoRewrite(simplify))

  override def patterns: PatternStringifier = new PatternStringifier {
    private val innerPatterns = inner.patterns

    override def apply(p: Pattern): String = innerPatterns.apply(p.endoRewrite(simplifyPattern))

    override def apply(p: PatternPart): String = innerPatterns.apply(p.endoRewrite(simplifyPattern))

    override def apply(element: PatternElement): String = innerPatterns.apply(element.endoRewrite(simplifyPattern))

    override def apply(nodePattern: NodePattern): String = innerPatterns.apply(nodePattern.endoRewrite(simplifyPattern))

    override def apply(relationshipChain: RelationshipChain): String =
      innerPatterns.apply(relationshipChain.endoRewrite(simplifyPattern))

    override def apply(relationship: RelationshipPattern): String =
      innerPatterns.apply(relationship.endoRewrite(simplifyPattern))

    override def apply(concatenation: PathConcatenation): String =
      innerPatterns.apply(concatenation.endoRewrite(simplifyPattern))

    override def apply(quantified: QuantifiedPath): String =
      innerPatterns.apply(quantified.endoRewrite(simplifyPattern))

    override def apply(path: ParenthesizedPath): String =
      innerPatterns.apply(path.endoRewrite(simplifyPattern))
  }

  override def pathSteps: PathStepStringifier = new PathStepStringifier {
    override def apply(pathStep: PathStep): String = inner.pathSteps.apply(pathStep.endoRewrite(simplifyPattern))
  }

  override def backtick(in: String): String = inner.backtick(in)

  override def quote(txt: String): String = inner.quote(txt)

  override def escapePassword(password: Expression): String = inner.escapePassword(password)

  override def stringifyLabelExpression(le: LabelExpression): String = inner.stringifyLabelExpression(le)
}
