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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny

import scala.collection.mutable.ListBuffer

/**
 * Validates that the parser has used relevant symbolic names in all the label expressions, based on their context.
 * For example, a node pattern should only contain label names and no relationship type names.
 */
object ValidSymbolicNamesInLabelExpressions extends ValidatingCondition {
  override def name: String = "Valid symbolic names in label expressions"

  override def apply(ast: Any)(cancellationChecker: CancellationChecker): Seq[String] =
    ast.folder(cancellationChecker).fold(ListBuffer.empty[String]) {
      case NodePattern(_, Some(labelExpression), _, _) =>
        restrictSymbolicNames("node pattern", labelExpression, cancellationChecker, _.isInstanceOf[LabelName])

      case RelationshipPattern(_, Some(labelExpression), _, _, _, _) =>
        restrictSymbolicNames("relationship pattern", labelExpression, cancellationChecker, _.isInstanceOf[RelTypeName])

      case LabelExpressionPredicate(_, labelExpression) =>
        restrictSymbolicNames(
          "label expression predicate",
          labelExpression,
          cancellationChecker,
          _.isInstanceOf[LabelOrRelTypeName]
        )
    }.toSeq

  private def restrictSymbolicNames(
    context: String,
    labelExpression: LabelExpression,
    cancellationChecker: CancellationChecker,
    symbolicNamePredicate: SymbolicName => Boolean
  )(
    errors: ListBuffer[String]
  ): ListBuffer[String] =
    errors.addAll(
      labelExpression.folder(cancellationChecker).treeFindByClass[SymbolicName].filterNot(symbolicNamePredicate).map(
        symbolicName =>
          s"Illegal symbolic name $symbolicName inside a $context at position: ${symbolicName.position}"
      )
    )
}
