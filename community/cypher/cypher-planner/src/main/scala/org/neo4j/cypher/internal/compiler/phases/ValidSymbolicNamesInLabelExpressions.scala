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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.Foldable.FoldableAny

import scala.collection.mutable.ListBuffer

/**
 * Validates that the parser has used relevant symbolic names in all the label expressions, based on their context.
 * For example, a node pattern should only contain label names and no relationship type names.
 */
object ValidSymbolicNamesInLabelExpressions extends ValidatingCondition {
  override def name: String = "Valid symbolic names in label expressions"

  override def apply(ast: Any): Seq[String] =
    ast.folder.fold(ListBuffer.empty[String]) {
      case NodePattern(_, Some(labelExpression), _, _) =>
        restrictSymbolicNames("node pattern", labelExpression, _.isInstanceOf[LabelName])

      case RelationshipPattern(_, Some(labelExpression), _, _, _, _) =>
        restrictSymbolicNames("relationship pattern", labelExpression, _.isInstanceOf[RelTypeName])

      case LabelExpressionPredicate(_, labelExpression) =>
        restrictSymbolicNames("label expression predicate", labelExpression, _.isInstanceOf[LabelOrRelTypeName])
    }.toSeq

  private def restrictSymbolicNames(
    context: String,
    labelExpression: LabelExpression,
    symbolicNamePredicate: SymbolicName => Boolean
  )(
    errors: ListBuffer[String]
  ): ListBuffer[String] =
    errors.addAll(
      labelExpression.folder.treeFindByClass[SymbolicName].filterNot(symbolicNamePredicate).map(symbolicName =>
        s"Illegal symbolic name $symbolicName inside a $context at position: ${symbolicName.position}"
      )
    )
}
