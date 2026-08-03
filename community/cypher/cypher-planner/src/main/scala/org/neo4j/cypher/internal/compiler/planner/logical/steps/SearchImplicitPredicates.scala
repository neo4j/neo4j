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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabel
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken

/**
 * Determines which query graph predicates are implicitly solved by a SEARCH index leaf plan.
 *
 * Shared by [[VectorSearchLeafPlanner]] and [[FulltextSearchLeafPlanner]], which differ only in which
 * IS NOT NULL predicates a match from their index type implies. Each planner injects that rule as
 * `solvesIsNotNull`, typically built from [[SearchImplicitPredicates.solvesIsNotNull]].
 */
object SearchImplicitPredicates {

  def solvedNodePredicates(
    searchVariable: LogicalVariable,
    solvesIsNotNull: Expression => Boolean,
    labelTokens: Seq[LabelToken],
    predicates: Set[Expression]
  ): Set[Expression] = {
    val isSolved: Expression => Boolean =
      labelTokens match {
        case Seq(labelToken) =>
          expression =>
            solvesIsNotNull(expression) ||
              solvesHasLabel(searchVariable, labelToken, expression)
        case _ =>
          // If the index is defined for more than one label, the logic needs to be more complicated.
          // For example, if it is defined for (n:Foo|Bar), it implicitly solves x:Foo|Bar or any superset like
          // x:Foo|Bar|Zor but not x:Foo or x:Bar|Zor.
          // We decided not to handle it for now.
          solvesIsNotNull
      }

    predicates.filter(expandToOr(isSolved))
  }

  def solvedRelationshipPredicates(
    searchVariable: LogicalVariable,
    solvesIsNotNull: Expression => Boolean,
    relationshipTokens: Seq[RelationshipTypeToken],
    predicates: Set[Expression]
  ): Set[Expression] = {
    val isSolved: Expression => Boolean =
      relationshipTokens match {
        case Seq(typeToken) =>
          expression =>
            solvesIsNotNull(expression) ||
              solvesHasTypes(searchVariable, typeToken, expression)
        case _ =>
          // If the index is defined for more than one relationship type, the logic needs to be more complicated.
          // For example, if it is defined for ()-[r:R|S]-(), it implicitly solves x:R|S or any superset like
          // x:R|S|T but not x:R or x:S|T.
          // We decided not to handle it for now.
          solvesIsNotNull
      }

    predicates.filter(expandToOr(isSolved))
  }

  def solvesIsNotNull(
    searchVariable: LogicalVariable,
    propertyKeyToken: PropertyKeyToken,
    expression: Expression
  ): Boolean =
    expression match {
      case IsNotNull(Property(`searchVariable`, propertyKey)) if propertyKey.name == propertyKeyToken.name => true
      case _                                                                                               => false
    }

  private def expandToOr(predicate: Expression => Boolean): Expression => Boolean = {
    case Ors(nestedExpressions) => nestedExpressions.exists(predicate)
    case otherExpression        => predicate(otherExpression)
  }

  private def solvesHasLabel(
    searchVariable: LogicalVariable,
    labelToken: LabelToken,
    expression: Expression
  ): Boolean =
    expression match {
      case HasLabel(`searchVariable`, lbl) if lbl.name == labelToken.name => true
      case _                                                              => false
    }

  private def solvesHasTypes(
    searchVariable: LogicalVariable,
    typeToken: RelationshipTypeToken,
    expression: Expression
  ): Boolean =
    expression match {
      case HasTypes(`searchVariable`, relTypes)
        if relTypes.size == 1 && relTypes.exists(_.name == typeToken.name) => true
      case _ => false
    }
}
