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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheckResult, SemanticError, TypeGenerator}

case class ListLiteral(expressions: Seq[Expression])(val position: InputPosition) extends Expression {

  def semanticCheck(ctx: SemanticContext) = expressions.semanticCheck(ctx) chain specifyType(possibleTypes)

  def map(f: Expression => Expression) = copy(expressions = expressions.map(f))(position)

  private def possibleTypes: TypeGenerator = state => expressions match {
    case Seq() => CTList(CTAny).covariant
    case _     => expressions.leastUpperBoundsOfTypes(state).wrapInCovariantList
  }
  override def asCanonicalStringVal: String = expressions.map(_.asCanonicalStringVal).mkString("[", ", ", "]")
}

case class ListSlice(list: Expression, from: Option[Expression], to: Option[Expression])(val position: InputPosition)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    list.semanticCheck(ctx) chain
    list.expectType(CTList(CTAny).covariant) chain
    when(from.isEmpty && to.isEmpty) {
      SemanticError("The start or end (or both) is required for a collection slice", position)
    } chain
    from.semanticCheck(ctx) chain
    from.expectType(CTInteger.covariant) chain
    to.semanticCheck(ctx) chain
    to.expectType(CTInteger.covariant) chain
    specifyType(list.types)
}

case class ContainerIndex(expr: Expression, idx: Expression)(val position: InputPosition)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    expr.semanticCheck(ctx) chain
    idx.semanticCheck(ctx) chain
    expr.typeSwitch {
      case exprT =>
        idx.typeSwitch {
          case idxT =>
            val listT = CTList(CTAny).covariant & exprT
            val mapT = CTMap.covariant & exprT
            val exprIsList = listT != TypeSpec.none
            val exprIsMap = mapT != TypeSpec.none
            val idxIsInteger = (CTInteger.covariant & idxT) != TypeSpec.none
            val idxIsString = (CTString.covariant & idxT) != TypeSpec.none
            val listLookup = exprIsList || idxIsInteger
            val mapLookup = exprIsMap || idxIsString

            if (listLookup && !mapLookup) {
                expr.expectType(CTList(CTAny).covariant) chain
                idx.expectType(CTInteger.covariant) chain
                specifyType(expr.types(_).unwrapLists)
            }
            else if (!listLookup && mapLookup) {
                expr.expectType(CTMap.covariant) chain
                idx.expectType(CTString.covariant)
            } else {
                SemanticCheckResult.success
            }
        }
    }
}
