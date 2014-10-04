/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.Pretty

import scala.reflect.runtime.universe.TypeTag

case object astExpressionDocGen  extends CustomDocGen[ASTNode] {

  import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.Pretty._

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
    case identifier: Identifier => identifier.asPretty
    case literal: Literal => literal.asPretty
    case hasLabels: HasLabels => hasLabels.asPretty
    case property: Property => property.asPretty
    case param: Parameter => param.asPretty
    case binOp: BinaryOperatorExpression => binOp.asPretty
    case leftOp: LeftUnaryOperatorExpression => leftOp.asPretty
    case rightOp: RightUnaryOperatorExpression => rightOp.asPretty
    case multiOp: MultiOperatorExpression => multiOp.asPretty
    case fun: FunctionInvocation => fun.asPretty
    case coll: Collection => coll.asPretty
    case countStar: CountStar => countStar.asPretty
    case _ => None
  }

  implicit class IdentifierConverter(identifier: Identifier) {
    def asPretty: Option[DocRecipe[Any]] = AstNameConverter(identifier.name).asPretty
  }

  implicit class LiteralConverter(literal: Literal) {
    def asPretty: Option[DocRecipe[Any]] = Pretty(literal.asCanonicalStringVal)
  }

  implicit class HasLabelsConverter(hasLabels: HasLabels) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(group(pretty(hasLabels.expression) :: breakList(hasLabels.labels.map(pretty[LabelName]), break = silentBreak)))
  }

  implicit class PropertyConverter(property: Property) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(group(pretty(property.map) :: "." :: pretty(property.propertyKey)))
  }

  implicit class ParameterConverter(param: Parameter) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(braces(param.name))
  }

  implicit class BinOpConverter(binOp: BinaryOperatorExpression) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(group(pretty(binOp.lhs) :/: binOp.canonicalOperatorSymbol :/: pretty(binOp.rhs)))
  }

  implicit class LeftOpConverter(leftOp: LeftUnaryOperatorExpression) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(group(leftOp.canonicalOperatorSymbol :/: pretty(leftOp.rhs)))
  }

  implicit class RightOpConverter(rightOp: RightUnaryOperatorExpression) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(group(pretty(rightOp.lhs) :/: rightOp.canonicalOperatorSymbol))
  }

  implicit class MultiOpConverter(multiOp: MultiOperatorExpression) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(group(groupedSepList(multiOp.exprs.map(pretty[Expression]), sep = break :: multiOp.canonicalOperatorSymbol)))
  }

  implicit class FunctionInvocationConverter(fun: FunctionInvocation) {
    def asPretty: Option[DocRecipe[Any]] ={
      val callDoc = block(pretty(fun.functionName))(sepList(fun.args.map(pretty[Expression])))
      Pretty(if (fun.distinct) group("DISTINCT" :/: callDoc) else callDoc)
    }
  }

  implicit class CollectionConverter(coll: Collection) {
    def asPretty: Option[DocRecipe[Any]] =
      Pretty(brackets(sepList(coll.expressions.map(pretty[Expression]))))
  }

  implicit class CountStarConverter(countStar: CountStar) {
    def asPretty: Option[DocRecipe[Any]] = Pretty("count(*)")
  }
}
