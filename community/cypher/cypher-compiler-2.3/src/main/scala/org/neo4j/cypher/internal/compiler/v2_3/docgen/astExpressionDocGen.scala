/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.Pretty

import scala.reflect.runtime.universe.TypeTag

case object astExpressionDocGen  extends CustomDocGen[ASTNode] {

  import Pretty._

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
      case identifier: Identifier => Pretty(identifier.unquote)
      case literal: Literal => Pretty(literal.unquote)
      case hasLabels: HasLabels => Pretty(hasLabels.unquote)
      case property: Property => Pretty(property.unquote)
      case param: Parameter => Pretty(param.unquote)
      case binOp: BinaryOperatorExpression => Pretty(binOp.unquote)
      case leftOp: LeftUnaryOperatorExpression => Pretty(leftOp.unquote)
      case rightOp: RightUnaryOperatorExpression => Pretty(rightOp.unquote)
      case multiOp: MultiOperatorExpression => Pretty(multiOp.unquote)
      case fun: FunctionInvocation => Pretty(fun.unquote)
      case coll: Collection => Pretty(coll.unquote)
      case countStar: CountStar => Pretty(countStar.unquote)
      case _ => None
    }

  implicit class IdentifierConverter(identifier: Identifier) extends Converter {
    def unquote = AstNameConverter(identifier.name).unquote
  }

  implicit class LiteralConverter(literal: Literal) extends Converter {
    def unquote = text(literal.asCanonicalStringVal)
  }

  implicit class HasLabelsConverter(hasLabels: HasLabels) extends Converter {
    def unquote =
      group(pretty(hasLabels.expression) :: breakList(hasLabels.labels.map(pretty[LabelName]), break = silentBreak))
  }

  implicit class PropertyConverter(property: Property) extends Converter {
    def unquote = group(pretty(property.map) :: "." :: pretty(property.propertyKey))
  }

  implicit class ParameterConverter(param: Parameter) extends Converter {
    def unquote = braces(param.name)
  }

  implicit class BinOpConverter(binOp: BinaryOperatorExpression) extends Converter {
    def unquote = group(pretty(binOp.lhs) :/: binOp.canonicalOperatorSymbol :/: pretty(binOp.rhs))
  }

  implicit class LeftOpConverter(leftOp: LeftUnaryOperatorExpression) extends Converter {
    def unquote = group(leftOp.canonicalOperatorSymbol :/: pretty(leftOp.rhs))
  }

  implicit class RightOpConverter(rightOp: RightUnaryOperatorExpression) extends Converter {
    def unquote = group(pretty(rightOp.lhs) :/: rightOp.canonicalOperatorSymbol)
  }

  implicit class MultiOpConverter(multiOp: MultiOperatorExpression) extends Converter {
    def unquote =
      group(groupedSepList(multiOp.exprs.map(pretty[Expression]), sep = break :: multiOp.canonicalOperatorSymbol))
  }

  implicit class FunctionInvocationConverter(fun: FunctionInvocation) extends Converter {
    def unquote = {
      val callDoc = block(pretty(fun.functionName))(sepList(fun.args.map(pretty[Expression])))
      if (fun.distinct) group("DISTINCT" :/: callDoc) else callDoc
    }
  }

  implicit class CollectionConverter(coll: Collection) extends Converter {
    def unquote =
      brackets(sepList(coll.expressions.map(pretty[Expression])))
  }

  implicit class CountStarConverter(countStar: CountStar) extends Converter {
    def unquote = "count(*)"
  }
}
