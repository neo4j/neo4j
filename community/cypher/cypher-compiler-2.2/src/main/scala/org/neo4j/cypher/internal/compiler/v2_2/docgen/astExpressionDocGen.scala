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
import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._
import org.neo4j.cypher.internal.compiler.v2_2.perty._

import scala.util.Try

case object astExpressionDocGen extends CustomDocGen[ASTNode] {
  def newDocDrill = mkDocDrill[ASTNode]() {
    case expr: Expression =>
      inner =>
        astExpressionConverter(inner)(expr)
  }
}

case class astExpressionConverter(pretty: DocConverter[Any]) extends (Expression => Option[Doc]) {

  def apply(expr: Expression) = Try[Doc] {
    expr match {
      case identifier: Identifier => identifier.asDoc
      case literal: Literal => literal.asDoc
      case hasLabels: HasLabels => hasLabels.asDoc
      case property: Property => property.asDoc
      case param: Parameter => param.asDoc
      case binOp: BinaryOperatorExpression => binOp.asDoc
      case leftOp: LeftUnaryOperatorExpression => leftOp.asDoc
      case rightOp: RightUnaryOperatorExpression => rightOp.asDoc
      case multiOp: MultiOperatorExpression => multiOp.asDoc
      case fun: FunctionInvocation => fun.asDoc
      case coll: Collection => coll.asDoc
      case countStar: CountStar => countStar.asDoc
    }
  }.toOption

  implicit def expressionAsDoc(expression: ASTNode with ASTExpression): Doc = pretty(expression)
  implicit def particleAsDoc(particle: ASTNode with ASTParticle): Doc = pretty(particle)

  implicit class IdentifierConverter(identifier: Identifier) {
    def asDoc = identifier.name
  }

  implicit class LiteralConverter(literal: Literal) {
    def asDoc = literal.asCanonicalStringVal
  }

  implicit class HasLabelsConverter(hasLabels: HasLabels) {
    def asDoc = group(hasLabels.expression :: breakList(hasLabels.labels.map(pretty), break = breakSilent))
  }

  implicit class PropertyConverter(property: Property) {
    def asDoc = group(pretty(property.map) :: "." :: pretty(property.propertyKey))
  }

  implicit class ParameterConverter(param: Parameter) {
    def asDoc = braces(param.name)
  }

  implicit class BinOpConverter(binOp: BinaryOperatorExpression) {
    def asDoc = group(binOp.lhs :/: binOp.canonicalOperatorSymbol :/: binOp.rhs)
  }

  implicit class LeftOpConverter(leftOp: LeftUnaryOperatorExpression) {
    def asDoc = group(leftOp.canonicalOperatorSymbol :/: leftOp.rhs)
  }

  implicit class RightOpConverter(rightOp: RightUnaryOperatorExpression) {
    def asDoc = group(rightOp.lhs :/: rightOp.canonicalOperatorSymbol)
  }

  implicit class MultiOpConverter(multiOp: MultiOperatorExpression) {
    def asDoc = group(groupedSepList(multiOp.exprs.map(pretty), sep = break :: multiOp.canonicalOperatorSymbol))
  }

  implicit class FunctionInvocationConverter(fun: FunctionInvocation) {
    def asDoc = {
      val callDoc = block(fun.functionName)(sepList(fun.args.map(pretty)))
      if (fun.distinct) group("DISTINCT" :/: callDoc) else callDoc
    }
  }

  implicit class CollectionConverter(coll: Collection) {
    def asDoc = brackets(sepList(coll.expressions.map(pretty)))
  }

  implicit class CountStarConverter(countStar: CountStar) {
    def asDoc: Doc = "count(*)"
  }
}
