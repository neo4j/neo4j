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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v2_3.perty.PageDocFormatting
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3._

trait ASTNode
  extends Product
  with Foldable
  with Rewritable
  with PageDocFormatting /* multi line */
  // with LineDocFormatting  /* single line */
//  with ToPrettyString[ASTNode]
{

  self =>

//  def toDefaultPrettyString(formatter: DocFormatter): String =
////    toPrettyString(formatter)(DefaultDocHandler.docGen) /* scala like */
//    toPrettyString(formatter)(InternalDocHandler.docGen) /* see there for more choices */

  def position: InputPosition

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.children)
      this
    else {
      val constructor = this.copyConstructor
      val params = constructor.getParameterTypes
      val args = children.toVector
      val hasExtraParam = params.length == args.length + 1
      val lastParamIsPos = params.last.isAssignableFrom(classOf[InputPosition])
      val ctorArgs = if (hasExtraParam && lastParamIsPos) args :+ this.position else args
      val duped = constructor.invoke(this, ctorArgs: _*)
      duped.asInstanceOf[self.type]
    }
}

// This is used by pretty printing to distinguish between
//
// - expressions
// - particles (non-expression ast nodes contained in expressions)
// - terms (neither expressions nor particles, like Clause)
//
sealed trait ASTNodeType { self: ASTNode => }

trait ASTExpression extends ASTNodeType { self: ASTNode => }
trait ASTParticle extends ASTNodeType { self: ASTNode => }
trait ASTPhrase extends ASTNodeType { self: ASTNode => }

// Skip/Limit
trait ASTSlicingPhrase extends ASTPhrase with SemanticCheckable {
  self: ASTNode =>
  def name: String
  def dependencies = expression.dependencies
  def expression: Expression

  def semanticCheck =
    containsNoIdentifiers chain
      literalShouldBeUnsignedInteger chain
      expression.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.expectType(CTInteger.covariant)

  private def containsNoIdentifiers: SemanticCheck = {
    val deps = dependencies
    if (deps.nonEmpty) {
      val id = deps.toSeq.sortBy(_.position).head
      SemanticError(s"It is not allowed to refer to identifiers in $name", id.position)
    }
    else SemanticCheckResult.success
  }

  private def literalShouldBeUnsignedInteger: SemanticCheck = {
    expression match {
      case lit: Literal => lit match {
        case _: UnsignedDecimalIntegerLiteral => SemanticCheckResult.success
        case i: SignedDecimalIntegerLiteral if i.value >= 0 => SemanticCheckResult.success
        case l => SemanticError(s"Invalid input '${l.asCanonicalStringVal}' is not a valid value, must be a positive integer", l.position)
      }
      case _ => SemanticCheckResult.success
    }
  }
}
