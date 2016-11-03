/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1._

trait ASTNode
  extends Product
  with Foldable
  with Rewritable {

  self =>

  def recordCurrentScope: SemanticCheck = s => SemanticCheckResult.success(s.recordCurrentScope(this))

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
    containsNoVariables chain
      literalShouldBeUnsignedInteger chain
      expression.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.expectType(CTInteger.covariant)

  private def containsNoVariables: SemanticCheck = {
    val deps = dependencies
    if (deps.nonEmpty) {
      val id = deps.toSeq.sortBy(_.position).head
      SemanticError(s"It is not allowed to refer to variables in $name", id.position)
    }
    else SemanticCheckResult.success
  }

  private def literalShouldBeUnsignedInteger: SemanticCheck = {
    expression match {
      case _: UnsignedDecimalIntegerLiteral => SemanticCheckResult.success
      case i: SignedDecimalIntegerLiteral if i.value >= 0 => SemanticCheckResult.success
      case lit: Literal => SemanticError(s"Invalid input '${lit.asCanonicalStringVal}' is not a valid value, must be a positive integer", lit.position)
      case _ => SemanticCheckResult.success
    }
  }
}
