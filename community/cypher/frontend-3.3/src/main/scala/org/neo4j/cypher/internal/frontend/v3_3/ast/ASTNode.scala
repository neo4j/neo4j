/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._

trait ASTNode
  extends Product
  with Foldable
  with Rewritable {

  self =>

  def recordCurrentScope: SemanticCheck = s => SemanticCheckResult.success(s.recordCurrentScope(this))

  def recordCurrentGraphScope: SemanticCheck = s => SemanticCheckResult.success(s.recordCurrentGraphScope(this))

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

  def asCanonicalStringVal: String = toString

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
