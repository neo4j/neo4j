/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.perty._

trait ASTNode
  extends Product
  with Foldable
  with Rewritable
  with PageDocFormatting /* multi line */
  // with LineDocFormatting  /* single line */
//  with ToPrettyString[ASTNode]
{

  self =>

  import org.neo4j.cypher.internal.compiler.v2_3.Foldable._
  import org.neo4j.cypher.internal.compiler.v2_3.Rewritable._

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
trait ASTSlicingPhrase extends ASTPhrase { self: ASTNode => }
