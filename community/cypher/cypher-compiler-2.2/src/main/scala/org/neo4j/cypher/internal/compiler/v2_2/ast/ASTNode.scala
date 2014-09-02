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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.docbuilders.internalDocBuilder
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.docbuilders.{defaultDocBuilder, simpleDocBuilder}

trait ASTNode
  extends Product
  with Foldable
  with Rewritable
  // disable pretty printing by using: with simpleDocBuilder.GeneratorToString[ASTNode] {
  with internalDocBuilder.GeneratorToString[ASTNode] {

  import org.neo4j.cypher.internal.compiler.v2_2.Foldable._
  import org.neo4j.cypher.internal.compiler.v2_2.Rewritable._

  def position: InputPosition

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.children)
      this
    else {
      val constructor = this.copyConstructor
      val params = constructor.getParameterTypes
      val args = children.toVector
      if ((params.length == args.length + 1) && params.last.isAssignableFrom(classOf[InputPosition]))
        constructor.invoke(this, args :+ this.position: _*).asInstanceOf[this.type]
      else
        constructor.invoke(this, args: _*).asInstanceOf[this.type]
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
trait ASTTerm extends ASTNodeType { self: ASTNode => }
