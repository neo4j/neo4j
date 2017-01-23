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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_2.codegen.CodeGenContext
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiler.v3_2.helpers.LiteralTypeSupport
import org.neo4j.cypher.internal.frontend.v3_2.symbols
import org.neo4j.cypher.internal.frontend.v3_2.symbols.ListType

case class ListLiteral(expressions: Seq[CodeGenExpression]) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    expressions.foreach { instruction =>
      instruction.init(generator)
    }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    val cType = codeGenType
    cType match {
      case CodeGenType(ListType(_), ListReferenceType(innerRepr)) if RepresentationType.isPrimitive(innerRepr) =>
        structure.asPrimitiveStream(expressions.map(e => {
          e.generateExpression(structure)
        }), cType)

      case _ =>
        structure.asList(expressions.map(e => structure.box(e.generateExpression(structure), e.codeGenType)))
    }
  }

  override def nullable(implicit context: CodeGenContext) = false

  override def codeGenType(implicit context: CodeGenContext) = {
    val commonType =
      if (expressions.nonEmpty)
        expressions.map(_.codeGenType.ct).reduce[symbols.CypherType](_ leastUpperBound _)
      else
        symbols.CTAny

    val elementType = LiteralTypeSupport.deriveCodeGenType(commonType)
    CodeGenType(symbols.CTList(commonType), ListReferenceType(elementType.repr))
  }
}
