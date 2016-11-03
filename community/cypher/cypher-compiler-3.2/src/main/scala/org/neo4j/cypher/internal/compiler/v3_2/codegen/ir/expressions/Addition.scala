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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.frontend.v3_2.symbols._

case class Addition(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression with BinaryOperator {

  override protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = structure.add

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override def name: String = "add"

  override def codeGenType(implicit context: CodeGenContext) = (lhs.codeGenType.ct, rhs.codeGenType.ct) match {

    // Collections
    case (ListType(left), ListType(right)) =>
      CodeGenType(ListType(left leastUpperBound right), ReferenceType)
    case (ListType(innerType), singleElement) =>
      CodeGenType(ListType(innerType leastUpperBound singleElement), ReferenceType)
    case (singleElement, ListType(innerType)) => CodeGenType(ListType(innerType leastUpperBound singleElement), ReferenceType)

    // Strings
    case (CTString, _) => CodeGenType(CTString, ReferenceType)
    case (_, CTString) => CodeGenType(CTString, ReferenceType)

    // Numbers
    case (CTInteger, CTInteger) => CodeGenType(CTInteger, ReferenceType)
    case (Number(_), Number(_)) => CodeGenType(CTFloat, ReferenceType)

    // Runtime we'll figure it out
    case _ => CodeGenType.Any
  }
}
