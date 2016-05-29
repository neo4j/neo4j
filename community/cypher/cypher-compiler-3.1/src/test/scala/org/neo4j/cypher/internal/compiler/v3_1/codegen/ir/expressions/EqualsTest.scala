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
package org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_1.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.frontend.v3_1.symbols
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class EqualsTest extends CypherFunSuite {

  val typeMap = Map(
    CodeGenType.primitiveBool -> false,
    CodeGenType.primitiveRel -> false,
    CodeGenType.primitiveNode -> false,
    CodeGenType.primitiveInt -> false,
    CodeGenType.primitiveFloat -> false,
    CodeGenType(symbols.CTNode, ReferenceType) -> false,
    CodeGenType(symbols.CTRelationship, ReferenceType) -> false,
    CodeGenType(symbols.CTInteger, ReferenceType) -> true,
    CodeGenType(symbols.CTFloat, ReferenceType) -> true,
    CodeGenType(symbols.CTList(symbols.CTInteger), ReferenceType) -> true,
    CodeGenType(symbols.CTPath, ReferenceType) -> true,
    CodeGenType(symbols.CTPoint, ReferenceType) -> true
  )

  private implicit val context = mock[CodeGenContext]
  private def mockExpression(t: CodeGenType) = new CodeGenExpression {

    override def generateExpression[E](structure: MethodStructure[E])
                                      (implicit
                                       context: CodeGenContext): E = ???

    override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = ???

    override def nullable(implicit context: CodeGenContext): Boolean = false

    override def codeGenType(implicit context: CodeGenContext): CodeGenType = t
  }

  typeMap.foreach {
    case (codeGenType, expectedNullable) => test(s"$codeGenType should have nullable = $expectedNullable") {
      val element: CodeGenExpression = mockExpression(codeGenType)
      Equals(element, element).nullable should equal(expectedNullable)
    }
  }
}
