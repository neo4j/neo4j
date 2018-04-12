/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.helpers.LiteralTypeSupport

case class Literal(value: Object) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    // When the literal value comes from the AST it should already have been converted
    assert({
      val needsConverison = value match {
        case n: java.lang.Byte => true // n.longValue()
        case n: java.lang.Short => true // n.longValue()
        case n: java.lang.Character => true // n.toString
        case n: java.lang.Integer => true // n.longValue()
        case n: java.lang.Float => true // n.doubleValue()
        case _ => false
      }
      !needsConverison
    })
    val ct = codeGenType
    if (value == null)
      structure.noValue()
    else if (ct.isPrimitive)
      structure.constantExpression(value)
    else
      structure.constantValueExpression(value, ct)
  }

  override def nullable(implicit context: CodeGenContext) = value == null

  override def codeGenType(implicit context: CodeGenContext) = LiteralTypeSupport.deriveCodeGenType(value)
}
