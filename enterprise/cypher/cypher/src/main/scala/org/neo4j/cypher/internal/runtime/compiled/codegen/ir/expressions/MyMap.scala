/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.opencypher.v9_0.util.symbols._

//Named MyMap to avoid conflict with collection.Map which makes everything weird
case class MyMap(instructions: Map[String, CodeGenExpression]) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    instructions.values.foreach { instruction =>
      instruction.init(generator)
    }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    structure.asMap(instructions.mapValues(e => e.codeGenType match {
      case CypherCodeGenType(ListType(_), ListReferenceType(innerRepr)) if RepresentationType.isPrimitive(innerRepr) =>
        structure.toMaterializedAnyValue(structure.iteratorFrom(e.generateExpression(structure)), e.codeGenType)
      case _ =>
        structure.toMaterializedAnyValue(e.generateExpression(structure), e.codeGenType)
    }))

  override def nullable(implicit context: CodeGenContext) = false

  override def codeGenType(implicit context: CodeGenContext) = CypherCodeGenType(CTMap, AnyValueType)
}
