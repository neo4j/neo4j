/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.util.v3_4.symbols._

case class TypeOf(relId: Variable)
  extends CodeGenExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    val typeName = context.namer.newVarName()
    structure.declareAndInitialize(typeName, CypherCodeGenType(CTString, ReferenceType))
    if (nullable) {
      structure.ifNotStatement(structure.isNull(relId.name, CodeGenType.primitiveRel)) { body =>
        body.relType(relId.name, typeName)
      }
      structure.loadVariable(typeName)
    }
    else {
      structure.relType(relId.name, typeName)
      structure.loadVariable(typeName)
    }
  }

  override def nullable(implicit context: CodeGenContext) = relId.nullable

  override def codeGenType(implicit context: CodeGenContext) = CypherCodeGenType(CTString, ReferenceType)
}
