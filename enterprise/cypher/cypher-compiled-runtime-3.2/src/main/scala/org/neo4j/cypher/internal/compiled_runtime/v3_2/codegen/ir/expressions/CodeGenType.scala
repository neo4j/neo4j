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
package org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.expressions

import org.neo4j.cypher.internal.frontend.v3_2.symbols
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CypherType

trait CodeGenType {
  def isPrimitive: Boolean

  def canBeNullable: Boolean

  def repr: RepresentationType
}

object CodeGenType {
  val Any = CypherCodeGenType(symbols.CTAny, ReferenceType)
  val primitiveNode = CypherCodeGenType(symbols.CTNode, LongType)
  val primitiveRel = CypherCodeGenType(symbols.CTRelationship, LongType)
  val primitiveInt = CypherCodeGenType(symbols.CTInteger, LongType)
  val primitiveFloat = CypherCodeGenType(symbols.CTFloat, FloatType)
  val primitiveBool = CypherCodeGenType(symbols.CTBoolean, BoolType)
  val javaInt = JavaCodeGenType(IntType)
  val javaLong = JavaCodeGenType(LongType)
}

case class JavaCodeGenType(repr: RepresentationType) extends CodeGenType {
  override def isPrimitive = RepresentationType.isPrimitive(repr)

  override def canBeNullable: Boolean = false
}

case class CypherCodeGenType(ct: CypherType, repr: RepresentationType) extends CodeGenType {
  def isPrimitive = RepresentationType.isPrimitive(repr)

  def canBeNullable = !isPrimitive || (ct == symbols.CTNode) || (ct == symbols.CTRelationship)
}
