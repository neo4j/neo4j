/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
}

case class JavaCodeGenType(repr: RepresentationType) extends CodeGenType {
  override def isPrimitive = RepresentationType.isPrimitive(repr)

  override def canBeNullable: Boolean = false
}

case class CypherCodeGenType(ct: CypherType, repr: RepresentationType) extends CodeGenType {
  def isPrimitive = RepresentationType.isPrimitive(repr)

  def canBeNullable = !isPrimitive || (ct == symbols.CTNode) || (ct == symbols.CTRelationship)
}
