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

import org.neo4j.cypher.internal.util.v3_4.symbols._

trait CodeGenType {
  def isPrimitive: Boolean

  def isValue: Boolean

  def isAnyValue: Boolean

  def canBeNullable: Boolean

  def repr: RepresentationType
}

object CodeGenType {
  val Any = CypherCodeGenType(CTAny, ReferenceType)
  val AnyValue = CypherCodeGenType(CTAny, AnyValueType)
  val Value = CypherCodeGenType(CTAny, ValueType)
  val primitiveNode = CypherCodeGenType(CTNode, LongType)
  val primitiveRel = CypherCodeGenType(CTRelationship, LongType)
  val primitiveInt = CypherCodeGenType(CTInteger, LongType)
  val primitiveFloat = CypherCodeGenType(CTFloat, FloatType)
  val primitiveBool = CypherCodeGenType(CTBoolean, BoolType)
  val javaInt = JavaCodeGenType(IntType)
  val javaLong = JavaCodeGenType(LongType)
}

case class JavaCodeGenType(repr: RepresentationType) extends CodeGenType {
  override def isPrimitive = RepresentationType.isPrimitive(repr)

  def isValue = RepresentationType.isValue(repr)

  def isAnyValue = RepresentationType.isAnyValue(repr)

  override def canBeNullable: Boolean = false
}

case class CypherCodeGenType(ct: CypherType, repr: RepresentationType) extends CodeGenType {
  def isPrimitive = RepresentationType.isPrimitive(repr)

  def isValue = RepresentationType.isValue(repr)

  def isAnyValue = RepresentationType.isAnyValue(repr)

  def canBeNullable = !isPrimitive || (ct == CTNode) || (ct == CTRelationship)
}
