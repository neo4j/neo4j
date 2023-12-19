/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
