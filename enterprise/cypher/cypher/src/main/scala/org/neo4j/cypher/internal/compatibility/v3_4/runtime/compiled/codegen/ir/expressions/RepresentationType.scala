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

/**
  * Type representation of a CodeGenExpression - these are the JVM types that will be used!
  */
sealed trait RepresentationType

case object IntType extends RepresentationType // Primitive int

case object LongType extends RepresentationType // Primitive long

case object BoolType extends RepresentationType // Primitive boolean

case object FloatType extends RepresentationType // Primitive double

sealed trait ReferenceType extends RepresentationType // Boxed type (Object)

case object ReferenceType extends ReferenceType

sealed trait AnyValueType extends ReferenceType

case object AnyValueType extends AnyValueType

case object ValueType extends AnyValueType

case class ListReferenceType(inner: RepresentationType) extends RepresentationType

object RepresentationType {
  def isPrimitive(repr: RepresentationType): Boolean = repr match {
    case IntType | LongType | FloatType | BoolType => true
    case _ => false
  }

  def isValue(repr: RepresentationType): Boolean = repr match {
    case ValueType => true
    case _ => false
  }

  def isAnyValue(repr: RepresentationType): Boolean = repr match {
    case AnyValueType | ValueType => true
    case _ => false
  }
}
