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
