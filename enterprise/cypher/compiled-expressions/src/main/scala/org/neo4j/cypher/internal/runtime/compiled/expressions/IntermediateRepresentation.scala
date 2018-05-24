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
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.MethodReference
import org.neo4j.values.storable.{FloatingPointValue, IntegralValue, TextValue}

import scala.reflect.ClassTag

sealed trait IntermediateRepresentation
case class InvokeStatic(method: Method, params: Seq[IntermediateRepresentation] ) extends IntermediateRepresentation
case class Invoke(target: IntermediateRepresentation, method: Method, params: Seq[IntermediateRepresentation] ) extends IntermediateRepresentation
case class Load(variable: String) extends IntermediateRepresentation
case class Integer(value: IntegralValue) extends IntermediateRepresentation
case class Float(value: FloatingPointValue) extends IntermediateRepresentation
case class StringLiteral(value: TextValue) extends IntermediateRepresentation
case class Constant(value: Any) extends IntermediateRepresentation
case object NULL extends IntermediateRepresentation
case object TRUE extends IntermediateRepresentation
case object FALSE extends IntermediateRepresentation

case class Method(owner: Class[_], output: Class[_], name: String, params: Class[_]* ) {
  def asReference: MethodReference = MethodReference.methodReference(owner, output, name, params:_*)
}

object IntermediateRepresentation {

  def method[OWNER, OUT](name: String)(implicit owner: ClassTag[OWNER], out: ClassTag[OUT]) =
    Method(owner.runtimeClass, out.runtimeClass, name)

  def method[OWNER, OUT, IN](name: String)(implicit owner: ClassTag[OWNER], out: ClassTag[OUT], in: ClassTag[IN]) =
    Method(owner.runtimeClass, out.runtimeClass, name, in.runtimeClass)

  def method[OWNER, OUT, IN1, IN2](name: String)
                                  (implicit owner: ClassTag[OWNER], out: ClassTag[OUT], in1: ClassTag[IN1],
                                   in2: ClassTag[IN2]) =
    Method(owner.runtimeClass, out.runtimeClass, name, in1.runtimeClass, in2.runtimeClass)
  def method[OWNER, OUT, IN1, IN2, IN3](name: String)
                                  (implicit owner: ClassTag[OWNER], out: ClassTag[OUT], in1: ClassTag[IN1],
                                   in2: ClassTag[IN2], in3: ClassTag[IN3]) =
    Method(owner.runtimeClass, out.runtimeClass, name, in1.runtimeClass, in2.runtimeClass, in3.runtimeClass)

  def invokeStatic(method: Method, params: IntermediateRepresentation*): IntermediateRepresentation = InvokeStatic(method, params)
  def invoke(owner: IntermediateRepresentation, method: Method, params: IntermediateRepresentation*): IntermediateRepresentation =
    Invoke(owner, method, params)
  def load(variable: String): IntermediateRepresentation = Load(variable)
  def integer(value: IntegralValue): IntermediateRepresentation = Integer(value)
  def float(value: FloatingPointValue): IntermediateRepresentation = Float(value)
  def string(value: TextValue): IntermediateRepresentation = StringLiteral(value)
  def noValue: IntermediateRepresentation = NULL
  def truthy: IntermediateRepresentation = TRUE
  def falsy: IntermediateRepresentation = FALSE
  def constantJavaValue(value: Any): IntermediateRepresentation = Constant(value)
}