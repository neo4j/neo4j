/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.expressions

import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType

trait TypeSignature {

  def argumentTypes: IndexedSeq[CypherType]

  def outputType: CypherType

  def removeFirstArgumentType: TypeSignature
}

case class FunctionTypeSignature(functionName : String, outputType: CypherType, names: IndexedSeq[String], description: String, argumentTypes: IndexedSeq[CypherType], optionalTypes: IndexedSeq[CypherType] = Vector.empty, deprecated: Boolean = false) extends TypeSignature {
  def getSignatureAsString: String =
    functionName.toLowerCase + "(" + names.zip(argumentTypes.map(_.toNeoTypeString) ++ optionalTypes.map(_.toNeoTypeString)).map { f =>
      f._1 + " :: " + f._2
    }.mkString(", ") + ") :: (" + outputType.toNeoTypeString + ")"

  override def removeFirstArgumentType: TypeSignature = this.copy(argumentTypes = this.argumentTypes.tail)
}

object TypeSignature {
  def deprecated(functionName : String, argumentType: CypherType, outputType: CypherType, description: String) =
    FunctionTypeSignature(functionName, outputType, Vector("input"), description, Vector(argumentType), deprecated = true)

  def apply(functionName : String, argumentType: CypherType, outputType: CypherType, description: String): FunctionTypeSignature =
    FunctionTypeSignature(functionName, outputType, Vector("input"), description, Vector(argumentType))

  def noArg(functionName : String, outputType: CypherType, description: String): FunctionTypeSignature =
    FunctionTypeSignature(functionName, outputType, Vector("input"), description, Vector())

  def apply(argumentTypes: IndexedSeq[CypherType], outputType: CypherType) =
    ExpressionTypeSignature(argumentTypes, outputType)
}

case class ExpressionTypeSignature(argumentTypes: IndexedSeq[CypherType], outputType: CypherType) extends TypeSignature {
  override def removeFirstArgumentType: TypeSignature = this.copy(argumentTypes = this.argumentTypes.tail)
}
