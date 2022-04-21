/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.util.hashing.MurmurHash3

sealed trait Parameter extends Expression {
  def name: String
  def parameterType: CypherType
  override def asCanonicalStringVal: String = "$" + name

  // NOTE: hashCode and equals must be same for different parameter types, since we
  // auto parameterize for efficient cache utilization
  override def hashCode(): Int = MurmurHash3.arrayHash(Array(name, parameterType))

  override def equals(obj: Any): Boolean = obj match {
    case that: Parameter => that.canEqual(this) && this.name == that.name && this.parameterType == that.parameterType
    case _               => false
  }
}

object Parameter {
  def unapply(p: Parameter): Option[(String, CypherType)] = Some((p.name, p.parameterType))

  def apply(name: String, parameterType: CypherType)(position: InputPosition): Parameter =
    ExplicitParameter(name, parameterType)(position)
}

case class ExplicitParameter(name: String, parameterType: CypherType)(val position: InputPosition) extends Parameter {
  override def hashCode(): Int = super.hashCode()
  override def canEqual(that: Any): Boolean = that.isInstanceOf[ExplicitParameter]
  override def equals(obj: Any): Boolean = super.equals(obj)
}

case class ListOfLiteralWriter(literals: Seq[Literal]) extends LiteralWriter {

  override def writeTo(literalExtractor: LiteralExtractor): Unit = {
    literalExtractor.beginList(literals.size)
    literals.foreach(_.writeTo(literalExtractor))
    literalExtractor.endList()
  }
}

case class AutoExtractedParameter(
  name: String,
  parameterType: CypherType,
  writer: LiteralWriter,
  sizeHint: Option[Int] = None
)(val position: InputPosition) extends Parameter {
  override def hashCode(): Int = MurmurHash3.arrayHash(Array(name, parameterType, sizeHint))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[AutoExtractedParameter]

  override def equals(obj: Any): Boolean = obj match {
    case that: AutoExtractedParameter => that.canEqual(this) && this.sizeHint == that.sizeHint && super.equals(that)
    case _                            => false
  }

  def writeTo(literalExtractor: LiteralExtractor): Unit = writer.writeTo(literalExtractor)
}

trait SensitiveParameter {
  val name: String
  val position: InputPosition
}

trait SensitiveAutoParameter extends SensitiveParameter
