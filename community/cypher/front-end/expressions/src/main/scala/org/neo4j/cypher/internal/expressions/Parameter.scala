/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import org.neo4j.cypher.internal.util.BucketSize
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.UnknownSize
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.annotation.nowarn
import scala.util.hashing.MurmurHash3

sealed trait Parameter extends Expression {
  def name: String
  def parameterType: CypherType
  def sizeHint: BucketSize
  override def asCanonicalStringVal: String = "$" + name

  // NOTE: hashCode and equals must be same for different parameter types, since we
  // auto parameterize for efficient cache utilization
  @nowarn("msg=a type was inferred to be `Object`")
  override def hashCode(): Int = MurmurHash3.arrayHash(Array(name, parameterType))

  override def equals(obj: Any): Boolean = obj match {
    case that: Parameter => that.canEqual(this) &&
      this.name == that.name &&
      this.parameterType == that.parameterType &&
      this.sizeHint == that.sizeHint
    case _ => false
  }

  override def isConstantForQuery: Boolean = true
}

object Parameter {
  def unapply(p: Parameter): Option[(String, CypherType, BucketSize)] = Some((p.name, p.parameterType, p.sizeHint))
}

case class ExplicitParameter(name: String, parameterType: CypherType, sizeHint: BucketSize = UnknownSize)(
  val position: InputPosition
) extends Parameter {
  override def hashCode(): Int = super.hashCode()
  override def canEqual(that: Any): Boolean = that.isInstanceOf[ExplicitParameter]
  override def equals(obj: Any): Boolean = super.equals(obj)
}

case class AutoExtractedParameter(
  name: String,
  parameterType: CypherType,
  sizeHint: BucketSize = UnknownSize
)(val position: InputPosition) extends Parameter {

  @nowarn("msg=a type was inferred to be `Object`")
  override def hashCode(): Int = MurmurHash3.arrayHash(Array(name, parameterType, sizeHint))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[AutoExtractedParameter]

  override def equals(obj: Any): Boolean = obj match {
    case that: AutoExtractedParameter => that.canEqual(this) && super.equals(that)
    case _                            => false
  }
}

trait SensitiveParameter {
  def name: String
  def position: InputPosition
}

trait SensitiveAutoParameter extends SensitiveParameter
