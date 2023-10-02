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

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.values.AnyValue

/** Test expression to verify null checks in compiled expressions. */
case class NullCheckAssert() extends Expression {
  override def isConstantForQuery: Boolean = false
  override def position: InputPosition = InputPosition.NONE
}

object NullCheckAssert {
  val isNull = true

  def fail(): AnyValue = {
    throw new RuntimeException("Null check failed, not supposed to come here")
  }
}
