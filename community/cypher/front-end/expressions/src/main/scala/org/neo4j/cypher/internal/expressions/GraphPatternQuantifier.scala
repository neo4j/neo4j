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

import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

sealed trait GraphPatternQuantifier extends ASTNode {
  def canBeEmpty: Boolean
}

case class PlusQuantifier()(val position: InputPosition) extends GraphPatternQuantifier {
  override def canBeEmpty: Boolean = false
}

case class StarQuantifier()(val position: InputPosition) extends GraphPatternQuantifier {
  override def canBeEmpty: Boolean = true
}

/**
 * Represents a quantifier like {1, 5} with optional lower and upper bound.
 */
case class IntervalQuantifier(lower: Option[UnsignedIntegerLiteral], upper: Option[UnsignedIntegerLiteral])(
  val position: InputPosition
) extends GraphPatternQuantifier {
  override def canBeEmpty: Boolean = lower.map(_.value.longValue()).getOrElse(0L) == 0
}

/**
 * Represents a quantifier like {3} with for which lower and upper bound are equal.
 */
case class FixedQuantifier(value: UnsignedIntegerLiteral)(val position: InputPosition) extends GraphPatternQuantifier {
  override def canBeEmpty: Boolean = value.value == 0
}
