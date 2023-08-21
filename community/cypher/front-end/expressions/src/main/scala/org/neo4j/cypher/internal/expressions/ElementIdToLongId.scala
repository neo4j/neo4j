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

/**
 * Internal expression for converting elementId() strings into long ids. Not exposed in Cypher.
 */
case class ElementIdToLongId(entityType: EntityType, mode: ElementIdToLongId.Mode, elementIdExpr: Expression)(
  override val position: InputPosition
) extends Expression {

  override def asCanonicalStringVal: String = s"elementIdToLongId(${elementIdExpr.asCanonicalStringVal})"

  override def isConstantForQuery: Boolean = elementIdExpr.isConstantForQuery
}

object ElementIdToLongId {
  sealed trait Mode

  object Mode {
    case object Single extends Mode
    case object Many extends Mode
  }
}
