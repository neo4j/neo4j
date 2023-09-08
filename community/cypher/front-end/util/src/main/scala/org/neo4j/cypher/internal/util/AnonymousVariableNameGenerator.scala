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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator.prefix
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.UNNAMED_PATTERN

class AnonymousVariableNameGenerator() {
  private var counter = 0

  def nextName: String = {
    val result = s"$prefix$counter"
    counter += 1
    result
  }
}

object AnonymousVariableNameGenerator {
  val generatorName = "UNNAMED"
  private val prefix = s"  $generatorName"

  def isNamed(x: String): Boolean = !notNamed(x)
  def notNamed(x: String): Boolean = UNNAMED_PATTERN.matches(x)
}
