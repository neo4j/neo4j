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
package org.neo4j.cypher.internal.frontend.helpers

trait ShortestSyntax {

  case class SelectorSyntax(
    syntax: String,
    selective: Boolean,
    shortest: Boolean
  )

  val selectors: Seq[SelectorSyntax] = Seq(
    SelectorSyntax("ALL", selective = false, shortest = false),
    SelectorSyntax("ANY 2 PATHS", selective = true, shortest = false),
    SelectorSyntax("SHORTEST 2 PATHS", selective = true, shortest = true),
    SelectorSyntax("ALL SHORTEST PATHS", selective = true, shortest = true),
    SelectorSyntax("SHORTEST 1 PATH GROUPS", selective = true, shortest = true)
  )

  val allSelectiveSelectors: Seq[String] = selectors.filter(_.selective).map(_.syntax)

  val allSelectors: Seq[String] = selectors.map(_.syntax)
}
