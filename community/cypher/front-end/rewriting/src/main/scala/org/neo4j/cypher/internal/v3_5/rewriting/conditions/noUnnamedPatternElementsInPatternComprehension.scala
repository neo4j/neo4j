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
package org.neo4j.cypher.internal.v3_5.rewriting.conditions

import org.neo4j.cypher.internal.v3_5.expressions.{PatternComprehension, PatternElement}
import org.neo4j.cypher.internal.v3_5.util.Foldable._
import org.neo4j.cypher.internal.v3_5.expressions.{PatternElement, RelationshipsPattern}
import org.neo4j.cypher.internal.v3_5.rewriting.Condition

case object noUnnamedPatternElementsInPatternComprehension extends Condition {

  override def name: String = productPrefix

  override def apply(that: Any): Seq[String] = that.treeFold(Seq.empty[String]) {
    case expr: PatternComprehension if containsUnNamedPatternElement(expr.pattern) =>
      acc => (acc :+ s"Expression $expr contains pattern elements which are not named", None)
  }

  private def containsUnNamedPatternElement(expr: RelationshipsPattern) = expr.treeExists {
    case p: PatternElement => p.variable.isEmpty
  }
}
