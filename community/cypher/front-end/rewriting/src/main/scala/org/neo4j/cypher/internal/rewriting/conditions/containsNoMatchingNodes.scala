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
package org.neo4j.cypher.internal.rewriting.conditions

import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition

case class containsNoMatchingNodes(matcher: PartialFunction[ASTNode, String]) extends ValidatingCondition {

  override def apply(that: Any)(cancellationChecker: CancellationChecker): Seq[String] = {
    that.folder(cancellationChecker).fold(Seq.empty[(String, InputPosition)]) {
      case node: ASTNode if matcher.isDefinedAt(node) =>
        acc => acc :+ ((matcher(node), node.position))
    }.map { case (name, position) => s"Expected none but found $name at position $position" }
  }

  override def name: String = productPrefix
}
