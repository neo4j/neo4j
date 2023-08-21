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

object MatchMode {

  sealed trait MatchMode extends ASTNode {
    def prettified: String
    def requiresDifferentRelationships: Boolean
  }

  def default(position: InputPosition): MatchMode = {
    DifferentRelationships(implicitlyCreated = true)(position)
  }

  case class RepeatableElements()(val position: InputPosition) extends MatchMode {
    override def prettified: String = "REPEATABLE ELEMENTS"

    override def requiresDifferentRelationships: Boolean = false
  }

  /**
   * @param implicitlyCreated TODO This is tracked so that we in semantic analysis know if someone explicitly wrote
   *                            "DIFFERENT RELATIONSHIPS" or if we added that implicitly. Adding it explicitly should
   *                            for now fail, unless the semantic feature
   *                            {@link org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MatchModes} has been
   *                            explicitly turned on.
   *
   *                          TODO This can be removed once "MatchModes" is enabled by default.
   */
  case class DifferentRelationships(implicitlyCreated: Boolean = false)(val position: InputPosition) extends MatchMode {
    override def prettified: String = if (implicitlyCreated) "" else "DIFFERENT RELATIONSHIPS"

    override def requiresDifferentRelationships: Boolean = true
  }
}
