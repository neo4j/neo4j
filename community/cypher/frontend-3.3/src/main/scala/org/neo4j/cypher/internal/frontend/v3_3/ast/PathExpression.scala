/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTPath
import org.neo4j.cypher.internal.frontend.v3_3.symbols.TypeSpec
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection

sealed trait PathStep {
  def dependencies: Set[Variable]
}

final case class NodePathStep(node: Variable, next: PathStep) extends PathStep {
  val dependencies = next.dependencies + node
}

final case class SingleRelationshipPathStep(rel: Variable, direction: SemanticDirection, next: PathStep)
    extends PathStep {
  val dependencies = next.dependencies + rel
}

final case class MultiRelationshipPathStep(rel: Variable, direction: SemanticDirection, next: PathStep)
    extends PathStep {
  val dependencies = next.dependencies + rel
}

case object NilPathStep extends PathStep {
  def dependencies = Set.empty[Variable]
}

case class PathExpression(step: PathStep)(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes: TypeSpec = CTPath
}
