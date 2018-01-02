/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.symbols.{CTPath, TypeSpec}
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticDirection}

sealed trait PathStep {
  def dependencies: Set[Identifier]
}

final case class NodePathStep(node: Identifier, next: PathStep) extends PathStep {
  val dependencies = next.dependencies + node
}

final case class SingleRelationshipPathStep(rel: Identifier, direction: SemanticDirection, next: PathStep) extends PathStep {
  val dependencies = next.dependencies + rel
}

final case class MultiRelationshipPathStep(rel: Identifier, direction: SemanticDirection, next: PathStep) extends PathStep {
  val dependencies = next.dependencies + rel
}

case object NilPathStep extends PathStep {
  def dependencies = Set.empty[Identifier]
}

case class PathExpression(step: PathStep)(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes: TypeSpec = CTPath
}
