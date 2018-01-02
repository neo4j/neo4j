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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v2_3.Foldable._
import org.neo4j.cypher.internal.frontend.v2_3.InputPosition
import org.neo4j.cypher.internal.frontend.v2_3.ast.ASTNode

case class containsNoMatchingNodes(matcher: PartialFunction[ASTNode, String]) extends (Any => Seq[String]) {

  def apply(that: Any): Seq[String] = {
    that.fold(Seq.empty[(String, InputPosition)]) {
      case node: ASTNode if matcher.isDefinedAt(node) =>
        (acc) => acc :+ ((matcher(node), node.position))
    }.map{ case (name, position) => s"Expected none but found $name at position $position" }
  }
}
