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

import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.Condition
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Match, NodePattern, RelationshipPattern}

case object noUnnamedPatternElementsInMatch extends Condition {
  def apply(that: Any): Seq[String] = {
    val into = collectNodesOfType[Match].apply(that).map(_.pattern)
    into.flatMap(unnamedNodePatterns) ++ into.flatMap(unnamedRelationshipPatterns)
  }

  private def unnamedRelationshipPatterns(that: Any): Seq[String] = {
    collectNodesOfType[RelationshipPattern].apply(that).collect {
      case rel@RelationshipPattern(None, _, _, _, _, _) =>
        s"RelationshipPattern at ${rel.position} is unnamed"
    }
  }

  private def unnamedNodePatterns(that: Any): Seq[String] = {
    collectNodesOfType[NodePattern].apply(that).collect {
      case node@NodePattern(None, _, _, _) =>
        s"NodePattern at ${node.position} is unnamed"
    }
  }

  override def name: String = productPrefix
}
