/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.verifiers

import org.neo4j.cypher.internal.commands.Query
import org.neo4j.cypher.PatternException


object OptionalPatternWithoutStartVerifier extends Verifier {
  def verifyFunction = {
    case Query(_, start, _, patterns, _, _, _, _, _, _, _, _)
      if start.isEmpty && patterns.exists(_.optional) =>

      val optionalRelationships: String = patterns.
        filter(_.optional).
        flatMap(_.rels.map("`" + _ + "`")).
        mkString(", ")

      throw new PatternException("Can't use optional patterns without explicit START clause. Optional relationships: " + optionalRelationships)
  }
}