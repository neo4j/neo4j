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
package org.neo4j.cypher.internal

import org.neo4j.cypher._
import org.neo4j.cypher.CypherVersion._
import org.neo4j.cypher.internal.commands.AbstractQuery


trait CypherCompiler {
  def parse(queryText: String): AbstractQuery
}


object CypherCompiler {
  def apply() = CypherCompilerWrapper("2.0")
  def apply(defaultVersion: String) = CypherCompilerWrapper(defaultVersion)

  case class CypherCompilerWrapper(defaultVersion: String) extends CypherCompiler {
    val hasVersionDefined = """(?si)^\s*cypher\s*([^\s]+)\s*(.*)""".r

    @throws(classOf[SyntaxException])
    def parse(queryText: String): AbstractQuery = {

      val (v, q) = queryText match {
        case hasVersionDefined(v1, q1) => (v1, q1)
        case _                         => (defaultVersion, queryText)
      }

      val result = v match {
        case v1_9.name    => v1_9.compiler.parse(q)
        case v2_0.name    => v2_0.compiler.parse(q)
        case vLegacy.name => vLegacy.compiler.parse(q)
        case _            => throw new SyntaxException("Versions supported are 1.9 and 2.0")
      }
      result.verifySemantics()
      result
    }
  }
}
