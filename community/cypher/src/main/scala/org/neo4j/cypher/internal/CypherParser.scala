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
import org.neo4j.cypher.internal.commands.AbstractQuery

trait CypherParser {
  def parse(queryText: String): AbstractQuery
}


object CypherParser {
  def apply() = VersionProxy(CypherVersion.vDefault)
  def apply(versionName: String) = VersionProxy(CypherVersion(versionName))
  def apply(defaultVersion: CypherVersion) = VersionProxy(defaultVersion)

  case class VersionProxy(defaultVersion: CypherVersion) extends CypherParser {
    private val hasVersionDefined = """(?si)^\s*cypher\s*([^\s]+)\s*(.*)""".r

    @throws(classOf[SyntaxException])
    def parse(queryText: String): AbstractQuery = {

      val result = queryText match {
        case hasVersionDefined(versionName, remainingQuery) => CypherVersion(versionName).parser.parse(remainingQuery)
        case _                                              => defaultVersion.parser.parse(queryText)
      }

      result.verifySemantics()
      result
    }
  }
}
