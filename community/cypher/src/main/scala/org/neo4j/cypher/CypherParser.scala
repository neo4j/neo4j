/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher

import internal.commands.Query

class CypherParser(version: String) {
  def this() = this ("1.7")

  val hasVersionDefined = """(?si)^\s*cypher\s*([^\s]+)\s*(.*)""".r

  val v15 = new internal.parser.v1_5.CypherParserImpl
  val v16 = new internal.parser.v1_6.CypherParserImpl
  val v17 = new internal.parser.v1_7.CypherParserImpl

  @throws(classOf[SyntaxException])
  def parse(queryText: String): Query = {

    val (v, q) = queryText match {
      case hasVersionDefined(v1, q1) => (v1, q1)
      case _ => (version, queryText)
    }

    v match {
      case "1.5" => v15.parse(q)
      case "1.6" => v16.parse(q)
      case "1.7" => v17.parse(q)
      case _ => throw new SyntaxException("Versions supported are 1.5, 1.6 and 1.7")
    }

  }
}
