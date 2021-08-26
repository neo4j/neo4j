/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.util.InputPosition

final case class PreParserOption(key: String, value: String)
object PreParserOption {
  val explain: PreParserOption = PreParserOption(CypherExecutionMode.name, "EXPLAIN")
  val profile: PreParserOption = PreParserOption(CypherExecutionMode.name, "PROFILE")
  def version(value: String): PreParserOption = PreParserOption(CypherVersion.name, value)
  def generic(key: String, value: String): PreParserOption = PreParserOption(key, value)
}

final case class PreParsedStatement(statement: String, options: List[PreParserOption], offset: InputPosition)
