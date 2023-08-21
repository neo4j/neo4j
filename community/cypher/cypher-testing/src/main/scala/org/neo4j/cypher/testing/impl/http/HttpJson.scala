/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.testing.impl.http

import org.json4s.Formats
import org.json4s.NoTypeHints
import org.json4s.native.JsonMethods
import org.json4s.native.Serialization

object HttpJson {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def write[T](value: T): String = Serialization.writePretty(value)

  def read[T: Manifest](string: String): T = JsonMethods.parse(string).extract[T]
}
