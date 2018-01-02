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
package org.neo4j.cypher.internal.frontend.v2_3.perty.format

object quoteString extends (String => String) {
  def apply(v: String): String = {
    val builder = new StringBuilder
    builder += '\"'
    var i = 0
    while (i < v.length) {
      v.charAt(i) match {
        case '\'' => builder += '\\' += '\''
        case '\"' => builder += '\\' += '\"'
        case '\t' => builder += '\\' += 't'
        case '\b' => builder += '\\' += 'b'
        case '\n' => builder += '\\' += 'n'
        case '\r' => builder += '\\' += 'r'
        case '\f' => builder += '\\' += 'f'
        case '\\' => builder += '\\' += '\\'
        case ch   => builder += ch
      }
      i += 1
    }
    builder += '\"'
    builder.result()
  }
}
