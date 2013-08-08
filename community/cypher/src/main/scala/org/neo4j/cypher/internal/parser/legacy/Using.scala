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
package org.neo4j.cypher.internal.parser.legacy

import org.neo4j.cypher.internal.commands.{StartItem, Hint, NodeByLabel, SchemaIndex}


trait Using extends Expressions {
  def hints: Parser[Seq[StartItem with Hint]] = rep(indexHint|scanHint)

  def indexHint: Parser[SchemaIndex] = USING ~> INDEX ~> identity ~ ":" ~ escapableString ~ parens(escapableString) ^^ {
    case id ~ ":" ~ label ~ prop => SchemaIndex(id, label, prop, None)
  }

  def scanHint: Parser[NodeByLabel] = USING ~> SCAN ~> identity ~ ":" ~ escapableString ^^ {
    case id ~ ":" ~ label => NodeByLabel(id, label)
  }
}