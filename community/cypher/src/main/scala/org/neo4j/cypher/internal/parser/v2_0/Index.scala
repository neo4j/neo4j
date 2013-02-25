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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.commands.values.{LabelValue, LabelName}
import org.neo4j.cypher.internal.commands.{DropIndex, CreateIndex}


trait Index extends Base with Labels {
  def createIndex = CREATE ~> indexOps ^^ {
    case (label, properties) => CreateIndex(label, properties)
  }

  def dropIndex = DROP ~> indexOps ^^ {
    case (label, properties) => DropIndex(label, properties)
  }

  private def indexOps: Parser[(String, List[String])] = INDEX ~> ON ~> labelName ~ parens(identity) ^^ {
    case LabelName(labelName) ~ property => (labelName, List(property))
  }
}