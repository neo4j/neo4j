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
package org.neo4j.cypher.internal.parser

import v2_0.{Expressions, MatchClause}
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.junit.Test
import org.neo4j.cypher.internal.commands._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.NamedPath


class MatchClauseTest extends MatchClause with Expressions with ParserTest {
  @Test def label_literal_list_parsing() {
    implicit val parserToTest = matching

    parsing("MATCH a-[:FOO|BAR]->b") or
    parsing("MATCH a-[:FOO|:BAR]->b") shouldGive
      RelatedTo("a", "b", "  UNNAMED9", Seq("FOO", "BAR"), Direction.OUTGOING, false)
  }

  implicit def a(p: Pattern): (Seq[Pattern], Seq[NamedPath], Predicate) = (Seq(p), Seq.empty, True())

  def createProperty(entity: String, propName: String): Expression = ???
}