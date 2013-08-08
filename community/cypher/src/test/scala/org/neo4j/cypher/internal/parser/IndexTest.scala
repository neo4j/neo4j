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

import org.junit.Test
import legacy.Index
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.commands.{DropIndex, CreateIndex}


class IndexTest extends Index with ParserTest {
  @Test def create() {
    implicit val parser = createIndex

    parsing("create index on :MyLabel(prop1)") or
    parsing("CREATE INDEX ON :MyLabel (prop1)") shouldGive
      CreateIndex("MyLabel", Seq("prop1"))

    assertFails("create index on :MyLabel()")
  }

  @Test def drop() {
    implicit val parser = dropIndex

    parsing("drop index on :MyLabel(prop1)") or
    parsing("DROP INDEX ON :MyLabel (prop1)") shouldGive
      DropIndex("MyLabel", Seq("prop1"))

    assertFails("drop index on :MyLabel()")
  }

  def expression: Parser[Expression] = ???
}