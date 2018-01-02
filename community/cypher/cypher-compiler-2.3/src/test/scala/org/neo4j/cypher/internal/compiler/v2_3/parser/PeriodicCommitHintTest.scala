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
package org.neo4j.cypher.internal.compiler.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.parser.{ParserTest, Query}
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, ast}
import org.parboiled.scala._

class PeriodicCommitHintTest extends ParserTest[ast.PeriodicCommitHint, Any] with Query {

  implicit val parserToTest = PeriodicCommitHint ~ EOI

  val t = DummyPosition(0)

  test("tests") {
    parsing("USING PERIODIC COMMIT") shouldGive ast.PeriodicCommitHint(None)(t)
    parsing("USING PERIODIC COMMIT 300") shouldGive ast.PeriodicCommitHint(Some(ast.SignedDecimalIntegerLiteral("300")(t)))(t)
  }

  override def convert(astNode: ast.PeriodicCommitHint): Any = astNode
}
