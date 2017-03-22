/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_1.parser

import org.neo4j.cypher.internal.frontend.v3_1.{DummyPosition, ast}
import org.parboiled.scala._

class MergeTest extends ParserTest[Any, Any] with Clauses {

  override def Clause: Rule1[ast.Clause] = ???
  implicit val rule_under_test = Merge
  val p = DummyPosition(0)

  test("can parse locking hint for MERGE") {
    parsing("MERGE (x:Foo{bar:'baz'}) USING EXCLUSIVE LOCK") shouldMatch {
      case merge:ast.Merge => merge shouldBe 'exclusive
    }
  }

  test("MERGE without exclusive lock hint should not be exclusive") {
    parsing("MERGE (x:Foo{bar:'baz'})") shouldMatch {
      case merge:ast.Merge => merge shouldNot be('exclusive)
    }
  }

  def convert(result: Any): Any = result
}
