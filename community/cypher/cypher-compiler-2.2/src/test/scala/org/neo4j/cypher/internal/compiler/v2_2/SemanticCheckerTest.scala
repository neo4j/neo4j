/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.Return

class SemanticCheckerTest extends CypherFunSuite {

  val semantickChecker = new SemanticChecker(mock[SemanticCheckMonitor])
  import parser.ParserFixture._

  test("correctly exposes simple scope used by RETURN") {
    val result = returnScopeNames("MATCH n RETURN *")

    result should equal(Some(Set("n")))
  }

  test("correctly exposes complex scope used by RETURN") {
    val result = returnScopeNames("WITH 1 as n MATCH m RETURN *")

    result should equal(Some(Set("n", "m")))
  }

  test("correctly exposes scope that removes symbols and is used by RETURN") {
    val result = returnScopeNames("WITH 1 as n MATCH m WITH n, 2 AS x RETURN *")

    result should equal(Some(Set("n", "x")))
  }

  private def returnScopeNames(queryText: String) = {
    val statement = parser.parse(queryText)
    val table = semantickChecker.check(queryText, statement)
    val ret = statement.treeFold[Option[Return]](None) {
      case r: Return => (_, _) => Some(r)
      case _         => (acc, children) => children(acc)
    }
    ret.map( x => table.namesInScope(x.position) )
  }
}
