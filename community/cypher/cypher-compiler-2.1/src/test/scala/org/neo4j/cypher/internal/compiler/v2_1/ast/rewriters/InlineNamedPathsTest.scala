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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Return
import org.neo4j.cypher.internal.compiler.v2_1.ast.Query
import org.neo4j.cypher.internal.compiler.v2_1.ast.SingleQuery
import org.neo4j.graphdb.Direction

class InlineNamedPathsTest extends CypherFunSuite {

  import parser.ParserFixture._

  val pos = DummyPosition(0)

  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

  test("match p = (a) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, NilPathStep)
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (a)-[r]->(b) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, SingleRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (b)<-[r]->(a) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, SingleRelationshipPathStep(Identifier("r")_, Direction.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (a)-[r*]->(b) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r*]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, MultiRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (b)<-[r*]-(a) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r*]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, MultiRelationshipPathStep(Identifier("r")_, Direction.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  private def parseReturnedExpr(queryText: String) =
    inlineNamedPaths(parser.parse(queryText)) match {
      case Query(_, SingleQuery(Seq(_, Return(_, ListedReturnItems(Seq(UnaliasedReturnItem(expr, "p"))), _, _, _)))) => expr
    }

}
