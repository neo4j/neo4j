/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j.it

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccParserTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.DummyPosition

class PeriodicCommitHintIT extends JavaccParserTestBase[ast.Query, ast.PeriodicCommitHint] {

  implicit val parserToTest: JavaccRule[ast.Query] = JavaccRule.fromQueryAndParser(
    transformQuery = q => s"$q LOAD CSV FROM '' AS line RETURN 1",
    runParser = _.PeriodicCommitQuery()
  )

  private val t = DummyPosition(0)

  test("tests") {
    parsing("USING PERIODIC COMMIT") shouldGive ast.PeriodicCommitHint(None)(t)
    parsing("USING PERIODIC COMMIT 300") shouldGive ast.PeriodicCommitHint(Some(UnsignedDecimalIntegerLiteral("300")(t)))(t)
  }

  override def convert(astNode: ast.Query): ast.PeriodicCommitHint = astNode.periodicCommitHint.get
}
