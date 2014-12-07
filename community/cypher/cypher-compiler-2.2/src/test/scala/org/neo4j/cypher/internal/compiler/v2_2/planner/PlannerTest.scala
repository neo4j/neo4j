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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.{ASTAnnotationMap, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.{ExpressionTypeInfo, InputPosition}

class PlannerTest extends CypherFunSuite {

  test("Renames identifiers in semantic table") {
    val idA1 = Identifier("a")(InputPosition(1, 0, 1))
    val idA2 = Identifier("a")(InputPosition(2, 0, 2))
    val idA3 = Identifier("a")(InputPosition(3, 0, 3))
    val idB5 = Identifier("b")(InputPosition(5, 0, 5))

    val infoA1 = mock[ExpressionTypeInfo]
    val infoA2 = mock[ExpressionTypeInfo]
    val infoA3 = mock[ExpressionTypeInfo]
    val infoA4 = mock[ExpressionTypeInfo]
    val infoB5 = mock[ExpressionTypeInfo]

    val table = SemanticTable(ASTAnnotationMap(
      idA1 -> infoA1,
      idA2 -> infoA2,
      idA3 -> infoA3,
      idB5 -> infoB5
    ))

    val identifierNames = Map(
      ("a", InputPosition(1, 0 ,1)) -> "a@1",
      ("a", InputPosition(2, 0, 2)) -> "a@2"
    )

    val newTable = Planner.rewriteSemanticTable(identifierNames, table)

    newTable.types should equal(ASTAnnotationMap(
      Identifier("a@1")(InputPosition(1, 0, 1)) -> infoA1,
      Identifier("a@2")(InputPosition(2, 0, 2)) -> infoA2,
      idA3 -> infoA3,
      idB5 -> infoB5
    ))
  }
}
