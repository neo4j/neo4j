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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, InputPosition, SemanticState}

class PeriodicCommitHintTest extends CypherFunSuite with Positional {
  test("negative values should fail") {
    // Given
    val sizePosition: InputPosition = pos
    val input = "-1"
    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral(input)(sizePosition)
    val hint = PeriodicCommitHint(Some(value))(pos)

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === s"Commit size error - expected positive value larger than zero, got ${input}")
    assert(result.errors.head.position === sizePosition)
  }

  test("no periodic commit size is ok") {
    // Given
    val hint = PeriodicCommitHint(None)(pos)

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }

  test("positive values are OK") {
    // Given
    val sizePosition: InputPosition = pos
    val input = "1"
    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral(input)(sizePosition)
    val hint = PeriodicCommitHint(Some(value))(pos)

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }

  test("queries with periodic commit and no updates are not OK") {
    // Given USING PERIODIC COMMIT RETURN "Hello World!"

    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral("1")(pos)
    val periodicCommitPos: InputPosition = pos
    val hint = PeriodicCommitHint(Some(value))(periodicCommitPos)
    val literal: StringLiteral = StringLiteral("Hello world!")(pos)
    val returnItem = UnaliasedReturnItem(literal, "Hello world!")(pos)
    val returnItems = ReturnItems(includeExisting = false, Seq(returnItem))(pos)
    val returns: Return = Return(false, returnItems, None, None, None)(pos)
    val queryPart = SingleQuery(Seq(returns))(pos)
    val query = Query(Some(hint), queryPart)(pos)

    // When
    val result = query.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === "Cannot use periodic commit in a non-updating query")
    assert(result.errors.head.position === periodicCommitPos)
  }

  test("queries with periodic commit and updates are OK") {

    // Given USING PERIODIC COMMIT CREATE ()

    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral("1")(pos)
    val hint = PeriodicCommitHint(Some(value))(pos)
    val nodePattern = NodePattern(None,Seq.empty,None,false)(pos)
    val pattern = Pattern(Seq(EveryPath(nodePattern)))(pos)
    val create = Create(pattern)(pos)
    val queryPart = SingleQuery(Seq(create))(pos)
    val query = Query(Some(hint), queryPart)(pos)

    // When
    val result = query.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }
}

trait Positional {
  var currentPos = 0
  def pos = {
    currentPos += 1
    DummyPosition(currentPos)
  }
}
