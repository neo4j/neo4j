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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.neo4j.cypher.internal.compiler.v2_0.{SemanticCheckResult, InputPosition, SemanticState, DummyPosition}

@RunWith(classOf[JUnitRunner])
class AutoCommitHintTest extends FunSuite with Positional {
  test("negative values should fail") {
    // Given
    val sizePosition: InputPosition = pos
    val input = "-1"
    val value: SignedIntegerLiteral = SignedIntegerLiteral(input)(sizePosition)
    val hint = AutoCommitHint(Some(value))(pos)

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === s"Commit size error - expected positive value larger than zero, got ${input}")
    assert(result.errors.head.position === sizePosition)
  }

  test("no autocommit size is ok") {
    // Given
    val hint = AutoCommitHint(None)(pos)

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }

  test("positive values are OK") {
    // Given
    val sizePosition: InputPosition = pos
    val input = "1"
    val value: SignedIntegerLiteral = SignedIntegerLiteral(input)(sizePosition)
    val hint = AutoCommitHint(Some(value))(pos)

    // When
    val result: SemanticCheckResult = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }
  
  test("queries with autocommit and no updates are not OK") {
    // Given USING AUTOCOMMIT RETURN "Hello World!"

    val value: SignedIntegerLiteral = SignedIntegerLiteral("1")(pos)
    val autoCommitPos: InputPosition = pos
    val hint = AutoCommitHint(Some(value))(autoCommitPos)
    val literal: StringLiteral = StringLiteral("Hello world!")(pos)
    val returnItem = UnaliasedReturnItem(literal, "Hello world!")(pos)
    val returnItems = ListedReturnItems(Seq(returnItem))(pos)
    val returns: Return = Return(false, returnItems, None, None, None)(pos)
    val queryPart = SingleQuery(Seq(returns))(pos)
    val query = Query(Some(hint), queryPart)(pos)

    // When
    val result: SemanticCheckResult = query.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === "Cannot use autocommit in a non-updating query")
    assert(result.errors.head.position === autoCommitPos)
  }

  test("queries with autocommit and updates are OK") {

    // Given USING AUTOCOMMIT CREATE ()

    val value: SignedIntegerLiteral = SignedIntegerLiteral("1")(pos)
    val hint = AutoCommitHint(Some(value))(pos)
    val nodePattern = NodePattern(None,Seq.empty,None,false)(pos)
    val pattern = Pattern(Seq(EveryPath(nodePattern)))(pos)
    val create = Create(pattern)(pos)
    val queryPart = SingleQuery(Seq(create))(pos)
    val query = Query(Some(hint), queryPart)(pos)

    // When
    val result: SemanticCheckResult = query.semanticCheck(SemanticState.clean)

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