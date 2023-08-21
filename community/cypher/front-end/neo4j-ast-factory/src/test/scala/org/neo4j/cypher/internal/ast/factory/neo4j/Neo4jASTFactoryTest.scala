/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.ast.factory.CreateIndexTypes
import org.neo4j.cypher.internal.ast.factory.HintIndexType
import org.neo4j.cypher.internal.ast.factory.ShowCommandFilterTypes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class Neo4jASTFactoryTest extends CypherFunSuite {

  test("invalidDropCommand") {
    ASTExceptionFactory.invalidDropCommand shouldBe "Unsupported drop constraint command: Please delete the constraint by name instead"
  }

  test("relationshipPatternNotAllowed") {
    ASTExceptionFactory.relationshipPatternNotAllowed(
      ConstraintType.NODE_UNIQUE
    ) shouldBe "'IS NODE UNIQUE' does not allow relationship patterns"
  }

  test("nodePatternNotAllowed") {
    ASTExceptionFactory.nodePatternNotAllowed(
      ConstraintType.REL_KEY
    ) shouldBe "'IS RELATIONSHIP KEY' does not allow node patterns"
  }

  test("onlySinglePropertyAllowed") {
    ASTExceptionFactory.onlySinglePropertyAllowed(
      ConstraintType.NODE_EXISTS
    ) shouldBe "Constraint type 'EXISTS' does not allow multiple properties"
  }

  test("invalidShowFilterType") {
    ASTExceptionFactory.invalidShowFilterType(
      "indexes",
      ShowCommandFilterTypes.INVALID
    ) shouldBe "Filter type INVALID is not defined for show indexes command."
  }

  test("invalidCreateIndexType") {
    ASTExceptionFactory.invalidCreateIndexType(
      CreateIndexTypes.INVALID
    ) shouldBe "Index type INVALID is not defined for create index command."
  }

  test("invalidBTREEHintIndexType") {
    ASTExceptionFactory.invalidHintIndexType(
      HintIndexType.BTREE
    ) shouldBe "Index type BTREE is no longer supported for USING index hint. Use TEXT, RANGE or POINT instead."
  }
}
