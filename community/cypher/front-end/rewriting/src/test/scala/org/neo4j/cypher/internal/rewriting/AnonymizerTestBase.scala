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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.rewriting.rewriters.Anonymizer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

abstract class AnonymizerTestBase extends CypherFunSuite with RewriteTest {

  def anonymizer: Anonymizer

  test("repeatable variable") {
    assertRename(anonymizer.variable)
  }

  test("repeatable label") {
    assertRename(anonymizer.label)
  }

  test("repeatable relationshipType") {
    assertRename(anonymizer.relationshipType)
  }

  test("repeatable labelOrRelationshipType") {
    assertRename(anonymizer.labelOrRelationshipType)
  }

  test("repeatable propertyKey") {
    assertRename(anonymizer.propertyKey)
  }

  test("repeatable parameter") {
    assertRename(anonymizer.parameter)
  }

  test("repeatable literal") {
    assertRename(anonymizer.literal)
  }

  test("repeatable indexName") {
    assertRename(anonymizer.indexName)
  }

  test("repeatable constraintName") {
    assertRename(anonymizer.constraintName)
  }

  // Assert that we get the same names when calling the anonymizer twice on same input
  private def assertRename(rename: String => String): Unit = {
    val names = Set("cat", "bob", "fish", "colly_flower11")
    val anons = names.map(rename)
    anons.size should be(names.size)
    anons should be(names.map(rename))
  }
}
