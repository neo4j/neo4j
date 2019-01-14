/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.Anonymizer
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

abstract class AnonymizerTestBase extends CypherFunSuite with RewriteTest {

  def anonymizer: Anonymizer

  test("repeatable variable") {
    assertRename(Set("cat", "bob", "fish", "colly_flower11"), anonymizer.variable)
  }

  test("repeatable label") {
    assertRename(Set("cat", "bob", "fish", "colly_flower11"), anonymizer.label)
  }

  test("repeatable relationshipType") {
    assertRename(Set("cat", "bob", "fish", "colly_flower11"), anonymizer.relationshipType)
  }

  test("repeatable propertyKey") {
    assertRename(Set("cat", "bob", "fish", "colly_flower11"), anonymizer.propertyKey)
  }

  test("repeatable parameter") {
    assertRename(Set("cat", "bob", "fish", "colly_flower11"), anonymizer.parameter)
  }

  private def assertRename(names: Set[String], rename: String => String): Unit = {
    val anons = names.map(rename)
    anons.size should be(4)
    anons should be(names.map(rename))
  }
}
