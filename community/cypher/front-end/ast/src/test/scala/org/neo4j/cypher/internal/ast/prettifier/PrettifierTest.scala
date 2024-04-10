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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllIndexActions
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ArraySeq

class PrettifierTest extends CypherFunSuite with AstConstructionTestSupport {
  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  test("stringify deny privilege") {
    val commandArraySeq = DenyPrivilege(
      DatabasePrivilege(AllIndexActions, NamedDatabasesScope(ArraySeq(NamespacedName("hej")(pos)))(pos))(pos),
      immutable = false,
      None,
      List(AllDatabasesQualifier()(pos)),
      ArraySeq(StringLiteral("hej")(pos.withInputLength(0)))
    )(pos)

    val commandList = DenyPrivilege(
      DatabasePrivilege(AllIndexActions, NamedDatabasesScope(List(NamespacedName("hej")(pos)))(pos))(pos),
      immutable = false,
      None,
      List(AllDatabasesQualifier()(pos)),
      List(StringLiteral("hej")(pos.withInputLength(0)))
    )(pos)

    commandArraySeq shouldBe commandList
    prettifier.asString(commandArraySeq) shouldBe "DENY INDEX MANAGEMENT ON DATABASE hej TO hej"
    prettifier.asString(commandArraySeq) shouldBe prettifier.asString(commandList)
  }
}
