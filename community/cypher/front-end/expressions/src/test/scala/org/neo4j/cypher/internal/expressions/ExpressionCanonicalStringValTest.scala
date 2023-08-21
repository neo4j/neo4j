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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpressionCanonicalStringValTest extends CypherFunSuite {

  protected val pos: InputPosition = InputPosition.NONE
  def varFor(name: String): Variable = Variable(name)(pos)
  def relTypeName(s: String): RelTypeName = RelTypeName(s)(pos)
  def literalInt(value: Long): SignedDecimalIntegerLiteral = SignedDecimalIntegerLiteral(value.toString)(pos)

  test("collect all should render nicely") {
    CollectAll(varFor("x"))(pos).asCanonicalStringVal should equal("collect_all(x)")
  }

  test("get degree should render nicely") {
    GetDegree(varFor("x"), None, OUTGOING)(pos).asCanonicalStringVal should equal("getDegree((x)-->())")
    GetDegree(varFor("x"), None, INCOMING)(pos).asCanonicalStringVal should equal("getDegree((x)<--())")
    GetDegree(varFor("x"), None, BOTH)(pos).asCanonicalStringVal should equal("getDegree((x)--())")
    GetDegree(varFor("x"), Some(relTypeName("Rel")), OUTGOING)(pos).asCanonicalStringVal should equal(
      "getDegree((x)-[:Rel]->())"
    )
    GetDegree(varFor("x"), Some(relTypeName("Rel")), INCOMING)(pos).asCanonicalStringVal should equal(
      "getDegree((x)<-[:Rel]-())"
    )
    GetDegree(varFor("x"), Some(relTypeName("Rel")), BOTH)(pos).asCanonicalStringVal should equal(
      "getDegree((x)-[:Rel]-())"
    )
  }

  test("has degree greater than should render nicely") {
    HasDegreeGreaterThan(varFor("node"), None, BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)--()) > 10")

    HasDegreeGreaterThan(varFor("node"), None, INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<--()) > 10")

    HasDegreeGreaterThan(varFor("node"), None, OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-->()) > 10")

    HasDegreeGreaterThan(varFor("node"), Some(RelTypeName("Rel")(pos)), BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]-()) > 10")

    HasDegreeGreaterThan(varFor("node"), Some(RelTypeName("Rel")(pos)), INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<-[:Rel]-()) > 10")

    HasDegreeGreaterThan(varFor("node"), Some(RelTypeName("Rel")(pos)), OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]->()) > 10")
  }

  test("HasDegreeLessThan should render nicely") {
    HasDegreeLessThan(varFor("node"), None, BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)--()) < 10")

    HasDegreeLessThan(varFor("node"), None, INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<--()) < 10")

    HasDegreeLessThan(varFor("node"), None, OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-->()) < 10")

    HasDegreeLessThan(varFor("node"), Some(RelTypeName("Rel")(pos)), BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]-()) < 10")

    HasDegreeLessThan(varFor("node"), Some(RelTypeName("Rel")(pos)), INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<-[:Rel]-()) < 10")

    HasDegreeLessThan(varFor("node"), Some(RelTypeName("Rel")(pos)), OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]->()) < 10")
  }

  test("HasDegreeLessThanOrEqual should render nicely") {
    HasDegreeLessThanOrEqual(varFor("node"), None, BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)--()) <= 10")

    HasDegreeLessThanOrEqual(varFor("node"), None, INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<--()) <= 10")

    HasDegreeLessThanOrEqual(varFor("node"), None, OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-->()) <= 10")

    HasDegreeLessThanOrEqual(varFor("node"), Some(RelTypeName("Rel")(pos)), BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]-()) <= 10")

    HasDegreeLessThanOrEqual(varFor("node"), Some(RelTypeName("Rel")(pos)), INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<-[:Rel]-()) <= 10")

    HasDegreeLessThanOrEqual(varFor("node"), Some(RelTypeName("Rel")(pos)), OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]->()) <= 10")
  }

  test("HasDegreeGreaterThanOrEqual should render nicely") {
    HasDegreeGreaterThanOrEqual(varFor("node"), None, BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)--()) >= 10")

    HasDegreeGreaterThanOrEqual(varFor("node"), None, INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<--()) >= 10")

    HasDegreeGreaterThanOrEqual(varFor("node"), None, OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-->()) >= 10")

    HasDegreeGreaterThanOrEqual(varFor("node"), Some(RelTypeName("Rel")(pos)), BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]-()) >= 10")

    HasDegreeGreaterThanOrEqual(varFor("node"), Some(RelTypeName("Rel")(pos)), INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<-[:Rel]-()) >= 10")

    HasDegreeGreaterThanOrEqual(varFor("node"), Some(RelTypeName("Rel")(pos)), OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]->()) >= 10")
  }

  test("HasDegree should render nicely") {
    HasDegree(varFor("node"), None, BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)--()) = 10")

    HasDegree(varFor("node"), None, INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<--()) = 10")

    HasDegree(varFor("node"), None, OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-->()) = 10")

    HasDegree(varFor("node"), Some(RelTypeName("Rel")(pos)), BOTH, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]-()) = 10")

    HasDegree(varFor("node"), Some(RelTypeName("Rel")(pos)), INCOMING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)<-[:Rel]-()) = 10")

    HasDegree(varFor("node"), Some(RelTypeName("Rel")(pos)), OUTGOING, literalInt(10L))(pos)
      .asCanonicalStringVal should equal("getDegree((node)-[:Rel]->()) = 10")
  }

}
