/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.frontend

import org.neo4j.cypher.internal.v4_0.frontend.helpers.{ErrorCollectingContext, NoPlannerName}
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

/**
  * Opposed to `SemanticAnalysisTest` this does not focus on whether
  * something actually passes semantic analysis or not, but rather on helpful
  * error messages.
  */
class SemanticAnalysisErrorMessagesTest extends CypherFunSuite {

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  private val pipeline = Parsing andThen SemanticAnalysis(warn = true) andThen SemanticAnalysis(warn = false)

  // positive tests that we get the error message
  // "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN"

  test("Should give helpful error when accessing illegal variable in ORDER BY after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail ORDER BY p.name RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail ORDER BY p.name RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after RETURN DISTINCT") {
    val query = "MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY p.name"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after RETURN with aggregation") {
    val query = "MATCH (p) RETURN collect(p.email) AS mail ORDER BY p.name"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in WHERE after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail WHERE exists(p.name) RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in WHERE after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail WHERE exists(p.name) RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  // negative tests that we do not get this error message otherwise

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail ORDER BY q.name RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Variable `q` not defined"))
  }
  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail ORDER BY q.name RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN DISTINCT") {
    val query = "MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY q.name"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN with aggregation") {
    val query = "MATCH (p) RETURN collect(p.email) AS mail ORDER BY q.name"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail WHERE exists(q.name) RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail WHERE exists(q.name) RETURN mail AS mail"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Variable `q` not defined"))
  }

  // Empty tokens for node property

  test("Should not allow empty node property key name in CREATE clause") {
    val query = "CREATE ({prop: 5, ``: 1})"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in MERGE clause") {
    val query = "MERGE (n {``: 1})"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ON CREATE SET") {
    val query = "MERGE (n :Label) ON CREATE SET n.`` = 1"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ON MATCH SET") {
    val query = "MERGE (n :Label) ON MATCH SET n.`` = 1"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in MATCH clause") {
    val query = "MATCH (n {``: 1}) RETURN n AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in SET clause") {
    val query = "MATCH (n) SET n.``= 1"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in REMOVE clause") {
    val query = "MATCH (n) REMOVE n.``"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in WHERE clause") {
    val query = "MATCH (n) WHERE n.``= 1 RETURN n AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in WITH clause") {
    val query = "MATCH (n) WITH n.`` AS prop RETURN prop AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ORDER BY in WITH") {
    val query = "MATCH (n) WITH n AS invalid ORDER BY n.`` RETURN count(*) AS count"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in RETURN clause") {
    val query = "MATCH (n) RETURN n.`` AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in DISTINCT RETURN clause") {
    val query = "MATCH (n) RETURN DISTINCT n.`` AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in aggregation in RETURN clause") {
    val query = "MATCH (n) RETURN count(n.``) AS count"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ORDER BY in RETURN") {
    val query = "MATCH (n) RETURN n AS invalid ORDER BY n.`` DESC LIMIT 2"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in CASE clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE n.``
        |WHEN 'val'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in CASE WHEN clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE
        |WHEN n.`` = 'blue'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in CASE THEN clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE
        |WHEN n.prop = 'blue'
        |THEN n.``
        |ELSE 2 END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in CASE ELSE clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE
        |WHEN n.prop = 'blue'
        |THEN 1
        |ELSE n.`` END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }


  // Empty tokens for relationship properties

  test("Should not allow empty relationship property key name in CREATE clause") {
    val query = "CREATE ()-[:REL {``: 1}]->()"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in MERGE clause") {
    val query = "MERGE ()-[r :REL {``: 1, prop: 42}]->()"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ON CREATE SET") {
    val query = "MERGE ()-[r:REL]->() ON CREATE SET r.`` = 1"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ON MATCH SET") {
    val query = "MERGE ()-[r:REL]->() ON MATCH SET r.`` = 1"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in MATCH clause") {
    val query = "MATCH ()-[r {prop:1337, ``: 1}]->() RETURN r AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in SET clause") {
    val query = "MATCH ()-[r]->() SET r.``= 1 RETURN r AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in REMOVE clause") {
    val query = "MATCH ()-[r]->() REMOVE r.``"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in WHERE clause") {
    val query = "MATCH (n)-[r]->() WHERE n.prop > r.`` RETURN n AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in WITH clause") {
    val query = "MATCH ()-[r]->() WITH r.`` AS prop, r.prop as prop2 RETURN count(*) AS count"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ORDER BY in WITH") {
    val query = "MATCH ()-[r]->() WITH r AS invalid ORDER BY r.`` RETURN count(*) AS count"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN r.`` as result"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in DISTINCT RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN DISTINCT r.`` as result"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in aggregation in RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN max(r.``) AS max"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ORDER BY in RETURN") {
    val query = "MATCH ()-[r]->() RETURN r AS result ORDER BY r.``"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in CASE clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE r.``
        |WHEN 'val'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in CASE WHEN clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE
        |WHEN r.`` = 'blue'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in CASE THEN clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE
        |WHEN r.prop = 'blue'
        |THEN r.``
        |ELSE 2 END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in CASE ELSE clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE
        |WHEN r.prop = 'blue'
        |THEN 1
        |ELSE r.`` END AS result
      """.stripMargin

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  // Empty tokens for labels

  test("Should not allow empty label in CREATE clause") {
    val query = "CREATE (:Valid:``)"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in MERGE clause") {
    val query = "MERGE (n:``)"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in MATCH clause") {
    val query = "MATCH (n:``:Valid) RETURN n AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in SET clause") {
    val query = "MATCH (n) SET n:``"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in REMOVE clause") {
    val query = "MATCH (n) REMOVE n:``"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  // Empty tokens for relationship type

  test("Should not allow empty relationship type in CREATE clause") {
    val query = "CREATE ()-[:``]->()"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship type in MERGE clause") {
    val query = "MERGE ()-[r :``]->()"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship type in MATCH clause") {
    val query = "MATCH ()-[r :``]->() RETURN r AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship type in variable length pattern") {
    val query = "MATCH ()-[r :``*1..5]->() RETURN r AS invalid"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List(emptyTokenErrorMessage))
  }

  private val emptyTokenErrorMessage = "'' is not a valid token name. Token names cannot be empty, or contain any null-bytes or back-ticks."

  private def initStartState(query: String, initialFields: Map[String, CypherType]) =
    InitialState(query, None, NoPlannerName, initialFields)
}
