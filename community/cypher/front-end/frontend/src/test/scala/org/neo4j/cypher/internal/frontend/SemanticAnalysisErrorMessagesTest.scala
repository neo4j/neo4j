/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseGraphSelector
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.OpenCypherJavaCCWithFallbackParsing
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.DeprecatedRepeatedRelVarInPatternExpression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.SubqueryVariableShadowing
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Opposed to `SemanticAnalysisTest` this does not focus on whether
 * something actually passes semantic analysis or not, but rather on helpful
 * error messages.
 */
class SemanticAnalysisErrorMessagesTest extends CypherFunSuite {

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  private def pipelineWithFeatures(features: Seq[SemanticFeature]) =
    OpenCypherJavaCCWithFallbackParsing andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true, features: _*) andThen
      SemanticAnalysis(warn = false, features: _*)

  private val pipeline = pipelineWithFeatures(Seq.empty)

  private val emptyTokenErrorMessage = "'' is not a valid token name. Token names cannot be empty or contain any null-bytes."

  // positive tests that we get the error message
  // "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN"

  test("Should give helpful error when accessing illegal variable in ORDER BY after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail ORDER BY p.name RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail ORDER BY p.name RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after RETURN DISTINCT") {
    val query = "MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY p.name"
    expectErrorMessagesFrom(query, List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after RETURN with aggregation") {
    val query = "MATCH (p) RETURN collect(p.email) AS mail ORDER BY p.name"
    expectErrorMessagesFrom(query, List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in WHERE after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail WHERE p.name IS NOT NULL RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  test("Should give helpful error when accessing illegal variable in WHERE after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail WHERE p.name IS NOT NULL RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p"))
  }

  // negative tests that we do not get this error message otherwise

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail ORDER BY q.name RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("Variable `q` not defined"))
  }
  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail ORDER BY q.name RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN DISTINCT") {
    val query = "MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY q.name"
    expectErrorMessagesFrom(query, List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN with aggregation") {
    val query = "MATCH (p) RETURN collect(p.email) AS mail ORDER BY q.name"
    expectErrorMessagesFrom(query, List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail WHERE q.name IS NOT NULL RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("Variable `q` not defined"))
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail WHERE q.name IS NOT NULL RETURN mail AS mail"
    expectErrorMessagesFrom(query, List("Variable `q` not defined"))
  }

  // Empty tokens for node property

  test("Should not allow empty node property key name in CREATE clause") {
    val query = "CREATE ({prop: 5, ``: 1})"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in MERGE clause") {
    val query = "MERGE (n {``: 1})"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ON CREATE SET") {
    val query = "MERGE (n :Label) ON CREATE SET n.`` = 1"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ON MATCH SET") {
    val query = "MERGE (n :Label) ON MATCH SET n.`` = 1"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in MATCH clause") {
    val query = "MATCH (n {``: 1}) RETURN n AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in SET clause") {
    val query = "MATCH (n) SET n.``= 1"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in REMOVE clause") {
    val query = "MATCH (n) REMOVE n.``"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in WHERE clause") {
    val query = "MATCH (n) WHERE n.``= 1 RETURN n AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in WITH clause") {
    val query = "MATCH (n) WITH n.`` AS prop RETURN prop AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ORDER BY in WITH") {
    val query = "MATCH (n) WITH n AS invalid ORDER BY n.`` RETURN count(*) AS count"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in RETURN clause") {
    val query = "MATCH (n) RETURN n.`` AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in DISTINCT RETURN clause") {
    val query = "MATCH (n) RETURN DISTINCT n.`` AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in aggregation in RETURN clause") {
    val query = "MATCH (n) RETURN count(n.``) AS count"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty node property key name in ORDER BY in RETURN") {
    val query = "MATCH (n) RETURN n AS invalid ORDER BY n.`` DESC LIMIT 2"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  // Empty tokens for relationship properties

  test("Should not allow empty relationship property key name in CREATE clause") {
    val query = "CREATE ()-[:REL {``: 1}]->()"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in MERGE clause") {
    val query = "MERGE ()-[r :REL {``: 1, prop: 42}]->()"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ON CREATE SET") {
    val query = "MERGE ()-[r:REL]->() ON CREATE SET r.`` = 1"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ON MATCH SET") {
    val query = "MERGE ()-[r:REL]->() ON MATCH SET r.`` = 1"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in MATCH clause") {
    val query = "MATCH ()-[r {prop:1337, ``: 1}]->() RETURN r AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in SET clause") {
    val query = "MATCH ()-[r]->() SET r.``= 1 RETURN r AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in REMOVE clause") {
    val query = "MATCH ()-[r]->() REMOVE r.``"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in WHERE clause") {
    val query = "MATCH (n)-[r]->() WHERE n.prop > r.`` RETURN n AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in WITH clause") {
    val query = "MATCH ()-[r]->() WITH r.`` AS prop, r.prop as prop2 RETURN count(*) AS count"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ORDER BY in WITH") {
    val query = "MATCH ()-[r]->() WITH r AS invalid ORDER BY r.`` RETURN count(*) AS count"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN r.`` as result"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in DISTINCT RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN DISTINCT r.`` as result"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in aggregation in RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN max(r.``) AS max"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship property key name in ORDER BY in RETURN") {
    val query = "MATCH ()-[r]->() RETURN r AS result ORDER BY r.``"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
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
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  // Empty tokens for labels

  test("Should not allow empty label in CREATE clause") {
    val query = "CREATE (:Valid:``)"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in MERGE clause") {
    val query = "MERGE (n:``)"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in MATCH clause") {
    val query = "MATCH (n:``:Valid) RETURN n AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in SET clause") {
    val query = "MATCH (n) SET n:``"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty label in REMOVE clause") {
    val query = "MATCH (n) REMOVE n:``"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  // Empty tokens for relationship type

  test("Should not allow empty relationship type in CREATE clause") {
    val query = "CREATE ()-[:``]->()"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship type in MERGE clause") {
    val query = "MERGE ()-[r :``]->()"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship type in MATCH clause") {
    val query = "MATCH ()-[r :``]->() RETURN r AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow empty relationship type in variable length pattern") {
    val query = "MATCH ()-[r :``*1..5]->() RETURN r AS invalid"
    expectErrorMessagesFrom(query, List(emptyTokenErrorMessage))
  }

  test("Should not allow to use aggregate functions inside aggregate functions") {
    val query = "WITH 1 AS x RETURN sum(max(x)) AS sumOfMax"
    expectErrorMessagesFrom(query, List("Can't use aggregate functions inside of aggregate functions."))
  }

  test("Should not allow to use count(*) inside aggregate functions") {
    val query = "WITH 1 AS x RETURN min(count(*)) AS minOfCount"
    expectErrorMessagesFrom(query, List("Can't use aggregate functions inside of aggregate functions."))
  }

  test("Should not allow repeating rel variable in pattern") {
    val query = "MATCH ()-[r]-()-[r]-() RETURN r AS r"
    expectErrorMessagesFrom(query, List("Cannot use the same relationship variable 'r' for multiple patterns"))
  }

  test("Should warn about repeated rel variable in pattern expression") {
    val query = normalizeNewLines("MATCH ()-[r]-() RETURN size( ()-[r]-()-[r]-() ) AS size")
    expectNotificationsFrom(query, Set(DeprecatedRepeatedRelVarInPatternExpression(InputPosition(33, 1, 34), "r")))
  }

  test("Should warn about repeated rel variable in pattern comprehension") {
    val query = "MATCH ()-[r]-() RETURN [ ()-[r]-()-[r]-() | r ] AS rs"
    expectNotificationsFrom(query, Set(DeprecatedRepeatedRelVarInPatternExpression(InputPosition(29, 1, 30), "r")))
  }

  test("Should type check predicates in FilteringExpression") {
    val queries = Seq(
      "RETURN [x IN [1,2,3] WHERE 42 | x + 1] AS foo",
      "RETURN all(x IN [1,2,3] WHERE 42) AS foo",
      "RETURN any(x IN [1,2,3] WHERE 42) AS foo",
      "RETURN none(x IN [1,2,3] WHERE 42) AS foo",
      "RETURN single(x IN [1,2,3] WHERE 42) AS foo",
    )
    queries.foreach { query =>
      withClue(query) {
        expectErrorMessagesFrom(query, List("Type mismatch: expected Boolean but was Integer"))
      }
    }
  }

  test("Returning a variable that is already bound outside should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN i
        |}
        |RETURN i
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `i` already declared in outer scope", 4, 10)
    ))
  }

  test("Returning a variable that is already bound outside, from a union, should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN i
        |    UNION
        |  WITH 3 AS i
        |  RETURN 2 AS i
        |}
        |RETURN i
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `i` already declared in outer scope", 4, 10),
      ("Variable `i` already declared in outer scope", 7, 15),
    ))
  }

  test("Returning a variable implicitly that is already bound outside should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN *
        |}
        |RETURN i
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(e => (e.msg, e.position.line)) should equal(List(
      ("Variable `i` already declared in outer scope", 4)
    ))
  }

  test("Returning a variable implicitly that is already bound outside, from a union, should give a useful error") {
    val query =
      """WITH 1 AS i
        |CALL {
        |  WITH 2 AS i
        |  RETURN *
        |    UNION
        |  WITH 3 AS i
        |  RETURN *
        |}
        |RETURN i
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(e => (e.msg, e.position.line)) should equal(List(
      ("Variable `i` already declared in outer scope", 4),
      ("Variable `i` already declared in outer scope", 7),
    ))
  }

  test("Should warn about variable shadowing in a subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(33, 3, 10), "shadowed")))
  }

  test("Should warn about variable shadowing in a subquery when aliasing") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (n)-[:REL]->(m)
        |  WITH m AS shadowed // warning here
        |  WITH shadowed AS m
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(60, 4, 13), "shadowed")))
  }

  test("Should warn about variable shadowing in a nested subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (n)-[:REL]->(m)
        |  CALL {
        |    MATCH (shadowed)-[:REL]->(x) // warning here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(68, 5, 12), "shadowed")))
  }

  test("Should warn about variable shadowing from enclosing subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  WITH shadowed
        |  MATCH (shadowed)-[:REL]->(m)
        |  CALL {
        |    MATCH (shadowed)-[:REL]->(x) // warning here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(91, 6, 12), "shadowed")))
  }

  test("Should warn about multiple shadowed variables in a subquery") {
    val query =
      """MATCH (shadowed)-->(alsoShadowed)
        |CALL {
        |  MATCH (shadowed)-->(alsoShadowed) // multiple warnings here
        |  RETURN shadowed AS n, alsoShadowed AS m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(
      SubqueryVariableShadowing(InputPosition(50, 3, 10), "shadowed"),
      SubqueryVariableShadowing(InputPosition(63, 3, 23), "alsoShadowed")
    ))
  }

  test("Should warn about multiple shadowed variables in a nested subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  CALL {
        |    MATCH (shadowed)-[:REL]->(x) // and also here
        |    RETURN x
        |  }
        |  RETURN m, x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(
      SubqueryVariableShadowing(InputPosition(33, 3, 10), "shadowed"),
      SubqueryVariableShadowing(InputPosition(91, 5, 12), "shadowed")
    ))
  }

  test("Should not warn about variable shadowing in a subquery if it has been removed from scope by WITH") {
    val query =
      """MATCH (notShadowed)
        |WITH notShadowed AS n
        |CALL {
        |  MATCH (notShadowed)-[:REL]->(m)
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set.empty)
  }

  test("Should not warn about variable shadowing in a subquery if it has been imported previously") {
    val query =
      """MATCH (notShadowed)
        |CALL {
        |  WITH notShadowed
        |  MATCH (notShadowed)-[:REL]->(m)
        |  WITH m AS notShadowed
        |  RETURN notShadowed AS x
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set.empty)
  }

  test("Should warn about variable shadowing in an union subquery") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  MATCH (m) RETURN m
        | UNION
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(61, 5, 10), "shadowed")))
  }

  test("Should warn about variable shadowing in one of the union subquery branches") {
    val query =
      """MATCH (shadowed)
        |CALL {
        |  WITH shadowed
        |  MATCH (shadowed)-[:REL]->(m)
        |  RETURN m
        | UNION
        |  MATCH (shadowed)-[:REL]->(m) // warning here
        |  RETURN m
        | UNION
        |  MATCH (x) RETURN x AS m
        |}
        |RETURN *""".stripMargin
    expectNotificationsFrom(query, Set(SubqueryVariableShadowing(InputPosition(98, 7, 10), "shadowed")))
  }

  test("Should disallow introducing variables in pattern expressions") {
    val query = "MATCH (x) WHERE (x)-[r]-(y) RETURN x"
    expectErrorMessagesFrom(query, List(
      "PatternExpressions are not allowed to introduce new variables: 'r'.",
      "PatternExpressions are not allowed to introduce new variables: 'y'."
    ))
  }

  test("Skip with PatternComprehension should complain") {
    val query = "RETURN 1 SKIP size([(a)-->(b) | a.prop])"
    expectErrorMessagesFrom(query, List("It is not allowed to refer to variables in SKIP"))
  }

  test("Skip with PatternExpression should complain") {
    val query = "RETURN 1 SKIP size(()-->())"
    expectErrorMessagesFrom(query, List("It is not allowed to refer to variables in SKIP"))
  }

  test("Limit with PatternComprehension should complain") {
    val query = "RETURN 1 LIMIT size([(a)-->(b) | a.prop])"
    expectErrorMessagesFrom(query, List("It is not allowed to refer to variables in LIMIT"))
  }

  test("Limit with PatternExpression should complain") {
    val query = "RETURN 1 LIMIT size(()-->())"
    expectErrorMessagesFrom(query, List("It is not allowed to refer to variables in LIMIT"))
  }

  test("UNION with incomplete first part") {
    val query = "MATCH (a) WITH a UNION MATCH (a) RETURN a"
    expectErrorMessagesFrom(query,
      List(
        "Query cannot conclude with WITH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
        "All sub queries in an UNION must have the same return column names"
      )
    )
  }

  test("UNION with incomplete second part") {
    val query = "MATCH (a) RETURN a UNION MATCH (a) WITH a"
    expectErrorMessagesFrom(query,
      List(
        "Query cannot conclude with WITH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
        "All sub queries in an UNION must have the same return column names"
      )
    )
  }

  test("Query ending in CALL ... YIELD ...") {
    val query = "MATCH (a) CALL proc.foo() YIELD bar"
    expectErrorMessagesFrom(query, List("Query cannot conclude with CALL together with YIELD"))
  }

  test("Query with only importing WITH") {
    val query = "WITH a"
    expectErrorMessagesFrom(query, List(
      "Variable `a` not defined",
      "Query cannot conclude with WITH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)"
    ))
  }

  test("Subquery with only importing WITH") {
    val query = "WITH 1 AS a CALL { WITH a } RETURN a"
    expectErrorMessagesFrom(query, List("Query must conclude with a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD"))
  }

  test("Subquery with only USE") {
    val query = "WITH 1 AS a CALL { USE x } RETURN a"
    expectErrorMessagesWithFeaturesFrom(Seq(UseGraphSelector), query, List(
      "Query must conclude with a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD",
    ))
  }

  test("Subquery with only USE and importing WITH") {
    val query = "WITH 1 AS a CALL { USE x WITH a } RETURN a"
    expectErrorMessagesWithFeaturesFrom(Seq(UseGraphSelector), query, List(
      "Query must conclude with a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD",
    ))
  }

  test("Subquery with only MATCH") {
    val query = "WITH 1 AS a CALL { MATCH (n) } RETURN a"
    expectErrorMessagesFrom(query, List("Query cannot conclude with MATCH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)"))
  }

  private def initStartState(query: String) =
    InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)

  private def expectErrorMessagesFrom(query: String, expectedErrors: Seq[String]): Unit = {
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(expectedErrors)
  }

  private def expectErrorMessagesWithFeaturesFrom(features: Seq[SemanticFeature], query: String, expectedErrors: Seq[String]): Unit = {
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipelineWithFeatures(features).transform(startState, context)

    context.errors.map(_.msg) should equal(expectedErrors)
  }

  private def expectNotificationsFrom(query: String, expectedNotifications: Set[InternalNotification]): Unit = {
    val startState = initStartState(normalizeNewLines(query))
    val context = new ErrorCollectingContext()

    val resultState = pipeline.transform(startState, context)

    resultState.semantics().notifications should equal(expectedNotifications)
    context.errors should be(empty)
  }
}
