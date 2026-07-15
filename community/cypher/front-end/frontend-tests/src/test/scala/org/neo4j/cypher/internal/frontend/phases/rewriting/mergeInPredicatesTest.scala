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
package org.neo4j.cypher.internal.frontend.phases.rewriting

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.MergeInPredicates
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class mergeInPredicatesTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should merge INs by intersection when IN lists joined by AND") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND v IN [2, 3] AS result",
      "RETURN v IN [2] AS result"
    )
  }

  test("should merge by diff of IN list from NOT IN list joined by AND") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND NOT v IN [2, 3] AS result",
      "RETURN v IN [1] AS result"
    )
  }

  test("should merge by diff of NOT IN list from IN list joined by AND") {
    shouldRewrite(
      "RETURN NOT v IN [1, 2] AND v IN [2, 3] AS result",
      "RETURN v IN [3] AS result"
    )
  }

  test("should merge by union when IN lists joined by OR") {
    shouldRewrite(
      "RETURN v IN [1, 2] OR v IN [2, 3] AS result",
      "RETURN v IN [1, 2, 3] AS result"
    )
  }

  test("should merge by NOT diff of IN list and NOT IN list joined by OR") {
    shouldRewrite(
      "RETURN v IN [1, 2] OR NOT v IN [2, 3] AS result",
      "RETURN NOT v IN [3] AS result"
    )
  }

  test("should merge by NOT diff of NOT IN list and IN list joined by OR") {
    shouldRewrite(
      "RETURN NOT v IN [1, 2] OR v IN [2, 3] AS result",
      "RETURN NOT v IN [1] AS result"
    )
  }

  test("should merge to NOT intersection of NOT IN list and NOT IN list joined by OR") {
    shouldRewrite(
      "RETURN NOT v IN [1, 2] OR NOT v IN [2, 3] AS result",
      "RETURN NOT v IN [2] AS result"
    )
  }

  test("should merge a series of IN and NOT IN joined by AND") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND NOT a.prop IN [3,4,5] AND a.prop IN [2,3,4] RETURN *",
      "MATCH (a) WHERE a.prop IN [2] RETURN *"
    )
  }

  test("should merge (NOT mergeable) AND (NOT mergeable)") {
    shouldRewrite(
      "MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3]) AND NOT (a.prop IN [3,4] AND a.prop IN [4,5])",
      "MATCH (a) WHERE NOT a.prop IN [2,4]"
    )
  }

  test("should merge a right-skewed tree of ORs") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1] OR (v IN [2] OR (v IN [3] OR v IN [4])) AS result",
      "WITH 1 AS v RETURN v IN [1, 2, 3, 4] AS result"
    )
  }

  test("should merge a tree of ORs") {
    shouldRewrite(
      "WITH 1 AS v RETURN (v IN [1] OR (v IN [2])) OR ((v IN [3] OR v IN [4]) OR v IN [5]) AS result",
      "WITH 1 AS v RETURN v IN [1, 2, 3, 4, 5] AS result"
    )
  }

  test("should merge a right-skewed tree of ANDs") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1, 5] AND (v IN [2, 5] AND (v IN [3, 5] AND v IN [4, 5])) AS result",
      "WITH 1 AS v RETURN v IN [5] AS result"
    )
  }

  test("should merge a tree of ANDs") {
    shouldRewrite(
      """
        |WITH 1 AS v
        |RETURN (v IN [1, 'tree'] AND (v IN [2, 'tree']))
        |  AND ((v IN [3, 'tree'] AND v IN [4, 'tree']) AND v IN [5, 'tree']) AS result""".stripMargin,
      """
        |WITH 1 AS v
        |RETURN v IN ['tree'] AS result""".stripMargin
    )
  }

  test("should replace with false three non-overlapping IN lists joined by AND") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND v IN [3, 4] AND v IN [5, 6] AS result",
      "RETURN false AS result"
    )
  }

  test("should replace with true three non-overlapping NOT IN lists joined by OR") {
    shouldRewrite(
      "RETURN NOT v IN [1, 2] OR NOT v IN [3, 4] OR NOT v IN [5, 6] AS result",
      "RETURN true AS result"
    )
  }

  test("should replace with false a sequence of AND IN where non-overlapping lists for one of two predicands") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND u IN [3, 4] AND v IN [5, 6] AS result",
      "RETURN false AS result"
    )
  }

  test("should replace (NOT _ IN [] AND predicate) with (predicate) as LHS is always TRUE") {
    shouldRewrite(
      "RETURN NOT n.prop IN [] AND n.prop IS NOT NULL AS result",
      "RETURN n.prop IS NOT NULL AS result"
    )
  }

  test("should replace with true a sequence of OR NOT IN where non-overlapping lists for one of two predicands") {
    shouldRewrite(
      "RETURN NOT v IN [1, 2] OR NOT u IN [3, 4] OR NOT v IN [5, 6] AS result",
      "RETURN true AS result"
    )
  }

  test("should merge only IN comparisons for the same predicand") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1] OR (w IN [2] OR (v IN [3] OR v IN [4])) AS result",
      "WITH 1 AS v RETURN v IN [1, 3, 4] OR w IN [2] AS result"
    )
  }

  test("should merge when lhs expression is parameter") {
    shouldRewrite(
      "RETURN NOT $p IN [1, 2] AND NOT $q IN [2, 3] AND NOT $p IN [1, 3] AS result",
      "RETURN NOT $p IN [1, 2, 3] AND NOT $q IN [2, 3] AS result"
    )
  }

  test("should merge for multiple predicands") {
    shouldRewrite(
      "WITH 1 AS p, 2 AS q RETURN NOT p IN [1, 2] AND NOT q IN [2, 3] AND NOT p IN [1, 3] AND q IN [4] AS result",
      "WITH 1 AS p, 2 AS q RETURN NOT p IN [1, 2, 3] AND q IN [4] AS result"
    )
  }

  test("should not consolidate lists of integers with lists of expressions") {
    shouldRewrite(
      "WITH 1 AS v RETURN v IN [1] OR (w IN [2] OR (v IN [v IN [3]] OR v IN [4])) AS result",
      "WITH 1 AS v RETURN v IN [1, 4] OR w IN [2] OR v IN [v IN [3]] AS result"
    )
  }

  test("should collapse collection containing ConstValues for id function") {
    shouldRewrite(
      "MATCH (a) WHERE id(a) IN [42] OR id(a) IN [13] RETURN a",
      "MATCH (a) WHERE id(a) IN [42, 13] RETURN a"
    )
  }

  test("should merge two mergeable ANDs joined by OR") {
    shouldRewrite(
      "MATCH (a) WHERE (a.prop IN [1,2,3] AND a.prop IN [2,3,4]) OR (a.prop IN [2,3,4] AND a.prop IN [3,4,5]) RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3,4] RETURN *"
    )
  }

  test(
    "should merge two mergeable ORs joined by AND"
  ) {
    shouldRewrite(
      "MATCH (a) WHERE (a.prop IN [1,2,3] OR a.prop IN [2,3,4]) AND (a.prop IN [2,3,4] OR a.prop IN [3,4,5]) RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3,4] RETURN *"
    )
  }

  test("should merge AND within NOT") {
    shouldRewrite(
      "MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3])",
      "MATCH (a) WHERE NOT a.prop IN [2]"
    )
  }

  test("should merge lists for multiple properties") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND a.foo IN ['foo','bar'] AND a.prop IN [2,3,4] AND a.foo IN ['bar'] RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3] AND a.foo IN ['bar'] RETURN *"
    )
  }

  test("should merge three lists joined by OR for a property") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] OR a.prop IN [3,4,5] RETURN *",
      "MATCH (a) WHERE a.prop IN [1,2,3,4,5] RETURN *"
    )
  }

  test("should replace AND whose LHS is always true with the RHS") {
    shouldRewrite(
      "MATCH (n) RETURN (n.prop IN [1,2,3] OR TRUE) AND n.prop IN [3,4,5] AS FOO",
      "MATCH (n) RETURN n.prop IN [3,4,5] AS FOO"
    )
  }

  test("should replace OR whose LHS is always false with the RHS") {
    shouldRewrite(
      "MATCH (n) RETURN (n.prop IN [1,2,3] AND FALSE) OR n.prop IN [3,4,5] AS FOO",
      "MATCH (n) RETURN n.prop IN [3,4,5] AS FOO"
    )
  }

  test("should not merge IN that are in different any()") {
    shouldNotRewrite("MATCH (n) WHERE ANY(i IN n.prop WHERE i IN [1,2]) AND ANY(i IN n.prop WHERE i IN [3]) RETURN n")
  }

  test("should merge IN that are in the same any()") {
    shouldRewrite(
      "MATCH (n) WHERE ANY(i IN n.prop WHERE i IN [1,2] AND i IN [3]) RETURN n",
      "MATCH (n) WHERE ANY(i IN n.prop WHERE FALSE) RETURN n"
    )
  }

  test("should not rewrite OR with LHS and RHS that are empty list literals") {
    shouldNotRewrite("RETURN [] OR [] AS n")
  }

  test("should not rewrite OR with LHS and RHS that are non-empty list literals") {
    shouldNotRewrite("RETURN [1] OR [1] AS n")
  }

  test("should not rewrite AND with LHS and RHS that are empty list literals") {
    shouldNotRewrite("RETURN [] AND [] AS n")
  }

  test("should not rewrite AND with LHS and RHS that are non-empty list literals") {
    shouldNotRewrite("RETURN [1] AND [1] AS n")
  }

  test("should make list elements distinct") {
    shouldRewrite(
      "MATCH (n:Label) WHERE n.prop1 IN [43, 44] AND n.prop2 IN [3, 3]",
      "MATCH (n:Label) WHERE n.prop1 IN [43, 44] AND n.prop2 IN [3]"
    )
  }

  test("should collapse collection containing ConstValues for id function but keep unrelated list") {
    shouldRewrite(
      "MATCH (a) WHERE id(a) IN [42] OR [] OR id(a) IN [13] RETURN a",
      "MATCH (a) WHERE id(a) IN [42, 13] OR [] RETURN a"
    )
  }

  test("should not merge IN for different variables") {
    shouldNotRewrite("MATCH (a)-->(b) WHERE a.prop IN [1, 2, 3] AND b.prop IN [2, 3, 4]")
  }

  test("should merge IN comparisons across AND for same predicand and ignore unrelated comparison") {
    shouldRewrite(
      "RETURN (m.id IN [21,22,31] AND (NOT m.id IN [22,32]) AND (t.id <> 1 AND t.id <> 3)) AS result",
      "RETURN (m.id IN [21,31] AND (t.id <> 1 AND t.id <> 3)) AS result"
    )
  }

  test("should merge IN comparisons across OR for same predicand and ignore unrelated comparison") {
    shouldRewrite(
      "RETURN (m.id IN [21,22,31] OR (m.id = 'other') OR (t.id <> 1 AND t.id <> 3) OR m.id IN [1]) AS result",
      "RETURN (m.id IN [21,22,31,1] OR (m.id = 'other') OR (t.id <> 1 AND t.id <> 3)) AS result"
    )
  }

  test("should merge IN comparisons with unrelated comparisons higher in a tree of boolean binaries") {
    shouldRewrite(
      "RETURN (m.id = 'other' OR (t.id <> 1 AND (t.id <> 3 OR m.id IN [21,22,31] OR m.id IN [1]))) AS result",
      "RETURN (m.id = 'other' OR (t.id <> 1 AND (t.id <> 3 OR m.id IN [21,22,31,1]))) AS result"
    )
  }

  test("should skip merging list of same predicand if in AND with different predicand nested in series of ORs") {
    shouldRewrite(
      "WITH v IN [1, 2] OR (b IN [4] OR v IN [2, 3]) OR (c IN [5] AND NOT v IN [0, 1, 2]) OR NOT v IN [3, 4] AS result",
      "WITH NOT v IN [4] OR b IN [4] OR (c IN [5] AND NOT v IN [0, 1, 2]) AS result"
    )
  }

  test("should not merge across AND and OR when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] AND ($q IN [1.2, 1.3] OR $p IN ['b', 'd']) AS result")
  }

  test("should not merge across OR and AND when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] OR ($q IN [1.2, 1.3] AND $p IN ['b', 'd']) AS result")
  }

  test("should not merge across AND NOT when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] AND NOT ($q IN [1.2, 1.3] AND $p IN ['b', 'd']) AS result")
  }

  test("should not merge across OR NOT when mixed with comparisons with different predicands") {
    shouldNotRewrite("RETURN $p IN ['a', 'b'] OR NOT ($q IN [1.2, 1.3] OR $p IN ['b', 'd']) AS result")
  }

  test("should not merge lists with an intermediate NOT that does not immediately contain IN") {
    shouldNotRewrite(
      "RETURN NOT $p IN [1, 2] OR NOT ($q IN [3, 4] OR $p IN [2, 3])"
    )
  }

  test("should not merge IN comparisons across QPP boundary") {
    shouldRewrite(
      "MATCH (a WHERE v IN ['a', 'b'] AND NOT v IN ['b']) ((l)-->(r) WHERE v IN [4.5, 10.1] AND NOT v IN [10.01])+",
      "MATCH (a WHERE v IN ['a']) ((l)-->(r) WHERE v IN [4.5, 10.1])+"
    )
  }

  test("should not merge IN comparisons across EXISTS subquery boundary") {
    shouldRewrite(
      "MATCH (a) WHERE v IN ['a', 'b'] AND NOT v IN ['b'] AND EXISTS { MATCH (a)-->(b) WHERE v IN ['c'] }",
      "MATCH (a) WHERE v IN ['a'] AND EXISTS { MATCH (a)-->(b) WHERE v IN ['c'] }"
    )
  }

  test("should not merge IN comparisons across pattern comprehension boundary") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND NOT v IN [2, 3] AND size([v IN $p WHERE v IN [1]]) > 0 AS result",
      "RETURN v IN [1] AND size([v IN $p WHERE v IN [1]]) > 0 AS result"
    )
  }

  test("should not collapse collections containing function invocations") {
    shouldNotRewrite("MATCH (a) WHERE id(a) IN [42] OR id(a) IN [rand()] RETURN a")
  }

  test("should not collapse non-equal collections containing function invocations") {
    shouldNotRewrite("MATCH (a) WHERE a.prop IN [42, rand()] OR a.prop IN [rand(), 54] RETURN a")
  }

  test("should collapse equal collections containing function invocations") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1, rand(), 'string'] OR a.prop IN [1, rand(), 'string'] RETURN a",
      "MATCH (a) WHERE a.prop IN [1, rand(), 'string'] RETURN a"
    )
  }

  test("should not merge IN lists containing properties") {
    shouldNotRewrite(
      "MATCH (a WHERE v IN [a.p] AND NOT v IN [a.q]) ((l)-->(r) WHERE v IN [l.p, r.p] AND NOT v IN [l.q, r.q])+"
    )
  }

  test("should not merge lists containing variables") {
    shouldNotRewrite("RETURN v IN [1, 2] AND v IN [2, a] AS result")
  }

  test("should not merge lists containing parameters") {
    shouldRewrite(
      "RETURN v IN [1, 2] AND NOT v IN [3, $x] AND NOT v IN [2, 3] AS result",
      "RETURN v IN [1] AND NOT v IN [3, $x] AS result"
    )
  }

  test("should not merge lists containing date functions") {
    shouldNotRewrite(
      "RETURN NOT v IN [date('2025-02-02'), 2] AND v IN [2, 3] AS result"
    )
  }

  private def shouldRewrite(from: String, to: String): Unit = {
    val exceptionFactory = Neo4jCypherExceptionFactory(from, None)
    val original = parse(from, exceptionFactory).asInstanceOf[Query]
    val expected = parse(to, exceptionFactory).asInstanceOf[Query]
    val common: Rewriter = flattenBooleanOperators.instance(CancellationChecker.NeverCancelled)
    val result = MergeInPredicates.instance(original)

    common(result) should equal(common(expected))
  }

  private def shouldNotRewrite(query: String): Unit = shouldRewrite(query, query)

}
