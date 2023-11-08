/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.StatementConverters
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.OptionalMatchRemover.smallestGraphIncluding
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.insertWithBetweenOptionalMatchAndMatch
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.recordScopes
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class OptionalMatchRemoverTest extends CypherFunSuite with LogicalPlanningTestSupport2 with TestName {

  private def rewriter(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter = {
    val state = mock[LogicalPlanState]
    when(state.anonymousVariableNameGenerator).thenReturn(anonymousVariableNameGenerator)
    val plannerContext = mock[PlannerContext]
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    OptionalMatchRemover.instance(state, plannerContext)
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN distinct a as a""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
         RETURN distinct a as a""")
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)-[r2]-(c)
       RETURN distinct a as a""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
         RETURN distinct a as a""")
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b), (a)-[r2]-(c)
       RETURN distinct a as a""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
         RETURN distinct a as a""")
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]-(c)
       RETURN distinct a as a""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
         RETURN distinct a as a""")
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]-(c)
       RETURN distinct a as a""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
         RETURN distinct a as a""")
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        RETURN DISTINCT a as a, b as b""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        RETURN DISTINCT c as c""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (c)-[r3]->(d)
        RETURN DISTINCT d as d""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c)-[r3]->(d)
        RETURN DISTINCT d as d""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN DISTINCT d as d""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN DISTINCT d as d"""
    )
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN count(distinct d) as x""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN count(distinct d) as x""")
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d) WHERE c.prop = d.prop
        RETURN DISTINCT d as d""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d) WHERE c.prop = d.prop
        RETURN DISTINCT d as d""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a), (b)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN DISTINCT a as a, b as b""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a), (b)
         RETURN DISTINCT a as a, b as b""")
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        RETURN DISTINCT a as a, r as r""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN count(distinct a) as x""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
         RETURN count(distinct a) as x""")
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c) WHERE c.prop = b.prop
       RETURN DISTINCT b as b""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (f:DoesExist)
       OPTIONAL MATCH (n:DoesNotExist)
       RETURN collect(DISTINCT n.property) AS a, collect(DISTINCT f.property) AS b """) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c)
          RETURN DISTINCT b as b""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b) WHERE (b)-[`  UNNAMED0`:T2]->(`  UNNAMED1`)
          RETURN DISTINCT b as b""")
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c)-[r3:T2]->(d)
          RETURN DISTINCT b as b, c AS c""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c)-[r3:T2]->(d)
          RETURN DISTINCT b as b, d AS d""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE b:B
          RETURN DISTINCT b as b""") {
    assert_that(testName).is_rewritten_to(
      """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b) WHERE b:B AND (b)-[`  UNNAMED0`:T2]->(`  UNNAMED1`)
          RETURN DISTINCT b as b""")
  }

  test(
    """MATCH (a)
            OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c.age <> 42
            RETURN DISTINCT b as b""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c:A:B AND c.id = 42 AND c.foo = 'apa'
          RETURN DISTINCT b as b""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c:A:B AND r2.id = 42 AND r2.foo = 'apa'
          RETURN DISTINCT b as b""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a:A)
      |OPTIONAL MATCH (z)-[IS_A]->(thing) WHERE z:Z
      |RETURN a AS a, count(distinct z.key) as zCount""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """MATCH (a:A)
        |OPTIONAL MATCH (z) WHERE (z)-[`  UNNAMED0`]->(`  UNNAMED1`) AND z:Z
        |RETURN a AS a, count(distinct z.key) as zCount""".stripMargin)
  }

  test(
    """OPTIONAL MATCH (a)-[r1]->(b)-[r2]->(c) WHERE a:A AND b:B AND c:C AND a <> b
          RETURN DISTINCT c as c""") {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |DELETE r
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET b.foo = 1
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET a.foo = b.foo
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET r.foo = 1
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET b:FOO
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |CREATE (c {id: b.prop})
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |CREATE (a)-[r:T]->(b)
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |MERGE (c:X {id: b.prop})
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |MERGE (a)-[r:T]->(b)
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |FOREACH( x in b.collectionProp |
      |  CREATE (z) )
      |RETURN DISTINCT a AS a""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)-[r]->(b) WHERE a:A AND b:B
      |RETURN COUNT(DISTINCT a) as count""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """OPTIONAL MATCH (a) WHERE a:A AND (a)-[`  UNNAMED0`]->(`  UNNAMED1`:B)
        |RETURN COUNT(DISTINCT a) as count
        |""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)-[r]->(b), (a)-[r2]->(c)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE b:B AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """OPTIONAL MATCH (a) WHERE (a)-[`  UNNAMED0`:R]->(`  UNNAMED1`:B) AND (a)-[`  UNNAMED4`:R2]->(`  UNNAMED5`:C)
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (b)-[r3:R3]-(d)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d) WHERE b:B AND c:C AND d:D
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """OPTIONAL MATCH (a)-[r:R]->(b) WHERE b:B AND (b)-[`  UNNAMED0`:R2]->(`  UNNAMED1`:C) AND (a)-[`  UNNAMED4`:R3]-(`  UNNAMED5`:D)
        |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)-[r3:R3]-(d), (a)-[r4:R4]-(e) WHERE b:B AND c:C AND d:D
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)-[r3:R3]-(d) WHERE b:B AND c:C AND d:D AND (a)-[`  UNNAMED0`:R4]-(`  UNNAMED1`)
        |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)-[r3:R3]-(d), (a)-[r4:R4]-(e)-[r5:R5]-(f) WHERE b:B AND c:C AND d:D AND f:F
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d), (b)-[r4:R4]-(e), (c)-[r5:R5]-(f)
      |WHERE a:A AND b:B AND c:C AND d:D AND e:E AND f:F
      |  AND a.prop = 0 AND b.prop = 1 AND c.prop = 2
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)
        |WHERE a:A AND b:B AND c:C
        | AND a.prop = 0 AND b.prop = 1 AND c.prop = 2
        | AND (a)-[`  UNNAMED0`:R3]-(`  UNNAMED1`:D)
        | AND (b)-[`  UNNAMED4`:R4]-(`  UNNAMED5`:E)
        | AND (c)-[`  UNNAMED8`:R5]-(`  UNNAMED9`:F)
        |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d), (b)-[r4:R4]-(e), (c)-[r5:R5]-(f)
      |WHERE a:A AND b:B AND c:C AND d:D AND e:E AND f:F
      |  AND a.prop = 0 AND b.prop = 1 AND c.prop = 2 AND d.prop = 3 AND e.prop = 4 AND f.prop = 5
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d), (b)-[r4:R4]-(e), (c)-[r5:R5]-(f)
      |WHERE a:A AND b:B AND c:C AND d:D AND e:E AND f:F
      |  AND a.prop = 0 AND b.prop = 1 AND c.prop = 2 AND d.prop = 3 AND e.prop = 4 AND f.prop = 5
      |  AND r.prop = 0 AND r2.prop = 1 AND r3.prop = 2 AND r4.prop = 3 AND r5.prop = 4
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)
      |MATCH (a)
      |OPTIONAL MATCH (a)
      |RETURN a AS res
      |""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """OPTIONAL MATCH (a)
        |MATCH (a)
        |RETURN a AS res
        |""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)
      |MATCH (a)
      |OPTIONAL MATCH (b)
      |OPTIONAL MATCH (a)
      |OPTIONAL MATCH (c)
      |RETURN a AS res
      |""".stripMargin) {
    assert_that(testName).is_rewritten_to(
      """OPTIONAL MATCH (a)
        |MATCH (a)
        |OPTIONAL MATCH (b)
        |OPTIONAL MATCH (c)
        |RETURN a AS res
        |""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a)-[r:REL*]->(b))
      |RETURN DISTINCT a AS a
      |""".stripMargin
  ) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a)-[r:REL*]->(b))
      |RETURN DISTINCT p AS p
      |""".stripMargin
  ) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a)-[r:REL*]->(b))
      |RETURN collect(DISTINCT a) AS result
      |""".stripMargin
  ) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a)-[r:REL*]->(b))
      |RETURN collect(DISTINCT p) AS result
      |""".stripMargin
  ) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)
      |CREATE (b) 
      |RETURN DISTINCT 1 AS result
      |""".stripMargin
  ) {
    assert_that(testName).is_not_rewritten()
  }

  test(
    """OPTIONAL MATCH (a)
      |CREATE (b) 
      |RETURN collect(DISTINCT 1) AS result
      |""".stripMargin
  ) {
    assert_that(testName).is_not_rewritten()
  }

  val x = "x"
  val n = "n"
  val m = "m"
  val c = "c"
  val d = "d"
  val e = "e"
  val r1 = "r1"
  val r2 = "r2"
  val r3 = "r3"
  val r4 = "r4"

  test("finds shortest path starting from a single element with a single node in the QG") {
    val qg = QueryGraph(patternNodes = Set(n))

    smallestGraphIncluding(qg, Set(n)) should equal(Set(n))
  }

  test("finds shortest path starting from a single element with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n, m))

    smallestGraphIncluding(qg, Set(n)) should equal(Set(n))
  }

  test("finds shortest path starting from two nodes with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n, m))

    smallestGraphIncluding(qg, Set(n, m)) should equal(Set(n, m, r1))
  }

  test("finds shortest path starting from two nodes with two relationships in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m, c))

    smallestGraphIncluding(qg, Set(n, m)) should equal(Set(n, m, r1))
  }

  test("finds shortest path starting from two nodes with two relationships between the same nodes in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m))

    val result = smallestGraphIncluding(qg, Set(n, m))
    result should contain(n)
    result should contain(m)
    result should contain oneOf(r1, r2)
  }

  test("finds shortest path starting from two nodes with an intermediate relationship in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m, c))

    smallestGraphIncluding(qg, Set(n, c)) should equal(
      Set(n, m, c, r1, r2))
  }

  test("find smallest graph that connect three nodes") { // MATCH (n)-[r1]-(m), (n)-[r2]->(c), (n)-[r3]->(x)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, c), BOTH, Seq.empty, SimplePatternLength)
    val pattRel3 = PatternRelationship(r3, (n, x), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2, pattRel3),
      patternNodes = Set(n, m, c, x))

    smallestGraphIncluding(qg, Set(n, m, c)) should equal(
      Set(n, m, c, r1, r2))
  }

  test("find smallest graph if mustInclude has the only relationship") { // MATCH (n)-[r1]-(m)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1),
      patternNodes = Set(n, m))

    smallestGraphIncluding(qg, Set(n, r1)) should equal(
      Set(n, m, r1))
  }

  test("find smallest graph if mustInclude has one of two relationships") { // MATCH (n)-[r1]-(m), (n)-[r2]->(c)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2),
      patternNodes = Set(n, m, c))

    smallestGraphIncluding(qg, Set(n, r1)) should equal(
      Set(n, m, r1))
  }

  test("find smallest graph if mustInclude has all but one of many relationships") { // MATCH (n)-[r1]->(m)-[r2]->(c)-[r3:R3]-(d), (n)-[r4]-(e)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val pattRel3 = PatternRelationship(r3, (c, d), BOTH, Seq.empty, SimplePatternLength)
    val pattRel4 = PatternRelationship(r4, (n, e), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2, pattRel3, pattRel4),
      patternNodes = Set(n, m, c, d, e))

    smallestGraphIncluding(qg, Set(n, r1, m, r2, c, r3, d)) should equal(
      Set(n, r1, m, r2, c, r3, d))
  }

  test("querygraphs containing only nodes") {
    val qg = QueryGraph(patternNodes = Set(n, m))

    smallestGraphIncluding(qg, Set(n, m)) should equal(Set(n, m))
  }

  case class RewriteTester(originalQuery: String) {
    def is_rewritten_to(newQuery: String): Unit = {
      val expectedGen = new AnonymousVariableNameGenerator()
      val actualGen = new AnonymousVariableNameGenerator()
      val expected = removeGeneratedNamesAndParamsOnTree(getTheWholePlannerQueryFrom(newQuery.stripMargin, expectedGen))
      val original = getTheWholePlannerQueryFrom(originalQuery.stripMargin, actualGen)

      val result = removeGeneratedNamesAndParamsOnTree(original.endoRewrite(fixedPoint(rewriter(actualGen))))
      assert(result === expected, "\nWas not rewritten correctly\n" + originalQuery)
    }

    def is_not_rewritten(): Unit = {
      val actualGen = new AnonymousVariableNameGenerator()
      val query = getTheWholePlannerQueryFrom(originalQuery.stripMargin, actualGen)
      val result = query.endoRewrite(fixedPoint(rewriter(actualGen)))
      assert(result === query, "\nShould not have been rewritten\n" + originalQuery)
    }
  }

  private def assert_that(originalQuery: String): RewriteTester = RewriteTester(originalQuery)

  private def getTheWholePlannerQueryFrom(query: String, anonymousVariableNameGenerator: AnonymousVariableNameGenerator): PlannerQuery = {
    val astOriginal = parseForRewriting(query)
    val orgAstState = SemanticChecker.check(astOriginal).state
    val ast = astOriginal.endoRewrite(inSequence(
      normalizeHasLabelsAndHasType(orgAstState),
      AddUniquenessPredicates(anonymousVariableNameGenerator),
      flattenBooleanOperators,
      insertWithBetweenOptionalMatchAndMatch,
      recordScopes(orgAstState)
    ))
    val exceptionFactory = Neo4jCypherExceptionFactory(query, Some(DummyPosition(0)))
    val onError = SyntaxExceptionCreator.throwOnError(exceptionFactory)
    val result = SemanticChecker.check(ast)
    onError(result.errors)
    val table = SemanticTable(types = result.state.typeTable, recordedScopes = result.state.recordedScopes.mapValues(_.scope))
    StatementConverters.toPlannerQuery(
      ast.asInstanceOf[Query],
      table,
      anonymousVariableNameGenerator,
      CancellationChecker.NeverCancelled,
      nonTerminating = false
    )
  }

  private def parseForRewriting(queryText: String) = parser.parse(queryText.replace("\r\n", "\n"), Neo4jCypherExceptionFactory(queryText, None), new AnonymousVariableNameGenerator)
}
