/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.OptionalMatchRemover.checkLabelExpression
import org.neo4j.cypher.internal.compiler.planner.logical.OptionalMatchRemover.smallestGraphIncluding
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.insertWithBetweenOptionalMatchAndMatch
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeExistsPatternExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class OptionalMatchRemoverTest extends CypherFunSuite with PlannerQueryRewriterTest with AstConstructionTestSupport
    with TestName {

  override def rewriter(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter = {
    val state = mock[LogicalPlanState]
    when(state.anonymousVariableNameGenerator).thenReturn(anonymousVariableNameGenerator)
    val plannerContext = mock[PlannerContext]
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    OptionalMatchRemover.instance(state, plannerContext)
  }

  override def rewriteAST(
    astOriginal: Statement,
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Statement = {
    val orgAstState = SemanticChecker.check(astOriginal).state
    val ast_0 = astOriginal.endoRewrite(inSequence(
      LabelExpressionPredicateNormalizer.instance,
      normalizeExistsPatternExpressions(orgAstState),
      normalizeHasLabelsAndHasType(orgAstState),
      AddUniquenessPredicates.rewriter,
      flattenBooleanOperators,
      insertWithBetweenOptionalMatchAndMatch.instance
    ))
    // computeDependenciesForExpressions needs a new run of SemanticChecker after normalizeExistsPatternExpressions
    ast_0.endoRewrite(computeDependenciesForExpressions(SemanticChecker.check(ast_0).state))
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN distinct a as a"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
         RETURN distinct a as a"""
    )
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)-[r2]-(c)
       RETURN distinct a as a"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
         RETURN distinct a as a"""
    )
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b), (a)-[r2]-(c)
       RETURN distinct a as a"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
         RETURN distinct a as a"""
    )
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]-(c)
       RETURN distinct a as a"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
         RETURN distinct a as a"""
    )
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]-(c)
       RETURN distinct a as a"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
         RETURN distinct a as a"""
    )
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        RETURN DISTINCT a as a, b as b"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        RETURN DISTINCT c as c"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (c)-[r3]->(d)
        RETURN DISTINCT d as d"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c)-[r3]->(d)
        RETURN DISTINCT d as d"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN DISTINCT d as d"""
  ) {
    assertRewrite(
      testName,
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
        RETURN count(distinct d) as x"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN count(distinct d) as x"""
    )
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d) WHERE c.prop = d.prop
        RETURN DISTINCT d as d"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d) WHERE c.prop = d.prop
        RETURN DISTINCT d as d"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a), (b)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN DISTINCT a as a, b as b"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a), (b)
         RETURN DISTINCT a as a, b as b"""
    )
  }

  test(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        RETURN DISTINCT a as a, r as r"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN count(distinct a) as x"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
         RETURN count(distinct a) as x"""
    )
  }

  test(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c) WHERE c.prop = b.prop
       RETURN DISTINCT b as b"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (f:DoesExist)
       OPTIONAL MATCH (n:DoesNotExist)
       RETURN collect(DISTINCT n.property) AS a, collect(DISTINCT f.property) AS b """
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (f:DoesExist&Foo)
       OPTIONAL MATCH (n:DoesNotExist&Foo)
       RETURN collect(DISTINCT n.property) AS a, collect(DISTINCT f.property) AS b """
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c)
          RETURN DISTINCT b as b"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b) WHERE EXISTS { (b)-[r2:T2]->(c) }
          RETURN DISTINCT b as b"""
    )
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c)-[r3:T2]->(d)
          RETURN DISTINCT b as b, c AS c"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c)-[r3:T2]->(d)
          RETURN DISTINCT b as b, d AS d"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE b:B
          RETURN DISTINCT b as b"""
  ) {
    assertRewrite(
      testName,
      """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b) WHERE b:B AND EXISTS { (b)-[r2:T2]->(c) }
          RETURN DISTINCT b as b"""
    )
  }

  test(
    """MATCH (a)
            OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c.age <> 42
            RETURN DISTINCT b as b"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c:A:B AND c.id = 42 AND c.foo = 'apa'
          RETURN DISTINCT b as b"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c:A:B AND r2.id = 42 AND r2.foo = 'apa'
          RETURN DISTINCT b as b"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a:A)
      |OPTIONAL MATCH (z)-[IS_A]->(thing) WHERE z:Z
      |RETURN a AS a, count(distinct z.key) as zCount""".stripMargin
  ) {
    assertRewrite(
      testName,
      """MATCH (a:A)
        |OPTIONAL MATCH (z) WHERE EXISTS { (z)-[IS_A]->(thing) } AND z:Z
        |RETURN a AS a, count(distinct z.key) as zCount""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r1]->(b)-[r2]->(c) WHERE a:A AND b:B AND c:C AND a <> b
          RETURN DISTINCT c as c"""
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |DELETE r
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET b.foo = 1
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET a.foo = b.foo
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET r.foo = 1
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET b:FOO
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |CREATE (c {id: b.prop})
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |CREATE (a)-[r:T]->(b)
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |MERGE (c:X {id: b.prop})
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |MERGE (a)-[r:T]->(b)
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |FOREACH( x in b.collectionProp |
      |  CREATE (z) )
      |RETURN DISTINCT a AS a""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r]->(b) WHERE a:A AND b:B
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a) WHERE a:A AND EXISTS { (a)-[r]->(b) WHERE b:B }
        |RETURN COUNT(DISTINCT a) as count
        |""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r]->(b), (a)-[r2]->(c)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE b:B AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a) WHERE EXISTS { (a)-[r:R]->(b) WHERE b:B } AND EXISTS { (a)-[r2:R2]->(c) WHERE c:C }
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE b:B|C AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a) WHERE EXISTS { (a)-[r:R]->(b) WHERE b:B|C } AND EXISTS { (a)-[r2:R2]->(c) WHERE c:C }
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE b:B&C AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a) WHERE EXISTS { (a)-[r:R]->(b) WHERE b:B&C } AND EXISTS { (a)-[r2:R2]->(c) WHERE c:C }
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE b:!(B&C) AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a) WHERE EXISTS { (a)-[r:R]->(b) WHERE b:!(B&C) } AND EXISTS { (a)-[r2:R2]->(c) WHERE c:C }
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE b:(B&C)|D AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a) WHERE EXISTS { (a)-[r:R]->(b) WHERE b:(B&C)|D } AND EXISTS { (a)-[r2:R2]->(c) WHERE c:C }
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE (b:B OR c:B) AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE (b:B OR b.prop = 42) AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a)-[r:R]->(b) WHERE EXISTS { (a)-[r2:R2]->(c) WHERE c:C } AND (b:B OR b.prop = 42)
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b), (a)-[r2:R2]->(c) WHERE (b:B OR b:C) AND c:C
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a) WHERE EXISTS { (a)-[r:R]->(b) WHERE b:B|C } AND EXISTS { (a)-[r2:R2]->(c) WHERE c:C }
        |RETURN COUNT(DISTINCT a) as count""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (b)-[r3:R3]-(d)
      |RETURN COUNT(DISTINCT a) as count""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d) WHERE b:B AND c:C AND d:D
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a)-[r:R]->(b) WHERE b:B AND EXISTS { (b)-[r2:R2]->(c) WHERE c:C } AND EXISTS { (a)-[r3:R3]-(d) WHERE d:D }
        |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)-[r3:R3]-(d), (a)-[r4:R4]-(e) WHERE b:B AND c:C AND d:D
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)-[r3:R3]-(d) WHERE b:B AND c:C AND d:D AND EXISTS { (a)-[r4:R4]-(e) }
        |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)-[r3:R3]-(d), (a)-[r4:R4]-(e)-[r5:R5]-(f) WHERE b:B AND c:C AND d:D AND f:F
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d), (b)-[r4:R4]-(e), (c)-[r5:R5]-(f)
      |WHERE a:A AND b:B AND c:C AND d:D AND e:E AND f:F
      |  AND a.prop = 0 AND b.prop = 1 AND c.prop = 2
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c)
        |WHERE a:A
        |  AND b:B
        |  AND c:C
        |  AND a.prop = 0
        |  AND b.prop = 1
        |  AND c.prop = 2
        |  // The order for the EXISTS predicates is unfortunately important, because anonymous variables get assigned in order.
        |  AND EXISTS { (a)-[r3:R3]-(d) WHERE d:D }
        |  AND EXISTS { (b)-[r4:R4]-(e) WHERE e:E }
        |  AND EXISTS { (c)-[r5:R5]-(f) WHERE f:F } 
        |        
        |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d), (b)-[r4:R4]-(e), (c)-[r5:R5]-(f)
      |WHERE a:A AND b:B AND c:C AND d:D AND e:E AND f:F
      |  AND a.prop = 0 AND b.prop = 1 AND c.prop = 2 AND d.prop = 3 AND e.prop = 4 AND f.prop = 5
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)-[r:R]->(b)-[r2:R2]->(c), (a)-[r3:R3]-(d), (b)-[r4:R4]-(e), (c)-[r5:R5]-(f)
      |WHERE a:A AND b:B AND c:C AND d:D AND e:E AND f:F
      |  AND a.prop = 0 AND b.prop = 1 AND c.prop = 2 AND d.prop = 3 AND e.prop = 4 AND f.prop = 5
      |  AND r.prop = 0 AND r2.prop = 1 AND r3.prop = 2 AND r4.prop = 3 AND r5.prop = 4
      |RETURN COUNT(DISTINCT a) as countA, COUNT(DISTINCT b) as countB, COUNT(DISTINCT c) as countC""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)
      |MATCH (a)
      |OPTIONAL MATCH (a)
      |RETURN a AS res
      |""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a)
        |MATCH (a)
        |RETURN a AS res
        |""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH (a:A&B)
      |MATCH (a:A&B)
      |OPTIONAL MATCH (a:A&B)
      |RETURN a AS res
      |""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a:A&B)
        |MATCH (a:A&B)
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
      |""".stripMargin
  ) {
    assertRewrite(
      testName,
      """OPTIONAL MATCH (a)
        |MATCH (a)
        |OPTIONAL MATCH (b)
        |OPTIONAL MATCH (c)
        |RETURN a AS res
        |""".stripMargin
    )
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a:A)-[r:REL*]->(b:B))
      |RETURN DISTINCT a AS a
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a:A)-[r:REL*]->(b:B))
      |RETURN DISTINCT p AS p
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a:A)-[r:REL*]->(b:B))
      |RETURN collect(DISTINCT a) AS result
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH p=shortestPath((a:A)-[r:REL*]->(b:B))
      |RETURN collect(DISTINCT p) AS result
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a:A) ((n)-[r:REL]->(m)){1, 10} (b:B)
      |RETURN DISTINCT a AS a
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a:A) ((n)-[r:REL]->(m)){1, 10} (b:B)
      |RETURN collect(DISTINCT a) AS result
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)
      |CREATE (b) 
      |RETURN DISTINCT 1 AS result
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  test(
    """OPTIONAL MATCH (a)
      |CREATE (b) 
      |RETURN collect(DISTINCT 1) AS result
      |""".stripMargin
  ) {
    assertIsNotRewritten(testName)
  }

  val x = v"x"
  val n = v"n"
  val m = v"m"
  val c = v"c"
  val d = v"d"
  val e = v"e"
  val r1 = v"r1"
  val r2 = v"r2"
  val r3 = v"r3"
  val r4 = v"r4"

  test("finds shortest path starting from a single element with a single node in the QG") {
    val qg = QueryGraph(patternNodes = Set(n.name))

    smallestGraphIncluding(qg, Set(n.name)) should equal(Set(n.name))
  }

  test("finds shortest path starting from a single element with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n.name, m.name))

    smallestGraphIncluding(qg, Set(n.name)) should equal(Set(n).map(_.name))
  }

  test("finds shortest path starting from two nodes with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n.name, m.name))

    smallestGraphIncluding(qg, Set(n.name, m.name)) should equal(Set(n, m, r1).map(_.name))
  }

  test("finds shortest path starting from two nodes with two relationships in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n.name, m.name, c.name))

    smallestGraphIncluding(qg, Set(n.name, m.name)) should equal(Set(n, m, r1).map(_.name))
  }

  test("finds shortest path starting from two nodes with two relationships between the same nodes in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n.name, m.name))

    val result = smallestGraphIncluding(qg, Set(n.name, m.name))
    result should contain(n.name)
    result should contain(m.name)
    result should contain.oneOf(r1.name, r2.name)
  }

  test("finds shortest path starting from two nodes with an intermediate relationship in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n.name, m.name, c.name))

    smallestGraphIncluding(qg, Set(n.name, c.name)) should equal(
      Set(n, m, c, r1, r2).map(_.name)
    )
  }

  test("find smallest graph that connect three nodes") { // MATCH (n)-[r1]-(m), (n)-[r2]->(c), (n)-[r3]->(x)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, c), BOTH, Seq.empty, SimplePatternLength)
    val pattRel3 = PatternRelationship(r3, (n, x), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2, pattRel3),
      patternNodes = Set(n.name, m.name, c.name, x.name)
    )

    smallestGraphIncluding(qg, Set(n.name, m.name, c.name)) should equal(
      Set(n, m, c, r1, r2).map(_.name)
    )
  }

  test("find smallest graph if mustInclude has the only relationship") { // MATCH (n)-[r1]-(m)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1),
      patternNodes = Set(n.name, m.name)
    )

    smallestGraphIncluding(qg, Set(n.name, r1.name)) should equal(
      Set(n, m, r1).map(_.name)
    )
  }

  test("find smallest graph if mustInclude has one of two relationships") { // MATCH (n)-[r1]-(m), (n)-[r2]->(c)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2),
      patternNodes = Set(n.name, m.name, c.name)
    )

    smallestGraphIncluding(qg, Set(n.name, r1.name)) should equal(
      Set(n, m, r1).map(_.name)
    )
  }

  test("find smallest graph if mustInclude has all but one of many relationships") { // MATCH (n)-[r1]->(m)-[r2]->(c)-[r3:R3]-(d), (n)-[r4]-(e)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val pattRel3 = PatternRelationship(r3, (c, d), BOTH, Seq.empty, SimplePatternLength)
    val pattRel4 = PatternRelationship(r4, (n, e), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2, pattRel3, pattRel4),
      patternNodes = Set(n, m, c, d, e).map(_.name)
    )

    smallestGraphIncluding(qg, Set(n, r1, m, r2, c, r3, d).map(_.name)) should equal(
      Set(n, r1, m, r2, c, r3, d).map(_.name)
    )
  }

  test("querygraphs containing only nodes") {
    val qg = QueryGraph(patternNodes = Set(n.name, m.name))

    smallestGraphIncluding(qg, Set(n.name, m.name)) should equal(Set(n, m).map(_.name))
  }

  test("checkLabelExpression should not blow up stack with lots of NOTs") {
    var x: Expression = trueLiteral
    for (_ <- 0 to 10000) x = not(x)

    checkLabelExpression(x, "variable")
  }

  test("checkLabelExpression should not blow up stack with lots of ANDs") {
    var x: Expression = trueLiteral
    for (_ <- 0 to 10000) x = ands(x, x)

    checkLabelExpression(x, "variable")
  }
}
