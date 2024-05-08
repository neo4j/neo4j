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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.normalizePredicates
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class normalizePredicatesTest extends CypherFunSuite with TestName {

  private val prettifier = Prettifier(ExpressionStringifier(_.asCanonicalStringVal))

  def rewriter(semanticState: SemanticState): Rewriter = {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    inSequence(
      LabelExpressionPredicateNormalizer.instance,
      nameAllPatternElements(anonVarNameGen),
      normalizePredicates.getRewriter(
        semanticState,
        Map.empty,
        OpenCypherExceptionFactory(None),
        new AnonymousVariableNameGenerator,
        CancellationChecker.neverCancelled()
      ),
      normalizeHasLabelsAndHasType(semanticState)
    )
  }

  def rewriterWithoutNormalizeMatchPredicates(semanticState: SemanticState): Rewriter = {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    inSequence(
      LabelExpressionPredicateNormalizer.instance,
      nameAllPatternElements(anonVarNameGen),
      normalizeHasLabelsAndHasType(semanticState)
    )
  }

  def parseForRewriting(queryText: String): Statement = JavaCCParser.parse(
    queryText.replace("\r\n", "\n"),
    OpenCypherExceptionFactory(None)
  )

  private def assertRewrite(expectedQuery: String): Unit = {
    def rewrite(query: String, rewriter: SemanticState => Rewriter): Statement = {
      val ast = parseForRewriting(query)
      ast.endoRewrite(rewriter(SemanticChecker.check(ast).state))
    }

    val result = rewrite(testName, rewriter)
    // For the test sake we need to rewrite away HasLabelsOrTypes to HasLabels also on the expected
    val expectedResult = rewrite(expectedQuery, rewriterWithoutNormalizeMatchPredicates)
    assert(
      result === expectedResult,
      s"\n$testName\nshould be rewritten to:\n${prettifier.asString(expectedResult)}\nbut was rewritten to:${prettifier.asString(result)}"
    )
  }

  test("MATCH (n {foo: 'bar'}) RETURN n") {
    assertRewrite("MATCH (n) WHERE n.foo = 'bar' RETURN n")
  }

  test("MATCH (n)-[r:Foo {foo: 1}]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 RETURN n")
  }

  test("MATCH (n {foo: 'bar', bar: 4}) RETURN n") {
    assertRewrite("MATCH (n) WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")
  }

  test("MATCH (n)-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("MATCH (n { }) RETURN n") {
    assertRewrite("MATCH (n) RETURN n")
  }

  test("MATCH (n)-[r:Foo { }]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() RETURN n")
  }

  test("MATCH (n { })-[r:Foo { }]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() RETURN n")
  }

  test("MATCH (n { })-[r:Foo { }]->() WHERE n.baz = true OR r.baz = false RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE n.baz = true OR r.baz = false RETURN n")
  }

  test("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() WHERE n.baz = true OR r.baz = false RETURN n") {
    assertRewrite(
      "MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' AND (n.baz = true OR r.baz = false) RETURN n"
    )
  }

  test("MATCH ({foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN r") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"
    assertRewrite(
      s"MATCH ($node1)-[r:Foo]->($node2) WHERE $node1.foo = 'bar' AND $node1.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN r"
    )
  }

  test("MATCH (n {foo: 'bar', bar: 4})-[:Foo {foo: 1, bar: 'baz'}]->() RETURN n") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val rel = s"`${anonVarNameGen.nextName}`"
    val node = s"`${anonVarNameGen.nextName}`"
    assertRewrite(
      s"MATCH (n)-[$rel:Foo]->($node) WHERE n.foo = 'bar' AND n.bar = 4 AND $rel.foo = 1 AND $rel.bar = 'baz' RETURN n"
    )
  }

  test("MATCH (n:LABEL) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:LABEL RETURN n")
  }

  test("MATCH (n:L1:L2) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:L1:L2 RETURN n")
  }

  test("MATCH (n:Foo)-[r]->(b) RETURN n") {
    assertRewrite("MATCH (n)-[r]->(b) WHERE n:Foo RETURN n")
  }

  test("MATCH (n)-[r]->(b:Foo) RETURN n") {
    assertRewrite("MATCH (n)-[r]->(b) WHERE b:Foo RETURN n")
  }

  test("MATCH (a:A {foo:'v1', bar:'v2'})-[r:R {baz: 'v1'}]->(b:B {foo:'v2', baz:'v2'}) RETURN *") {
    assertRewrite(
      "MATCH (a)-[r:R]->(b) WHERE a.foo = 'v1' AND a.bar = 'v2' AND a:A AND r.baz = 'v1' AND b.foo = 'v2' AND b.baz = 'v2' AND b:B RETURN *"
    )
  }

  test("MATCH (n)-[r* {prop: 42}]->(b) RETURN n") {
    assertRewrite("MATCH (n)-[r*]->(b) WHERE ALL(`  UNNAMED0` in r where `  UNNAMED0`.prop = 42) RETURN n")
  }

  test("MATCH (n)-[r* {prop: 42, p: 'aaa'}]->(b) RETURN n") {
    assertRewrite(
      "MATCH (n)-[r*]->(b) WHERE ALL(`  UNNAMED0` in r where `  UNNAMED0`.prop = 42 AND `  UNNAMED0`.p = 'aaa') RETURN n"
    )
  }

  // Empty parameter maps should not pose a problem but should simply be removed
  test("MATCH (n {})-[r* {}]->(b) RETURN n") {
    assertRewrite(
      "MATCH (n)-[r*]->(b) RETURN n"
    )
  }

  test("MATCH (a:Artist)-[r:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *") {
    assertRewrite(
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND ALL(`  UNNAMED0` in r where `  UNNAMED0`.year = 1988) AND b:Artist  RETURN *"
    )
  }

  test("MATCH (a:Artist)-[r:WORKED_WITH* { year: $foo }]->(b:Artist) RETURN *") {
    assertRewrite(
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND ALL(`  UNNAMED0` in r where `  UNNAMED0`.year = $foo)  AND b:Artist RETURN *"
    )
  }

  test("MATCH (n WHERE n.prop > 123) RETURN n") {
    assertRewrite("MATCH (n) WHERE n.prop > 123 RETURN n")
  }

  test("MATCH (n WHERE n.prop > 123)-->(m WHERE m.prop < 42 AND m.otherProp = 'hello') RETURN n") {
    assertRewrite("MATCH (n)-->(m) WHERE n.prop > 123 AND (m.prop < 42 AND m.otherProp = 'hello') RETURN n")
  }

  test(
    "MATCH (n WHERE n.prop > 123)-->(m WHERE m.prop < 42 AND m.otherProp = 'hello') WHERE n.prop <> m.prop RETURN n"
  ) {
    assertRewrite(
      "MATCH (n)-->(m) WHERE n.prop > 123 AND (m.prop < 42 AND m.otherProp = 'hello') AND n.prop <> m.prop RETURN n"
    )
  }

  test("MATCH (n)-[r WHERE r.prop > 123]->() RETURN r") {
    assertRewrite("MATCH (n)-[r]->() WHERE r.prop > 123 RETURN r")
  }

  test("MATCH (n)-[r WHERE r.prop > 123]->()-[s WHERE s.otherProp = \"ok\"]-() RETURN r") {
    assertRewrite(
      "MATCH (n)-[r]->()-[s]-() WHERE r.prop > 123 AND s.otherProp = \"ok\" RETURN r"
    )
  }

  test("MATCH (n:A&B) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:A AND n:B RETURN n")
  }

  test("MATCH (n:A|B) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:A OR n:B RETURN n")
  }

  test("MATCH (n:!A) RETURN n") {
    assertRewrite("MATCH (n) WHERE NOT n:A RETURN n")
  }

  test("MATCH (n:A&(B|C)) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:A AND (n:B OR n:C) RETURN n")
  }

  test("MATCH (n:%&(B|!%)) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:% AND (n:B OR NOT(n:%)) RETURN n")
  }

  test("MATCH ()-[r:!A&!B]->() RETURN r") {
    assertRewrite("MATCH ()-[r]->() WHERE NOT(r:A) AND NOT(r:B) RETURN r")
  }

  test("MATCH ()-[r:A|B|C]->() RETURN r") {
    assertRewrite("MATCH ()-[r:A|B|C]->() RETURN r")
  }

  test("MATCH () ((n {foo: 'bar'})--())+ () RETURN n") {
    assertRewrite("MATCH () ((n)--() WHERE n.foo = 'bar')+ () RETURN n")
  }

  test("MATCH () (()-[r {foo: 'bar'}]-())+ () RETURN n") {
    assertRewrite("MATCH () (()-[r]-() WHERE r.foo = 'bar')+ () RETURN n")
  }

  test("MATCH () ((n:N)-[r:REL]-(m:M))+ () RETURN n") {
    assertRewrite("MATCH () ((n)-[r:REL]-(m) WHERE n:N AND m:M)+ () RETURN n")
  }

  test("MATCH () ((n:N&!M)-[r:!REL]-())+ () RETURN n") {
    assertRewrite("MATCH () ((n)-[r]-() WHERE n:N AND not n:M AND not r:REL)+ () RETURN n")
  }

  test("MATCH () ((n WHERE n.foo = 'bar')--())+ () RETURN n") {
    assertRewrite("MATCH () ((n)--() WHERE n.foo = 'bar')+ () RETURN n")
  }

  test("MATCH () (()-[r WHERE r.foo = 'bar']-())+ () RETURN n") {
    assertRewrite("MATCH () (()-[r]-() WHERE r.foo = 'bar')+ () RETURN n")
  }

  test("MATCH (a:A) ((n {foo: 'bar'})--())+ () RETURN n") {
    assertRewrite("MATCH (a) ((n)--() WHERE n.foo = 'bar')+ () WHERE a:A RETURN n")
  }

  // should move pattern predicates out of node/rel in subclause

  test("MATCH (n) WHERE EXISTS {MATCH (n WHERE n.prop = 1)} RETURN *") {
    assertRewrite("MATCH (n) WHERE EXISTS { MATCH (n) WHERE n.prop = 1} RETURN *")
  }

  test("MATCH (n) WHERE COUNT {MATCH (n WHERE n.prop = 1)} > 1 RETURN *") {
    assertRewrite("MATCH (n) WHERE COUNT { MATCH (n) WHERE n.prop = 1} > 1 RETURN *")
  }

  test("MATCH (n) WHERE EXISTS {MATCH (n)-[r WHERE r.prop = 1]->()} RETURN *") {
    assertRewrite("MATCH (n) WHERE EXISTS { MATCH (n)-[r]->() WHERE r.prop = 1} RETURN *")
  }

  test("MATCH (n) WHERE COUNT {MATCH (n)-[r WHERE r.prop = 1]->()} > 1 RETURN *") {
    assertRewrite("MATCH (n) WHERE COUNT { MATCH (n)-[r]->() WHERE r.prop = 1} > 1 RETURN *")
  }

  // should move pattern predicates out of node/rel in subclause without MATCH keyword

  test("MATCH (n) WHERE EXISTS {(n WHERE n.prop = 1)} RETURN *") {
    assertRewrite("MATCH (n) WHERE EXISTS {(n) WHERE n.prop = 1} RETURN *")
  }

  test("MATCH (n) WHERE COUNT {(n WHERE n.prop = 1)} > 1 RETURN *") {
    assertRewrite("MATCH (n) WHERE COUNT {(n) WHERE n.prop = 1} > 1 RETURN *")
  }

  test("MATCH (n) WHERE EXISTS {(n)-[r WHERE r.prop = 1]->()} RETURN *") {
    assertRewrite("MATCH (n) WHERE EXISTS {(n)-[r]->() WHERE r.prop = 1} RETURN *")
  }

  test("MATCH (n) WHERE COUNT {(n)-[r WHERE r.prop = 1]->()} > 1 RETURN *") {
    assertRewrite("MATCH (n) WHERE COUNT {(n)-[r]->() WHERE r.prop = 1} > 1 RETURN *")
  }

  // should move pattern predicates to correct scope for subclauses

  test(
    """
      |MATCH (a
      |  WHERE EXISTS {
      |    MATCH (n WHERE n.prop = a.prop)-[r WHERE r.prop = 42]->()
      |  }
      |)
      |RETURN *
      |""".stripMargin
  ) {

    val rewritten =
      s"""
         |MATCH (a)
         |WHERE EXISTS {
         | MATCH (n)-[r]->()
         | WHERE n.prop = a.prop AND r.prop = 42
         |}
         |RETURN *
         | """.stripMargin

    assertRewrite(rewritten)
  }

  test(
    """
      |MATCH (a
      |  WHERE COUNT {
      |    MATCH (n WHERE n.prop = a.prop)-[r WHERE r.prop = 42]->()
      |  } > 1
      |)
      |RETURN *
      |""".stripMargin
  ) {

    val rewritten =
      s"""
         |MATCH (a)
         |WHERE COUNT {
         | MATCH (n)-[r]->()
         | WHERE n.prop = a.prop AND r.prop = 42
         |} > 1
         |RETURN *
         | """.stripMargin

    assertRewrite(rewritten)
  }

  test(
    """
      |MATCH (a
      | WHERE EXISTS {
      |   MATCH (n WHERE n.prop = a.prop)-[r]->()
      | } AND EXISTS {
      |   MATCH ()-[r WHERE r.prop = 1]->()
      | }
      |)
      |RETURN *
      |""".stripMargin
  ) {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"
    val node3 = s"`${anonVarNameGen.nextName}`"

    val rewritten =
      s"""
         |MATCH (a)
         |WHERE EXISTS {
         | MATCH (n)-[r]->($node1)
         | WHERE n.prop = a.prop
         |} AND EXISTS {
         | MATCH ($node2)-[r]->($node3)
         | WHERE r.prop = 1
         |}
         |RETURN *
         | """.stripMargin

    assertRewrite(rewritten)
  }

  test(
    """
      |MATCH (a
      | WHERE COUNT {
      |   MATCH (n WHERE n.prop = a.prop)-[r]->()
      | } > 1 AND COUNT {
      |   MATCH ()-[r WHERE r.prop = 1]->()
      | } > 1
      |)
      |RETURN *
      |""".stripMargin
  ) {

    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"
    val node3 = s"`${anonVarNameGen.nextName}`"

    val rewritten =
      s"""
         |MATCH (a)
         |WHERE COUNT {
         | MATCH (n)-[r]->($node1)
         | WHERE n.prop = a.prop
         |} > 1 AND COUNT {
         | MATCH ($node2)-[r]->($node3)
         | WHERE r.prop = 1
         |} > 1
         |RETURN *
         | """.stripMargin

    assertRewrite(rewritten)
  }

  test(
    """
      |MATCH (a
      |  WHERE EXISTS {
      |    MATCH (n WHERE EXISTS {
      |      MATCH (n WHERE n.prop = 1)-[r WHERE r.prop = a.prop]->()
      |    } XOR true)
      |  }
      |)
      |RETURN *
      |""".stripMargin
  ) {

    val rewritten =
      s"""
         |MATCH (a)
         |WHERE EXISTS {
         | MATCH (n)
         | WHERE EXISTS {
         |   MATCH (n)-[r]->()
         |   WHERE n.prop = 1 AND r.prop = a.prop
         | } XOR true
         |}
         |RETURN *
         |""".stripMargin

    assertRewrite(rewritten)
  }

  test(
    """
      |MATCH (a
      |  WHERE COUNT {
      |    MATCH (n WHERE COUNT {
      |      MATCH (n WHERE n.prop = 1)-[r WHERE r.prop = a.prop]->()
      |    } > 1 XOR true)
      |  } > 1
      |)
      |RETURN *
      |""".stripMargin
  ) {

    val rewritten =
      s"""
         |MATCH (a)
         |WHERE COUNT {
         | MATCH (n)
         | WHERE COUNT {
         |   MATCH (n)-[r]->()
         |   WHERE n.prop = 1 AND r.prop = a.prop
         | } > 1 XOR true
         |} > 1
         |RETURN *
         |""".stripMargin

    assertRewrite(rewritten)
  }

  // should move label expressions to correct scope for subclauses

  test("MATCH (a WHERE EXISTS {(:A)}) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    assertRewrite(s"MATCH (a) WHERE EXISTS {($node) WHERE $node:A} RETURN *")
  }

  test("MATCH (a WHERE COUNT {(:A)} > 1) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    assertRewrite(s"MATCH (a) WHERE COUNT {($node) WHERE $node:A} > 1 RETURN *")
  }

  test("MATCH (n WHERE EXISTS {MATCH (:A)-[]->(b:B)}) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n) WHERE EXISTS {MATCH ($node)-[$rel]->(b) WHERE $node:A AND b:B} RETURN *")
  }

  test("MATCH (n WHERE COUNT {MATCH (:A)-[]->(b:B)} > 1) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n) WHERE COUNT {MATCH ($node)-[$rel]->(b) WHERE $node:A AND b:B} > 1 RETURN *")
  }

  test("MATCH (a)-[r WHERE EXISTS {(:A)}]-(b) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    assertRewrite(s"MATCH (a)-[r]-(b) WHERE EXISTS {($node) WHERE $node:A} RETURN *")
  }

  test("MATCH (a)-[r WHERE COUNT {(:A)-[s]-(c)} > 1]->(b) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    assertRewrite(s"MATCH (a)-[r]->(b) WHERE COUNT {($node)-[s]-(c) WHERE $node:A} > 1 RETURN *")
  }

  test("MATCH (n)-[r WHERE EXISTS {MATCH (:A)-[]->(b:B)}]->(m) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n)-[r]->(m) WHERE EXISTS {MATCH ($node)-[$rel]->(b) WHERE $node:A AND b:B} RETURN *")
  }

  test("MATCH (n)-[r WHERE COUNT {MATCH (:A)-[]->(b:B)} > 1]->(m) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n)-[r]->(m) WHERE COUNT {MATCH ($node)-[$rel]->(b) WHERE $node:A AND b:B} > 1 RETURN *")
  }

  test("MATCH (a WHERE COUNT { (n)-->() WHERE n.prop > 0 } > 1) RETURN *") {
    assertRewrite("MATCH (a) WHERE COUNT { (n)-->() WHERE n.prop > 0 } > 1 RETURN *")
  }

  test("MATCH (a WHERE COUNT { (n:L)-->() WHERE n.prop > 0 } > 1) RETURN *") {
    assertRewrite("MATCH (a) WHERE COUNT { (n)-->() WHERE n:L AND n.prop > 0 } > 1 RETURN *")
  }

  test("MATCH (a { p: COUNT { (n)-->() WHERE n.prop > 0 } } ) RETURN *") {
    assertRewrite("MATCH (a) WHERE a.p = COUNT { (n)-->() WHERE n.prop > 0 } RETURN *")
  }

  test("MATCH (a { p: COUNT { (n {prop:42})-->() } } ) RETURN *") {
    assertRewrite("MATCH (a) WHERE a.p = COUNT { (n)-->() WHERE n.prop = 42 } RETURN *")
  }

  test("MATCH (a { p: COUNT { (n:L)-->() WHERE n.prop > 0 } } ) RETURN *") {
    assertRewrite("MATCH (a) WHERE a.p = COUNT { (n)-->() WHERE n:L AND n.prop > 0 } RETURN *")
  }

  test("MATCH (n WHERE exists( (:Label)--() )) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n) WHERE exists( ($node1:Label)-[$rel]-($node2) ) RETURN *")
  }

  test("MATCH (n { p: exists( (:Label)--() ) }) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n) WHERE n.p = exists( ($node1:Label)-[$rel]-($node2) ) RETURN *")
  }

  test("MATCH (n { p: exists( (:Label {prop: 42})--() ) }) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n) WHERE n.p = exists( ($node1:Label {prop: 42})-[$rel]-($node2) ) RETURN *")
  }

  test("MATCH (n)-[r WHERE exists( (:Label)--() )]->(m) RETURN *") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"

    assertRewrite(s"MATCH (n)-[r]->(m) WHERE exists( ($node1:Label)-[$rel]-($node2) ) RETURN *")
  }

  test("MATCH ANY SHORTEST (a)-[r:R]->+(b) RETURN *") {
    assertRewrite("MATCH ANY SHORTEST (a)-[r:R]->+(b) RETURN *")
  }

  test("MATCH ANY SHORTEST (a:A)-[r:R]->+(b:B) RETURN *") {
    assertRewrite("MATCH ANY SHORTEST ((a)-[r:R]->+(b) WHERE a:A AND b:B) RETURN *")
  }

  test("MATCH ANY SHORTEST ((a:A)-[r:R]->+(b:B) WHERE b.prop IS NOT NULL) RETURN *") {
    assertRewrite("MATCH ANY SHORTEST ((a)-[r:R]->+(b) WHERE a:A AND b:B AND b.prop IS NOT NULL) RETURN *")
  }

  test("MATCH ALL SHORTEST PATHS (a {prop: 42})-[r:R]->+(b) RETURN *") {
    assertRewrite("MATCH ALL SHORTEST PATHS ((a)-[r:R]->+(b) WHERE a.prop = 42) RETURN *")
  }

  test("MATCH ANY (a WHERE a.prop > 10)-[r:R]->+(b) RETURN *") {
    assertRewrite("MATCH ANY ((a)-[r:R]->+(b) WHERE a.prop > 10) RETURN *")
  }

  test("MATCH SHORTEST 2 GROUPS (a)-[r:R]->+(b WHERE b:!Foo) RETURN *") {
    assertRewrite("MATCH SHORTEST 2 GROUPS ((a)-[r:R]->+(b) WHERE b:!Foo) RETURN *")
  }
}
