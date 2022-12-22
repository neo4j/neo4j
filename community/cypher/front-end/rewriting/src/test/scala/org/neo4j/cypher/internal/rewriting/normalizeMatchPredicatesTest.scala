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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeMatchPredicates
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite


class normalizeMatchPredicatesTest extends CypherFunSuite {

  private val prettifier = Prettifier(ExpressionStringifier(_.asCanonicalStringVal))

  def rewriter(semanticState: SemanticState): Rewriter = {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    inSequence(
      nameAllPatternElements(anonVarNameGen),
      normalizeHasLabelsAndHasType(semanticState),
      normalizeMatchPredicates.getRewriter(semanticState, Map.empty, OpenCypherExceptionFactory(None), new AnonymousVariableNameGenerator),
    )
  }

  def parseForRewriting(queryText: String): Statement = JavaCCParser.parse(queryText.replace("\r\n", "\n"), OpenCypherExceptionFactory(None), new AnonymousVariableNameGenerator)

  private def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery)
    rewriter(SemanticChecker.check(original).state)
    val result = original.endoRewrite(rewriter(SemanticChecker.check(original).state))
    val expected = parseForRewriting(expectedQuery)
    //For the test sake we need to rewrite away HasLabelsOrTypes to HasLabels alse on the expected
    val expectedResult = expected.endoRewrite(normalizeHasLabelsAndHasType(SemanticChecker.check(expected).state))
    assert(result === expectedResult, s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(expectedResult)}\nbut was rewritten to:${prettifier.asString(result.asInstanceOf[Statement])}")
  }

  test("move single predicate from node to WHERE") {
    assertRewrite(
      "MATCH (n {foo: 'bar'}) RETURN n",
      "MATCH (n) WHERE n.foo = 'bar' RETURN n")
  }

  test("move single predicates from rel to WHERE") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n)-[r:Foo {foo: 1}]->() RETURN n",
      s"MATCH (n)-[r:Foo]->($node) WHERE r.foo = 1 RETURN n")
  }

  test("move multiple predicates from nodes to WHERE") {
    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4}) RETURN n",
      "MATCH (n) WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")
  }

  test("move multiple predicates from rels to WHERE") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n)-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n",
      s"MATCH (n)-[r:Foo]->($node) WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("move multiple predicates to WHERE") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n",
      s"MATCH (n)-[r:Foo]->($node) WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("remove empty predicate from node") {
    assertRewrite(
      "MATCH (n { }) RETURN n",
      "MATCH (n) RETURN n")
  }

  test("remove empty predicate from rel") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n)-[r:Foo { }]->() RETURN n",
      s"MATCH (n)-[r:Foo]->($node) RETURN n")
  }

  test("remove empty predicates") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n { })-[r:Foo { }]->() RETURN n",
      s"MATCH (n)-[r:Foo]->($node) RETURN n")
  }

  test("remove empty predicates and keep existing WHERE") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n { })-[r:Foo { }]->() WHERE n.baz = true OR r.baz = false RETURN n",
      s"MATCH (n)-[r:Foo]->($node) WHERE n.baz = true OR r.baz = false RETURN n")
  }

  test("prepend predicates to existing WHERE") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() WHERE n.baz = true OR r.baz = false RETURN n",
      s"MATCH (n)-[r:Foo]->($node) WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' AND (n.baz = true OR r.baz = false) RETURN n")
  }

  test("move internally named node pattern elements") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH ({foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN r",
      s"MATCH ($node1)-[r:Foo]->($node2) WHERE $node1.foo = 'bar' AND $node1.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN r")
  }

  test("move internally named rel pattern elements") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val rel = s"`${anonVarNameGen.nextName}`"
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4})-[:Foo {foo: 1, bar: 'baz'}]->() RETURN n",
      s"MATCH (n)-[$rel:Foo]->($node) WHERE n.foo = 'bar' AND n.bar = 4 AND $rel.foo = 1 AND $rel.bar = 'baz' RETURN n")
  }

  test("move single label from nodes to WHERE") {
    assertRewrite(
      "MATCH (n:LABEL) RETURN n",
      "MATCH (n) WHERE n:LABEL RETURN n")
  }

  test("move multiple labels from nodes to WHERE") {
    assertRewrite(
      "MATCH (n:L1:L2) RETURN n",
      "MATCH (n) WHERE n:L1:L2 RETURN n")
  }

  test("move single label from start node to WHERE when pattern contains relationship") {
    assertRewrite(
      "MATCH (n:Foo)-[r]->(b) RETURN n",
      "MATCH (n)-[r]->(b) WHERE n:Foo RETURN n")
  }

  test("move single label from end node to WHERE when pattern contains relationship") {
    assertRewrite(
      "MATCH (n)-[r]->(b:Foo) RETURN n",
      "MATCH (n)-[r]->(b) WHERE b:Foo RETURN n")
  }

  test("move properties and labels from nodes and relationships to WHERE clause") {
    assertRewrite(
      "MATCH (a:A {foo:'v1', bar:'v2'})-[r:R {baz: 'v1'}]->(b:B {foo:'v2', baz:'v2'}) RETURN *",
      "MATCH (a)-[r:R]->(b) WHERE a.foo = 'v1' AND a.bar = 'v2' AND a:A AND r.baz = 'v1' AND b.foo = 'v2' AND b.baz = 'v2' AND b:B RETURN *")
  }

  test("move single property from var length relationship to the where clause") {
    assertRewrite(
      "MATCH (n)-[r* {prop: 42}]->(b) RETURN n",
      "MATCH (n)-[r*]->(b) WHERE ALL(`  UNNAMED0` in r where `  UNNAMED0`.prop = 42) RETURN n")
  }

  test("move multiple properties from var length relationship to the where clause") {
    assertRewrite(
      "MATCH (n)-[r* {prop: 42, p: 'aaa'}]->(b) RETURN n",
      "MATCH (n)-[r*]->(b) WHERE ALL(`  UNNAMED0` in r where `  UNNAMED0`.prop = 42 AND `  UNNAMED0`.p = 'aaa') RETURN n")
  }

  test("varlength with labels") {
    assertRewrite(
      "MATCH (a:Artist)-[r:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *",
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND ALL(`  UNNAMED0` in r where `  UNNAMED0`.year = 1988) AND b:Artist  RETURN *")
  }

  test("varlength with labels and parameters") {
    assertRewrite(
      "MATCH (a:Artist)-[r:WORKED_WITH* { year: $foo }]->(b:Artist) RETURN *",
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND ALL(`  UNNAMED0` in r where `  UNNAMED0`.year = $foo)  AND b:Artist RETURN *")
  }

  test("move single node patter predicate from node to WHERE") {
    assertRewrite(
      "MATCH (n WHERE n.prop > 123) RETURN n",
      "MATCH (n) WHERE n.prop > 123 RETURN n")
  }

  test("move multiple node pattern predicates from node to WHERE") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val rel = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n WHERE n.prop > 123)-->(m WHERE m.prop < 42 AND m.otherProp = 'hello') RETURN n",
      s"MATCH (n)-[$rel]->(m) WHERE n.prop > 123 AND (m.prop < 42 AND m.otherProp = 'hello') RETURN n")
  }

  test("add multiple node pattern predicates from node to existing WHERE predicate") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val rel = s"`${anonVarNameGen.nextName}`"

    assertRewrite(
      "MATCH (n WHERE n.prop > 123)-->(m WHERE m.prop < 42 AND m.otherProp = 'hello') WHERE n.prop <> m.prop RETURN n",
      s"MATCH (n)-[$rel]->(m) WHERE n.prop > 123 AND (m.prop < 42 AND m.otherProp = 'hello') AND n.prop <> m.prop RETURN n")
  }

  test("should not move label expression in EXISTS clause") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"

    assertRewrite("MATCH (a WHERE EXISTS {(:A)}) RETURN *",
      s"MATCH (a) WHERE EXISTS {($node:A)} RETURN *")
  }

  test("should not move label expression in EXISTS clause with relationship pattern") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"

    assertRewrite("MATCH (n WHERE EXISTS {MATCH (:A)-[]->(b:B)}) RETURN *",
      s"MATCH (n) WHERE EXISTS {MATCH ($node:A)-[$rel]->(b:B)} RETURN *")
  }

  test("should not move label expression in exists() to a separate WHERE clause") {
    val anonVarNameGen = new AnonymousVariableNameGenerator
    val node1 = s"`${anonVarNameGen.nextName}`"
    val rel = s"`${anonVarNameGen.nextName}`"
    val node2 = s"`${anonVarNameGen.nextName}`"

    assertRewrite("MATCH (n WHERE exists( (:Label)--() )) RETURN *",
      s"MATCH (n) WHERE exists( ($node1:Label)-[$rel]-($node2) ) RETURN *")
  }
}
