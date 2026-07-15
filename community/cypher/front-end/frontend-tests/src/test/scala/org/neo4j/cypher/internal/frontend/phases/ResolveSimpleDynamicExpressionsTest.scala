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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveSimpleDynamicExpressions
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

class ResolveSimpleDynamicExpressionsTest extends CypherFunSuite with RewritePhaseTest
    with AstConstructionTestSupport with TestName {

  final private val parameters: MapValue = {
    val mvb = new MapValueBuilder()
    val lvb = ListValueBuilder.newListBuilder()
    val emptyLvb = ListValueBuilder.newListBuilder()
    lvb.add(Values.stringValue("Y"))
    lvb.add(Values.stringValue("Z"))
    mvb.add("param", Values.stringValue("X"))
    mvb.add("listParam", lvb.build())
    mvb.add("emptyListParam", emptyLvb.build())
    mvb.build()
  }

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    ResolveSimpleDynamicExpressions(parameters)

  test("MERGE (n:$($param)) RETURN n") {
    assertRewritten(testName, "MERGE (n:X) RETURN n")
  }

  test("MATCH (n:$($param)) RETURN n") {
    assertRewritten(testName, "MATCH (n:X) RETURN n")
  }

  test("CREATE (n:$($param)) RETURN n") {
    assertNotRewritten(testName)
  }

  test("MERGE ()-[r:$($param)]->() RETURN n") {
    assertRewritten(testName, "MERGE ()-[r:X]->() RETURN n", invalidSemantics = true)
  }

  test("MATCH ()-[r:$($param)]->() RETURN n") {
    assertRewritten(testName, "MATCH ()-[r:X]->() RETURN n", invalidSemantics = true)
  }

  test("CREATE ()-[r:$($param)]->() RETURN n") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("MERGE (n:$($listParam)) RETURN n") {
    assertRewritten(testName, "MERGE (n:Y&Z) RETURN n")
  }

  test("MATCH (n:$($listParam)) RETURN n") {
    assertRewritten(testName, "MATCH (n:Y&Z) RETURN n")
  }

  test("CREATE (n:$($listParam)) RETURN n") {
    assertNotRewritten(testName)
  }

  test("MERGE ()-[r:$($listParam)]->() RETURN n") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("MATCH ()-[r:$($listParam)]->() RETURN n") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("CREATE ()-[r:$($listParam)]->() RETURN n") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("MATCH ()-[r:$any($listParam)]->() RETURN n") {
    assertRewritten(testName, "MATCH ()-[r:Y|Z]->() RETURN n", invalidSemantics = true)
  }

  test("MERGE (n:$($emptyListParam)) RETURN n") {
    assertNotRewritten(testName)
  }

  test("MATCH (n:$($emptyListParam)) RETURN n") {
    assertNotRewritten(testName)
  }

  test("CREATE (n:$($emptyListParam)) RETURN n") {
    assertNotRewritten(testName)
  }

  test("MERGE ()-[r:$($emptyListParam)]->() RETURN n") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("MATCH ()-[r:$($emptyListParam)]->() RETURN n") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("""MERGE (n:$("Label")) RETURN n""") {
    assertRewritten(testName, """MERGE (n:Label) RETURN n""")
  }

  test("""MERGE (n:$(["Label1", "Label2"])) RETURN n""") {
    assertRewritten(testName, """MERGE (n:Label1&Label2) RETURN n""")
  }

  test("""MATCH (n:$(["Label1", "Label2"])) RETURN n""") {
    assertRewritten(testName, """MATCH (n:Label1&Label2) RETURN n""")
  }

  test("""CREATE (n:$(["Label1", "Label2"])) RETURN n""") {
    assertNotRewritten(testName)
  }

  test("""MERGE ()-[r:$("Rel")]->() RETURN n""") {
    assertRewritten(testName, """MERGE ()-[r:Rel]->() RETURN n""", invalidSemantics = true)
  }

  test("""MERGE ()-[r:$any(["Rel1", "Rel2"])]->() RETURN n""") {
    assertRewritten(testName, """MERGE ()-[r:Rel1|Rel2]->() RETURN n""", invalidSemantics = true)
  }

  test("""MERGE ()-[r:$([])]->() RETURN n""") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("""MATCH ()-[r:$(["Rel1", "Rel2"])]->() RETURN n""") {
    assertNotRewritten(testName, invalidSemantics = true)
  }

  test("""CREATE ()-[r:$(["Rel1", "Rel2"])]->() RETURN n""") {
    assertNotRewritten(testName, invalidSemantics = true)
  }
}
