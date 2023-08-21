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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PathStepStringifierTest extends CypherFunSuite with AstConstructionTestSupport {

  private val expressionStringifier = ExpressionStringifier()
  private val pathStringifier = PathStepStringifier(expressionStringifier)

  test("SingleRelationshipPathStep with outgoing relationship direction") {
    val pathStep = NodePathStep(
      varFor("a"),
      SingleRelationshipPathStep(varFor("b"), OUTGOING, Some(varFor("c")), NilPathStep()(pos))(pos)
    )(pos)

    assert(pathStringifier(pathStep) === "(a)-[b]->(c)")
  }

  test("MultiRelationshipPathStep with incoming relationship direction") {
    val pathStep = NodePathStep(
      varFor("a"),
      MultiRelationshipPathStep(varFor("b"), INCOMING, Some(varFor("c")), NilPathStep()(pos))(pos)
    )(pos)

    assert(pathStringifier(pathStep) === "(a)<-[b*]-(c)")
  }

  test("Multiple relationship path steps") {
    val nextPathStep = SingleRelationshipPathStep(varFor("d"), BOTH, Some(varFor("e")), NilPathStep()(pos))(pos)
    val pathStep = NodePathStep(
      varFor("a"),
      MultiRelationshipPathStep(varFor("b"), OUTGOING, Some(varFor("c")), nextPathStep)(pos)
    )(pos)

    assert(pathStringifier(pathStep) === "(a)-[b*]->(c)-[d]-(e)")
  }

  test("Repeat path steps") {
    val pathStep = NodePathStep(
      varFor("a"),
      RepeatPathStep(
        List(
          NodeRelPair(varFor("n"), varFor("r")),
          NodeRelPair(varFor("m"), varFor("q"))
        ),
        varFor("b"),
        NilPathStep()(pos)
      )(pos)
    )(pos)

    assert(pathStringifier(pathStep) === "(a) ((n)-[r]-(m)-[q]-())* (b)")
  }

  test("Repeat path steps juxtaposed qpp") {
    val pathStep = NodePathStep(
      varFor("a"),
      RepeatPathStep(
        List(NodeRelPair(varFor("n"), varFor("r")), NodeRelPair(varFor("m"), varFor("q"))),
        varFor("  UNNAMED0"),
        RepeatPathStep(
          List(NodeRelPair(varFor("b"), varFor("r2"))),
          varFor("k"),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos)

    assert(pathStringifier(pathStep) === "(a) ((n)-[r]-(m)-[q]-())* (  UNNAMED0) ((b)-[r2]-())* (k)")
  }
}
