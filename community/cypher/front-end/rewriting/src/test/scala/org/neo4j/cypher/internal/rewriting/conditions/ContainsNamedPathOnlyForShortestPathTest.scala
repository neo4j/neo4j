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
package org.neo4j.cypher.internal.rewriting.conditions

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ContainsNamedPathOnlyForShortestPathTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: Any => Seq[String] =
    containsNamedPathOnlyForShortestPath(_)(CancellationChecker.NeverCancelled)

  test("happy when we have no named paths") {
    val ast = SingleQuery(Seq(
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(NodePattern(Some(varFor("n")), None, None, None)(pos)),
        Seq.empty,
        None
      )(pos),
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(AliasedReturnItem(varFor("n"), varFor("n"))(pos))
        )(pos),
        None,
        None,
        None
      )(pos)
    ))(pos)

    condition(ast) shouldBe empty
  }

  test("unhappy when we have a named path") {
    val namedPattern: NamedPatternPart =
      NamedPatternPart(varFor("p"), PatternPart(NodePattern(Some(varFor("n")), None, None, None)(pos)))(pos)
    val ast = SingleQuery(Seq(
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(namedPattern),
        Seq.empty,
        None
      )(
        pos
      ),
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(AliasedReturnItem(varFor("n"), varFor("n"))(pos))
        )(pos),
        None,
        None,
        None
      )(pos)
    ))(pos)

    condition(ast) should equal(Seq(s"Expected none but found $namedPattern at position $pos"))
  }

  test("should allow named path for shortest path") {
    val ast = SingleQuery(Seq(
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(NamedPatternPart(
          varFor("p"),
          ShortestPathsPatternPart(NodePattern(Some(varFor("n")), None, None, None)(pos), single = true)(pos)
        )(pos)),
        Seq.empty,
        None
      )(pos),
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(AliasedReturnItem(varFor("n"), varFor("n"))(pos))
        )(pos),
        None,
        None,
        None
      )(pos)
    ))(pos)

    condition(ast) shouldBe empty
  }
}
