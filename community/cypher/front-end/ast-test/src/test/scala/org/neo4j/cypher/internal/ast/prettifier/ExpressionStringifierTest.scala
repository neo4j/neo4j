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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.PrecedenceLevelsTestBase
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpressionStringifierTest extends CypherFunSuite with PrecedenceLevelsTestBase {

  test("Meta: On level 1 all expression arguments should be syntactically delimited") {
    precedenceLevel.last.foreach(op =>
      op.syntacticallyDelimited.map(_._1) should contain theSameElementsAs (0 until op.numArgs)
    )
  }

  private val allVersions = CypherVersion.values().toSet

  private val tests: Seq[(Expression, String, Set[CypherVersion])] = Seq(
    (
      listComprehension(
        variable = varFor("x"),
        collection = varFor("list"),
        predicate = Some(labelExpressionPredicate(
          varFor("x"),
          labelDisjunctions(Seq(labelNegation(labelOrRelTypeLeaf("a")), labelOrRelTypeLeaf("m")))
        )),
        extractExpression = None
      ),
      """[x IN list WHERE (x:!a|m)]""".stripMargin,
      allVersions
    ),
    (
      listComprehension(
        variable = varFor("x"),
        collection = labelExpressionPredicate(
          varFor("foo"),
          labelDisjunctions(Seq(labelNegation(labelOrRelTypeLeaf("a")), labelOrRelTypeLeaf("m")))
        ),
        predicate = None,
        extractExpression = None
      ),
      """[x IN (foo:!a|m)]""".stripMargin,
      allVersions
    ),
    (
      listComprehension(
        variable = varFor("x"),
        collection = labelExpressionPredicate(
          varFor("foo"),
          labelDisjunctions(Seq(labelNegation(labelOrRelTypeLeaf("a")), labelOrRelTypeLeaf("m")))
        ),
        predicate = None,
        extractExpression = Some(varFor("bar"))
      ),
      """[x IN (foo:!a|m) | bar]""".stripMargin,
      allVersions
    ),
    (
      listComprehension(
        variable = varFor("x"),
        collection = varFor("list"),
        predicate = Some(isTyped(
          varFor("x"),
          IntegerType(isNullable = true)(pos)
        )),
        extractExpression = Some(nullLiteral)
      ),
      """[x IN list WHERE (x IS :: INTEGER) | NULL]""".stripMargin,
      allVersions
    ),
    (
      listComprehension(
        variable = varFor("x"),
        collection = isTyped(
          varFor("foo"),
          IntegerType(isNullable = true)(pos)
        ),
        predicate = None,
        extractExpression = Some(nullLiteral)
      ),
      """[x IN (foo IS :: INTEGER) | NULL]""".stripMargin,
      allVersions
    ),
    (
      listComprehension(
        variable = varFor("x"),
        collection = varFor("list"),
        predicate = Some(isNotTyped(
          varFor("x"),
          IntegerType(isNullable = true)(pos)
        )),
        extractExpression = Some(nullLiteral)
      ),
      """[x IN list WHERE (x IS NOT :: INTEGER) | NULL]""".stripMargin,
      allVersions
    ),
    (
      listComprehension(
        variable = varFor("x"),
        collection = isNotTyped(
          varFor("foo"),
          IntegerType(isNullable = true)(pos)
        ),
        predicate = None,
        extractExpression = Some(nullLiteral)
      ),
      """[x IN (foo IS NOT :: INTEGER) | NULL]""".stripMargin,
      allVersions
    ),
    (
      ExistsExpression(
        singleQuery(
          return_(aliasedReturnItem(literalInt(1), "one"))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """EXISTS { RETURN 1 AS one }""".stripMargin,
      allVersions
    ),
    (
      ExistsExpression(
        singleQuery(
          return_(aliasedReturnItem(
            caseExpression(
              (trueLiteral, literalInt(1)),
              (falseLiteral, literalInt(0))
            ),
            "one"
          ))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """EXISTS {
        |  RETURN CASE
        |    WHEN true THEN 1
        |    WHEN false THEN 0
        |  END AS one
        |}""".stripMargin,
      allVersions
    ),
    (
      ExistsExpression(
        singleQuery(
          match_(nodePat()),
          return_(aliasedReturnItem(literalInt(1), "one"))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """EXISTS {
        |  MATCH ()
        |  RETURN 1 AS one
        |}""".stripMargin,
      allVersions
    ),
    (
      CollectExpression(
        singleQuery(
          return_(aliasedReturnItem(literalInt(1), "one"))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """COLLECT { RETURN 1 AS one }""".stripMargin,
      allVersions
    ),
    (
      CollectExpression(
        singleQuery(
          return_(aliasedReturnItem(
            caseExpression(
              (trueLiteral, literalInt(1)),
              (falseLiteral, literalInt(0))
            ),
            "one"
          ))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """COLLECT {
        |  RETURN CASE
        |    WHEN true THEN 1
        |    WHEN false THEN 0
        |  END AS one
        |}""".stripMargin,
      allVersions
    ),
    (
      CollectExpression(
        singleQuery(
          match_(nodePat()),
          return_(aliasedReturnItem(literalInt(1), "one"))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """COLLECT {
        |  MATCH ()
        |  RETURN 1 AS one
        |}""".stripMargin,
      allVersions
    ),
    (
      CountExpression(
        singleQuery(
          return_(aliasedReturnItem(literalInt(1), "one"))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """COUNT { RETURN 1 AS one }""".stripMargin,
      allVersions
    ),
    (
      CountExpression(
        singleQuery(
          return_(aliasedReturnItem(
            caseExpression(
              (trueLiteral, literalInt(1)),
              (falseLiteral, literalInt(0))
            ),
            "one"
          ))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """COUNT {
        |  RETURN CASE
        |    WHEN true THEN 1
        |    WHEN false THEN 0
        |  END AS one
        |}""".stripMargin,
      allVersions
    ),
    (
      CountExpression(
        singleQuery(
          match_(nodePat()),
          return_(aliasedReturnItem(literalInt(1), "one"))
        )
      )(pos, Some(Set.empty), Some(Set.empty)),
      """COUNT {
        |  MATCH ()
        |  RETURN 1 AS one
        |}""".stripMargin,
      allVersions
    ),
    (
      PatternComprehension(
        namedPath = None,
        pattern = RelationshipsPattern(RelationshipChain(
          nodePat(Some("u")),
          RelationshipPattern(Some(varFor("r")), Some(labelRelTypeLeaf("FOLLOWS")), None, None, None, OUTGOING)(pos),
          nodePat(Some("u2"))
        )(pos))(pos),
        predicate = Some(hasLabels("u2", "User")),
        projection = prop("u2", "id")
      )(
        pos,
        computedIntroducedVariables = Some(Set(varFor("u"), varFor("u2"))),
        computedScopeDependencies = Some(Set(varFor("r")))
      ),
      "[(u)-[r:FOLLOWS]->(u2) WHERE u2:User | u2.id]",
      allVersions
    ),
    (
      PatternComprehension(
        namedPath = None,
        pattern = RelationshipsPattern(RelationshipChain(
          nodePat(Some("u")),
          RelationshipPattern(Some(varFor("r")), Some(labelRelTypeLeaf("FOLLOWS")), None, None, None, OUTGOING)(pos),
          nodePat(Some("u2"))
        )(pos))(pos),
        predicate = Some(labelExpressionPredicate("u2", labelDisjunction(labelLeaf("UserA"), labelLeaf("UserB")))),
        projection = prop("u2", "id")
      )(
        pos,
        computedIntroducedVariables = Some(Set(varFor("u"), varFor("u2"))),
        computedScopeDependencies = Some(Set(varFor("r")))
      ),
      "[(u)-[r:FOLLOWS]->(u2) WHERE (u2:UserA|UserB) | u2.id]",
      allVersions
    ),
    (
      PatternComprehension(
        namedPath = None,
        pattern = RelationshipsPattern(RelationshipChain(
          nodePat(Some("u")),
          RelationshipPattern(Some(varFor("r")), Some(labelRelTypeLeaf("FOLLOWS")), None, None, None, OUTGOING)(pos),
          nodePat(Some("u2"), Some(labelLeaf("User")))
        )(pos))(pos),
        predicate = None,
        projection = prop("u2", "id")
      )(
        pos,
        computedIntroducedVariables = Some(Set(varFor("u"), varFor("u2"))),
        computedScopeDependencies = Some(Set(varFor("r")))
      ),
      "[(u)-[r:FOLLOWS]->(u2:User) | u2.id]",
      allVersions
    ),
    (
      varFor(
        "yo\u005c\u0075\u0030\u0030\u0036\u0030 union all match (yo) return \u005c\u0075\u0030\u0030\u0036\u0030yo"
      ),
      "`yo\\\\u0060 union all match (yo) return \\\\u0060yo`",
      allVersions
    ),
    (
      varFor("\u005c\u0074 \u005c\u006e \u005c\u0066 \u005c\u0072 \u005c\u0062"),
      "`\\t \\n \\f \\r \\b`",
      allVersions
    ),
    (
      varFor("\\cantbeescaped"),
      "`\\cantbeescaped`",
      allVersions
    ),
    (
      varFor("a\\u"),
      "`a\\u`",
      allVersions
    ),
    (
      varFor("a\\\\u0041"),
      "`a\\\\u0041`",
      allVersions
    )
  ) ++
    all2LevelCombinationsWithRandomBase() ++
    sampledNLevelCombinationsWithRandomBase(500, 5)

  private val stringifier = ExpressionStringifier()

  for {
    ((expr, expectedResult, _), idx) <- tests.zipWithIndex
  } {
    test(s"[$idx] should produce $expectedResult") {
      withClue(expr) {
        lazy val stringifiedExpr = stringifier(expr)
        noException should be thrownBy stringifiedExpr
        stringifiedExpr shouldBe expectedResult
      }
    }
  }
}
