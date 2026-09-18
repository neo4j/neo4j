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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.DefaultWith
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.ParsedAsOrderBy
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.ParseStringMatcher
import org.neo4j.cypher.internal.ast.test.util.Parsers
import org.neo4j.cypher.internal.ast.test.util.Parses
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Namespace
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableFor2
import org.scalatest.prop.Tables

import scala.reflect.ClassTag
import scala.util.Random

class LetParserTest extends AstParsingTestBase with TableDrivenPropertyChecks {

  // positive tests

  def parseValidLet[T <: ASTNode : ClassTag](expected: T, offset: Int)(implicit p: Parsers[T]): ParseStringMatcher[T] =
    parseIn[T] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'LET': expected"
        ).withSyntaxErrorContaining(s"(line 1, column ${offset + 1} (offset: $offset))")
      case _ => _.toAst(expected)
    }

  def parsesValidLet[T <: ASTNode : ClassTag](expected: T, offset: Int)(implicit p: Parsers[T]): Unit =
    testName should parseValidLet(expected, offset)

  test(s"LET DISTINCT = false") {
    parsesValidLet[Clause](withAdditionalItemsTyped(ParsedAsLet, aliasedReturnItem(falseLiteral, "DISTINCT")), 0)
  }

  test(s"LET AS = false") {
    parsesValidLet[Clause](withAdditionalItemsTyped(ParsedAsLet, aliasedReturnItem(falseLiteral, "AS")), 0)
  }

  test(s"LET LET = false") {
    parsesValidLet[Clause](withAdditionalItemsTyped(ParsedAsLet, aliasedReturnItem(falseLiteral, "LET")), 0)
  }

  test(s"LET `*, a` = false") {
    parsesValidLet[Clause](withAdditionalItemsTyped(ParsedAsLet, aliasedReturnItem(falseLiteral, "*, a")), 0)
  }

  private val variableNames: Seq[(String, Boolean)] = Seq(
    ("marla", false),
    ("bunny", false),
    ("kellan", false),
    ("euphemia", false),
    ("_clement", false),
    ("_andie", false),
    ("_page4", false),
    ("_miriam4", false),
    ("rosa_lee", false),
    ("alina_lee", false),
    ("Thaddeus", true),
    ("123456", true),
    ("Sid&Ney", true),
    ("4Adrianna", true),
    ("Céline", true)
  )

  private val letItems: Seq[(String, Expression)] =
    Seq(
      ("null", nullLiteral),
      ("true", trueLiteral),
      ("123", literalInt(123)),
      ("a", varFor("a")),
      ("1 = 1", equals(literalInt(1), literalInt(1))),
      ("a.prop", prop(varFor("a"), "prop")),
      ("a.prop + 12", add(prop(varFor("a"), "prop"), literalInt(12))),
      ("a.prop > 5", greaterThan(prop(varFor("a"), "prop"), literalInt(5))),
      (
        "cos(0.5)",
        FunctionInvocation(
          functionName = FunctionName(namespace = Namespace(parts = List())(pos), name = "cos")(pos),
          distinct = false,
          args = Vector(DecimalDoubleLiteral(stringVal = "0.5")(pos))
        )(pos)
      ),
      (
        "COLLECT { MATCH (a)-->(b) RETURN b }",
        CollectExpression(
          singleQuery(
            match_(relationshipChain(nodePat(Some("a")), relPat(), nodePat(Some("b")))),
            return_(variableReturnItem("b"))
          )
        )(pos, None, None)
      ),
      (
        "COLLECT { UNWIND [1,2] AS x LET b = x^2 RETURN b }",
        CollectExpression(
          singleQuery(
            unwind(listOf(literalInt(1), literalInt(2)), varFor("x")),
            withAdditionalItemsTyped(ParsedAsLet, aliasedReturnItem(pow(varFor("x"), literalInt(2)), "b")),
            return_(variableReturnItem("b"))
          )
        )(pos, None, None)
      ),
      (
        "COUNT { MATCH (a)-->(b) RETURN b }",
        CountExpression(
          singleQuery(
            match_(relationshipChain(nodePat(Some("a")), relPat(), nodePat(Some("b")))),
            return_(variableReturnItem("b"))
          )
        )(pos, None, None)
      ),
      (
        "EXISTS { FILTER false }",
        ExistsExpression(singleQuery(withAllTyped(Some(where(falseLiteral)), ParsedAsFilter)))(pos, None, None)
      )
    )

  private val samples = 20
  private val maxAdditionalItems = 6

  private val seed = new Random().nextLong()

  private def positiveCases: TableFor2[String, With] = {
    val rand = new Random(seed)

    def pickNFrom[T](n: Int, seq: Seq[T]): Seq[T] = rand.shuffle(seq).take(n)

    def pickMaxNFrom[T](n: Int, seq: Seq[T]): Seq[T] = pickNFrom(rand.nextInt(n + 1), seq)

    val cases =
      for {
        firstItem <- letItems
        additionalItems <- (0 until samples).map(_ => pickMaxNFrom(maxAdditionalItems, letItems)).distinct
      } yield {
        val itemsGen = (firstItem +: additionalItems)
        val items = itemsGen.zip(pickNFrom(itemsGen.size, variableNames)).map {
          case ((rhsCypher, rhsExpression), (variable, escaped)) =>
            val ari = aliasedReturnItem(rhsExpression, variable)
            val cypher = if (escaped) s"`$variable` = $rhsCypher" else s"$variable = $rhsCypher"
            (cypher, ari)
        }
        val cypherLet = s"LET ${items.map(_._1).mkString(", ")}"
        val expectedWith = withAdditionalItemsTyped(ParsedAsLet, items.map(_._2): _*)
        (cypherLet, expectedWith)
      }

    Tables.Table(("cypherLet", "expectedWith"), cases: _*)
  }

  private def forAllPositiveCases(f: (String, With) => Unit): Unit =
    withClue(s"random seed = $seed (reproduce with new Random($seed)): ") {
      forAll(positiveCases) { case (cypherLet, expectedWith) => f(cypherLet, expectedWith) }
    }

  test("<cypherLet>") {
    forAllPositiveCases { (cypherLet, expectedWith) =>
      cypherLet should parseValidLet[Clause](expectedWith, 0)
    }
  }

  test("MATCH (a) <cypherLet> RETURN a") {
    forAllPositiveCases { (cypherLet, expectedWith) =>
      s"MATCH (a) $cypherLet RETURN a" should parseValidLet(
        Statements(Seq(SingleQuery(Seq(
          match_(Seq(nodePat(Some("a"))), None),
          expectedWith,
          return_(variableReturnItem("a"))
        ))(pos))),
        10
      )
    }
  }

  test("MATCH (a) WHERE true <cypherLet> RETURN a") {
    forAllPositiveCases { (cypherLet, expectedWith) =>
      s"MATCH (a) WHERE true $cypherLet RETURN a" should parseValidLet(
        Statements(Seq(SingleQuery(Seq(
          match_(Seq(nodePat(Some("a"))), Some(where(trueLiteral))),
          expectedWith,
          return_(variableReturnItem("a"))
        ))(pos))),
        21
      )
    }
  }

  test("MATCH (a) WITH * WHERE true <cypherLet> RETURN a") {
    forAllPositiveCases { (cypherLet, expectedWith) =>
      s"MATCH (a) WITH * WHERE true $cypherLet RETURN a" should parseValidLet(
        Statements(Seq(SingleQuery(Seq(
          match_(Seq(nodePat(Some("a"))), None),
          withAllTyped(Some(where(trueLiteral)), DefaultWith),
          expectedWith,
          return_(variableReturnItem("a"))
        ))(pos))),
        28
      )
    }
  }

  test("MATCH (a) <cypherLet> WITH *, 1 AS b RETURN a") {
    forAllPositiveCases { (cypherLet, expectedWith) =>
      s"MATCH (a) $cypherLet WITH *, 1 AS b RETURN a" should parseValidLet(
        Statements(Seq(SingleQuery(Seq(
          match_(Seq(nodePat(Some("a"))), None),
          expectedWith,
          withAll(aliasedReturnItem(literalInt(1), "b")),
          return_(variableReturnItem("a"))
        ))(pos))),
        10
      )
    }
  }

  test("MATCH (a) ORDER BY a.prop OFFSET 1 LIMIT 1 <cypherLet> FILTER true RETURN a") {
    forAllPositiveCases { (cypherLet, expectedWith) =>
      s"MATCH (a) ORDER BY a.prop OFFSET 1 LIMIT 1 $cypherLet FILTER true RETURN a" should parseValidLet(
        Statements(Seq(SingleQuery(Seq(
          match_(Seq(nodePat(Some("a"))), None),
          withAllTyped(
            Some(orderBy(sortItem(prop(varFor("a"), "prop")))),
            Some(offset(1)),
            Some(limit(1)),
            ParsedAsOrderBy
          ),
          expectedWith,
          withAllTyped(Some(where(trueLiteral)), ParsedAsFilter),
          return_(variableReturnItem("a"))
        ))(pos))),
        43
      )
    }
  }

  test("MATCH (a) <cypherLet> ORDER BY a.prop OFFSET 1 LIMIT 1 RETURN a") {
    forAllPositiveCases { (cypherLet, expectedWith) =>
      s"MATCH (a) $cypherLet ORDER BY a.prop OFFSET 1 LIMIT 1 RETURN a" should parseValidLet(
        Statements(Seq(SingleQuery(Seq(
          match_(Seq(nodePat(Some("a"))), None),
          expectedWith,
          withAllTyped(
            Some(orderBy(sortItem(prop(varFor("a"), "prop")))),
            Some(offset(1)),
            Some(limit(1)),
            ParsedAsOrderBy
          ),
          return_(variableReturnItem("a"))
        ))(pos))),
        10
      )
    }
  }

  // negative tests

  def parsesInvalidLet[T <: ASTNode : ClassTag]()(implicit p: Parsers[T]): Parses[T] =
    parsesIn[T] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'LET': expected")
      case _       => _.withSyntaxErrorContaining(s"Invalid input")
    }

  test(s"LET *") {
    parsesInvalidLet[Statements]()
  }

  for {
    (rhsCypher, _) <- letItems
  } {
    test(s"LET $rhsCypher") {
      parsesInvalidLet[Statements]()
    }
    test(s"LET $rhsCypher AS x") {
      parsesInvalidLet[Statements]()
    }
    test(s"LET `x = $rhsCypher`") {
      parsesInvalidLet[Statements]()
    }
    test(s"LET DISTINCT $rhsCypher") {
      parsesInvalidLet[Statements]()
    }
    test(s"LET DISTINCT $rhsCypher AS x") {
      parsesInvalidLet[Statements]()
    }
    test(s"LET DISTINCT x = $rhsCypher") {
      parsesInvalidLet[Statements]()
    }
  }

}
