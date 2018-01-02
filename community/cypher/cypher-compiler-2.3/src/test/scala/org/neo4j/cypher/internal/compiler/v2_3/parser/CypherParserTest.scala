/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.parser

import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.Query
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{UnresolvedLabel, TokenType, KeyToken}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_3.helpers.LabelSupport
import org.neo4j.cypher.internal.compiler.v2_3.mutation._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.StatementConverters._
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, SyntaxException}
import org.neo4j.cypher.internal.frontend.v2_3.parser._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CypherParserTest extends CypherFunSuite {
  val parser = new CypherParser
  test("shouldParseEasiestPossibleQuery") {
    expectQuery(
      "start s = NODE(1) return s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  test("should return string literal") {
    expectQuery(
      "start s = node(1) return \"apa\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("apa"), "\"apa\"")))
  }

  test("should return string literal with escaped sequence in") {
    expectQuery(
      "start s = node(1) return \"a\\tp\\\"a\\\'b\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a\tp\"a\'b"), "\"a\\tp\\\"a\\\'b\"")))

    expectQuery(
      "start s = node(1) return \'a\\tp\\\'a\\\"b\'",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a\tp\'a\"b"), "\'a\\tp\\\'a\\\"b\'")))
  }

  test("should return string literal containing UTF-16 escape sequence") {
    expectQuery(
      "start s = node(1) return \"a\\uE12345\" AS x",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a" + "\uE123" + "45"), "x")))
  }

  test("should return string literal containing UTF-32 escape sequence") {
    expectQuery(
      "start s = node(1) return \"a\\U000292b145\" AS x",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a" + "\uD864\uDEB1" + "45"), "x")))
  }

  test("allTheNodes") {
    expectQuery(
      "start s = NODE(*) return s",
      Query.
        start(AllNodes("s")).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  test("allTheRels") {
    expectQuery(
      "start r = relationship(*) return r",
      Query.
        start(AllRelationships("r")).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  test("shouldHandleAliasingOfColumnNames") {
    expectQuery(
      "start s = NODE(1) return s as somethingElse",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "somethingElse")))
  }

  test("sourceIsAnIndex") {
    expectQuery(
      """start a = node:index(key = "value") return a""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("sourceIsAnNonParsedIndexQuery") {
    expectQuery(
      """start a = node:index("key:value") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldParseEasiestPossibleRelationshipQuery") {
    expectQuery(
      "start s = relationship(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  test("shouldParseEasiestPossibleRelationshipQueryShort") {
    expectQuery(
      "start s = rel(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  test("sourceIsARelationshipIndex") {
    expectQuery(
      """start a = rel:index(key = "value") return a""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("keywordsShouldBeCaseInsensitive") {
    expectQuery(
      "START s = NODE(1) RETURN s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  test("shouldParseMultipleNodes") {
    expectQuery(
      "start s = NODE(1,2,3) return s",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  test("shouldParseMultipleInputs") {
    expectQuery(
      "start a = node(1), b = NODE(2) return a,b",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  test("shouldFilterOnProp") {
    expectQuery(
      "start a = NODE(1) where a.name = \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldReturnLiterals") {
    expectQuery(
      "start a = NODE(1) return 12",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Literal(12), "12")))
  }

  test("shouldReturnAdditions") {
    expectQuery(
      "start a = NODE(1) return 12+a.x",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Add(Literal(12), Property(Identifier("a"), PropertyKey("x"))), "12+a.x")))
  }

  test("arithmeticsPrecedence") {
    expectQuery(
      "start a = NODE(1) return a.a/a.b*a.c-a.d*a.e",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(
        Subtract(
          Multiply(
            Divide(
              Property(Identifier("a"), PropertyKey("a")),
              Property(Identifier("a"), PropertyKey("b"))),
            Property(Identifier("a"), PropertyKey("c"))),
          Multiply(
            Property(Identifier("a"), PropertyKey("d")),
            Property(Identifier("a"), PropertyKey("e"))))
        , "a.a/a.b*a.c-a.d*a.e")))

    expectQuery(
      "start a = NODE(1) return (10 - 5)^2 * COS(3.1415927/4)^2",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(
        Multiply(
          Pow(
            Subtract(
              Literal(10),
              Literal(5)),
            Literal(2)),
          Pow(
            CosFunction(
              Divide(
                Literal(3.1415927),
                Literal(4)
              )
            ),
            Literal(2)))
        , "(10 - 5)^2 * COS(3.1415927/4)^2")))
  }

  test("shouldFilterOnPropWithDecimals") {
    expectQuery(
      "start a = node(1) where a.extractReturnItems = 3.1415 return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), PropertyKey("extractReturnItems")), Literal(3.1415))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleNot") {
    expectQuery(
      "start a = node(1) where not(a.name = \"andres\") return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleNotEqualTo") {
    expectQuery(
      "start a = node(1) where a.name <> \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleLessThan") {
    expectQuery(
      "start a = node(1) where a.name < \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleGreaterThan") {
    expectQuery(
      "start a = node(1) where a.name > \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleLessThanOrEqual") {
    expectQuery(
      "start a = node(1) where a.name <= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }


  test("shouldHandleRegularComparison") {
    expectQuery(
      "start a = node(1) where \"Andres\" =~ 'And.*' return a",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("shouldHandleMultipleRegularComparison") {
    expectQuery(
      """start a = node(1) where a.name =~ 'And.*' AnD a.name =~ 'And.*' return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(LiteralRegularExpression(Property(Identifier("a"), PropertyKey("name")), Literal("And.*")), LiteralRegularExpression(Property(Identifier("a"), PropertyKey("name")), Literal("And.*")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("shouldHandleEscapedRegexs") {
    expectQuery(
      """start a = node(1) where a.name =~ 'And\\/.*' return a""",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Property(Identifier("a"), PropertyKey("name")), Literal("And\\/.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("shouldHandleGreaterThanOrEqual") {
    expectQuery(
      "start a = node(1) where a.name >= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThanOrEqual(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("booleanLiterals") {
    expectQuery(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(True(), Not(True()))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldFilterOnNumericProp") {
    expectQuery(
      "start a = NODE(1) where 35 = a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), Property(Identifier("a"), PropertyKey("age")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleNegativeLiteralsAsExpected") {
    expectQuery(
      "start a = NODE(1) where -35 = a.age AND (a.age > -1.2 AND a.weight=-50) return a",
      Query.
        start(NodeById("a", 1)).
        where(And(
          Equals(Literal(-35), Property(Identifier("a"), PropertyKey("age"))),
          And(
            GreaterThan(Property(Identifier("a"), PropertyKey("age")), Literal(-1.2)),
            Equals(Property(Identifier("a"), PropertyKey("weight")), Literal(-50))
          )
        )).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("shouldCreateNotEqualsQuery") {
    expectQuery(
      "start a = NODE(1) where 35 <> a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), Property(Identifier("a"), PropertyKey("age"))))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("multipleFilters") {
    expectQuery(
      "start a = NODE(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("mattias")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldCreateXorQuery") {
    expectQuery(
      "start a = NODE(1) where a.name = 'andres' xor a.name = 'mattias' return a",
      Query.
        start(NodeById("a", 1)).
        where(Xor(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("mattias")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("relatedTo") {
    expectQuery(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq("KNOWS"), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  test("relatedToUsingUnicodeDashes") {
    expectQuery(
      "start a = NODE(1) match a —[:KNOWS]﹘> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq("KNOWS"), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  test("relatedToUsingUnicodeArrowHeads") {
    expectQuery(
      "start a = NODE(1) match a〈—[:KNOWS]﹘⟩b return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED25", Seq("KNOWS"), SemanticDirection.BOTH)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  test("relatedToWithoutRelType") {
    expectQuery(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  test("relatedToWithoutRelTypeButWithRelVariable") {
    expectQuery(
      "start a = NODE(1) match a-[r]->b return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  test("relatedToTheOtherWay") {
    expectQuery(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq("KNOWS"), SemanticDirection.INCOMING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  test("twoDoubleOptionalWithFourHalfs") {
    expectQuery(
      "START a=node(1), b=node(2) OPTIONAL MATCH a-[r1]->X<-[r2]-b, a<-[r3]-Z-[r4]->b return r1,r2,r3,r4 order by id(r1),id(r2),id(r3),id(r4)",
      Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      matches(
        RelatedTo(SingleNode("a"), SingleNode("X"), "r1", Seq(), SemanticDirection.OUTGOING, Map.empty),
        RelatedTo(SingleNode("X"), SingleNode("b"), "r2", Seq(), SemanticDirection.INCOMING, Map.empty),
        RelatedTo(SingleNode("a"), SingleNode("Z"), "r3", Seq(), SemanticDirection.INCOMING, Map.empty),
        RelatedTo(SingleNode("Z"), SingleNode("b"), "r4", Seq(), SemanticDirection.OUTGOING, Map.empty)
      ).makeOptional().
      orderBy(
        SortItem(IdFunction(Identifier("r1")), ascending = true),
        SortItem(IdFunction(Identifier("r2")), ascending = true),
        SortItem(IdFunction(Identifier("r3")), ascending = true),
        SortItem(IdFunction(Identifier("r4")), ascending = true)
      ).returns(
        ReturnItem(Identifier("r1"), "r1"),
        ReturnItem(Identifier("r2"), "r2"),
        ReturnItem(Identifier("r3"), "r3"),
        ReturnItem(Identifier("r4"), "r4")
      )
    )
  }

  test("shouldOutputVariables") {
    expectQuery(
      "start a = NODE(1) return a.name",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "a.name")))
  }

  test("shouldReadPropertiesOnExpressions") {
    expectQuery(
      "start a = NODE(1) return (a).name",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "(a).name")))
  }

  test("shouldHandleAndPredicates") {
    expectQuery(
      "start a = NODE(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("lastname")), Literal("taylor")))).
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "a.name")))
  }

  test("relatedToWithRelationOutput") {
    expectQuery(
      "start a = NODE(1) match a -[rel:KNOWS]-> (b) return rel",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Seq("KNOWS"), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("rel"), "rel")))
  }


  test("relatedToWithoutEndName") {
    expectQuery(
      "start a = NODE(1) match a -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED41", "r", Seq("MARRIED"), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("relatedInTwoSteps") {
    expectQuery(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED26", Seq("KNOWS"), SemanticDirection.OUTGOING),
        RelatedTo("b", "c", "  UNNAMED40", Seq("FRIEND"), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("c"), "c"))
    )
  }

  test("djangoCTRelationship") {
    expectQuery(
      "start a = NODE(1) match a -[r:`<<KNOWS>>`]-> b return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("<<KNOWS>>"), SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  test("countTheNumberOfHits") {
    expectQuery(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), SemanticDirection.OUTGOING)).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(CountStar(), "count(*)")))
  }

  test("countStar") {
    expectQuery(
      "start a = NODE(1) return count(*) order by count(*)",
      Query.
        start(NodeById("a", 1)).
        aggregation(CountStar()).
        columns("count(*)").
        orderBy(SortItem(CountStar(), true)).
        returns(ReturnItem(CountStar(), "count(*)")))
  }

  test("distinct") {
    expectQuery(
      "start a = NODE(1) match a -[r]-> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), SemanticDirection.OUTGOING)).
        aggregation().
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  test("sumTheAgesOfPeople") {
    expectQuery(
      "start a = NODE(1) match a -[r]-> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), SemanticDirection.OUTGOING)).
        aggregation(Sum(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "sum(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Sum(Property(Identifier("a"), PropertyKey("age"))), "sum(a.age)")))
  }

  test("avgTheAgesOfPeople") {
    expectQuery(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), SemanticDirection.OUTGOING)).
        aggregation(Avg(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Avg(Property(Identifier("a"), PropertyKey("age"))), "avg(a.age)")))
  }

  test("minTheAgesOfPeople") {
    expectQuery(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED28", Seq(), SemanticDirection.OUTGOING)).
        aggregation(Min(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "min(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Min(Property(Identifier("a"), PropertyKey("age"))), "min(a.age)"))
    )
  }

  test("maxTheAgesOfPeople") {
    expectQuery(
      "start a = NODE(1) match a --> b return a, b, max(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), SemanticDirection.OUTGOING)).
        aggregation(Max(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "max(a.age)").
        returns(
        ReturnItem(Identifier("a"), "a"),
        ReturnItem(Identifier("b"), "b"),
        ReturnItem(Max(Property(Identifier("a"), PropertyKey("age"))), "max(a.age)"))
    )
  }

  test("singleColumnSorting") {
    expectQuery(
      "start a = NODE(1) return a order by a.name",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("sortOnAggregatedColumn") {
    expectQuery(
      "start a = NODE(1) return a order by avg(a.name)",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Avg(Property(Identifier("a"), PropertyKey("name"))), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("sortOnAliasedAggregatedColumn") {
    expectQuery(
      "start n = node(0) match (n)-[r:KNOWS]-(c) return n, count(c) as cnt order by cnt",
      Query.
        start(NodeById("n", 0)).
        matches(RelatedTo("n", "c", "r", Seq("KNOWS"), SemanticDirection.BOTH)).
        orderBy(SortItem(Identifier("cnt"), ascending = true)).
        aggregation(Count(Identifier("c"))).
        returns(ReturnItem(Identifier("n"), "n"), ReturnItem(Count(Identifier("c")), "cnt")))
  }

  test("shouldHandleTwoSortColumns") {
    expectQuery(
      "start a = NODE(1) return a order by a.name, a.age",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = true),
        SortItem(Property(Identifier("a"), PropertyKey("age")), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleTwoSortColumnsAscending") {
    expectQuery(
      "start a = NODE(1) return a order by a.name ASCENDING, a.age ASC",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = true),
        SortItem(Property(Identifier("a"), PropertyKey("age")), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))

  }

  test("orderByDescending") {
    expectQuery(
      "start a = NODE(1) return a order by a.name DESCENDING",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = false)).
        returns(ReturnItem(Identifier("a"), "a")))

  }

  test("orderByDesc") {
    expectQuery(
      "start a = NODE(1) return a order by a.name desc",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = false)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("nestedBooleanOperatorsAndParentesis") {
    expectQuery(
      """start n = NODE(1,2,3) where (n.animal = "monkey" and n.food = "banana") or (n.animal = "cow" and n
      .food="grass") return n""",
      Query.
        start(NodeById("n", 1, 2, 3)).
        where(Or(
        And(
          Equals(Property(Identifier("n"), PropertyKey("animal")), Literal("monkey")),
          Equals(Property(Identifier("n"), PropertyKey("food")), Literal("banana"))),
        And(
          Equals(Property(Identifier("n"), PropertyKey("animal")), Literal("cow")),
          Equals(Property(Identifier("n"), PropertyKey("food")), Literal("grass"))))).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  test("nestedBooleanOperatorsAndParentesisXor") {
    expectQuery(
      """start n = NODE(1,2,3) where (n.animal = "monkey" and n.food = "banana") xor (n.animal = "cow" and n
      .food="grass") return n""",
      Query.
        start(NodeById("n", 1, 2, 3)).
        where(Xor(
        And(
          Equals(Property(Identifier("n"), PropertyKey("animal")), Literal("monkey")),
          Equals(Property(Identifier("n"), PropertyKey("food")), Literal("banana"))),
        And(
          Equals(Property(Identifier("n"), PropertyKey("animal")), Literal("cow")),
          Equals(Property(Identifier("n"), PropertyKey("food")), Literal("grass"))))).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  test("limit5") {
    expectQuery(
      "start n=NODE(1) return n limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  test("skip5") {
    expectQuery(
      "start n=NODE(1) return n skip 5",
      Query.
        start(NodeById("n", 1)).
        skip(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  test("skip5limit5") {
    expectQuery(
      "start n=NODE(1) return n skip 5 limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        skip(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  test("relationshipType") {
    expectQuery(
      "start n=NODE(1) match n-[r]->(x) where type(r) = \"something\" return r",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), SemanticDirection.OUTGOING)).
        where(Equals(RelationshipTypeFunction(Identifier("r")), Literal("something"))).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  test("pathLength") {
    expectQuery(
      "start n=NODE(1) match p=(n-[r]->x) where LENGTH(p) = 10 return p",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "n", "x", Seq.empty, SemanticDirection.OUTGOING))).
        where(Equals(LengthFunction(Identifier("p")), Literal(10.0))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  test("stringLength") {
    expectQuery(
      "return LENGTH('foo') = 10 as n",
      Query.
        matches().
        returns(ReturnItem(Equals(LengthFunction(Literal("foo")), Literal(10.0)), "n")))
  }

  test("string size") {
    expectQuery(
      "return SIZE('foo') = 10 as n",
      Query.
        matches().
        returns(ReturnItem(Equals(SizeFunction(Literal("foo")), Literal(10.0)), "n")))
  }

  test("collectionSize") {
    expectQuery(
      "return SIZE([1, 2]) = 10 as n",
      Query.
        matches().
        returns(ReturnItem(Equals(SizeFunction(Collection(Literal(1), Literal(2))), Literal(10.0)), "n")))
  }

  test("relationshipTypeOut") {
    expectQuery(
      "start n=NODE(1) match n-[r]->(x) return TYPE(r)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), SemanticDirection.OUTGOING)).
        returns(ReturnItem(RelationshipTypeFunction(Identifier("r")), "TYPE(r)")))
  }


  test("shouldBeAbleToParseCoalesce") {
    expectQuery(
      "start n=NODE(1) match n-[r]->(x) return COALESCE(r.name,x.name)",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), SemanticDirection.OUTGOING)).
        returns(ReturnItem(CoalesceFunction(Property(Identifier("r"), PropertyKey("name")), Property(Identifier("x"), PropertyKey("name"))), "COALESCE(r.name,x.name)")))
  }

  test("relationshipsFromPathOutput") {
    expectQuery(
      "start n=NODE(1) match p=n-[r]->x return RELATIONSHIPS(p)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "n", "x", Seq.empty, SemanticDirection.OUTGOING))).
        returns(ReturnItem(RelationshipFunction(Identifier("p")), "RELATIONSHIPS(p)")))
  }

  test("keepDirectionForNamedPaths") {
    expectQuery(
      "START a=node(1) match p=b<-[r]-a return p",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", "r", Seq(), SemanticDirection.INCOMING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "b", "a", Seq(), SemanticDirection.INCOMING))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  test("relationshipsFromPathInWhere") {
    expectQuery(
      "start n=NODE(1) match p=n-[r]->x where length(rels(p))=1 return p",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "n", "x", Seq.empty, SemanticDirection.OUTGOING))).
        where(Equals(LengthFunction(RelationshipFunction(Identifier("p"))), Literal(1))).
        returns (ReturnItem(Identifier("p"), "p")))
  }

  test("countNonNullValues") {
    expectQuery(
      "start a = NODE(1) return a, count(a)",
      Query.
        start(NodeById("a", 1)).
        aggregation(Count(Identifier("a"))).
        columns("a", "count(a)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Count(Identifier("a")), "count(a)")))
  }

  test("shouldHandleIdBothInReturnAndWhere") {
    expectQuery(
      "start a = NODE(1) where id(a) = 0 return ID(a)",
      Query.
        start(NodeById("a", 1)).
        where(Equals(IdFunction(Identifier("a")), Literal(0)))
        returns (ReturnItem(IdFunction(Identifier("a")), "ID(a)")))
  }

  test("shouldBeAbleToHandleStringLiteralsWithApostrophe") {
    expectQuery(
      "start a = node:index(key = 'value') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("shouldHandleQuotationsInsideApostrophes") {
    expectQuery(
      "start a = node:index(key = 'val\"ue') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("val\"ue"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("simplePathExample") {
    expectQuery(
      "start a = node(0) match p = a-->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "  UNNAMED29", Seq(), SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("  UNNAMED29", "a", "b", Seq.empty, SemanticDirection.OUTGOING))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("threeStepsPath") {
    expectQuery(
      "start a = node(0) match p = ( a-[r1]->b-[r2]->c ) return a",
      Query.
        start(NodeById("a", 0)).
        matches(
        RelatedTo("a", "b", "r1", Seq(), SemanticDirection.OUTGOING),
        RelatedTo("b", "c", "r2", Seq(), SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p",
        ParsedRelation("r1", "a", "b", Seq.empty, SemanticDirection.OUTGOING),
        ParsedRelation("r2", "b", "c", Seq.empty, SemanticDirection.OUTGOING))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  test("pathsShouldBePossibleWithoutParenthesis") {
    expectQuery(
      "start a = node(0) match p = a-[r]->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", Seq(), SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq.empty, SemanticDirection.OUTGOING))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  test("variableLengthPath") {
    expectQuery(
      "start a=node(0) match a -[:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(1), Some(3), "knows", SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("variableLengthPathWithRelsIterable") {
    expectQuery(
      "start a=node(0) match a -[r:knows*1..3]-> x return length(r)",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(1), Some(3), "knows", SemanticDirection.OUTGOING, Some("r"))).
        returns(ReturnItem(LengthFunction(Identifier("r")), "length(r)"))
    )
  }

  test("fixedVarLengthPath") {
    expectQuery(
      "start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", SingleNode("a"), SingleNode("x"), Some(3), Some(3), Seq(),
        SemanticDirection.OUTGOING, None, Map.empty)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("variableLengthPathWithoutMinDepth") {
    expectQuery(
      "start a=node(0) match a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", None, Some(3), "knows", SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("variableLengthPathWithRelationshipIdentifier") {
    expectQuery(
      "start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(2), None, "knows", SemanticDirection.OUTGOING, Some("r"))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("variableLengthPathWithoutMaxDepth") {
    expectQuery(
      "start a=node(0) match a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(2), None, "knows", SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("unboundVariableLengthPath") {
    expectQuery(
      "start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", None, None, "knows", SemanticDirection.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("optionalRelationship") {
    expectQuery(
      "start a = node(1) optional match a --> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED35", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("optionalTypedRelationship") {
    expectQuery(
      "start a = node(1) optional match a -[:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED35", Seq("KNOWS"), SemanticDirection.OUTGOING)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("optionalTypedAndNamedRelationship") {
    expectQuery(
      "start a = node(1) optional match a -[r:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq("KNOWS"), SemanticDirection.OUTGOING, Map.empty)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("optionalNamedRelationship") {
    expectQuery(
      "start a = node(1) optional match a -[r]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), SemanticDirection.OUTGOING)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("testAllIterablePredicate") {
    expectQuery(
      """start a = node(1) match p=(a-[r]->b) where all(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), SemanticDirection.OUTGOING))).
        where(AllInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("testAnyIterablePredicate") {
    expectQuery(
      """start a = node(1) match p=(a-[r]->b) where any(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(SingleInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), SemanticDirection.OUTGOING))).
        where(AnyInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("testNoneIterablePredicate") {
    expectQuery(
      """start a = node(1) match p=(a-[r]->b) where none(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), SemanticDirection.OUTGOING))).
        where(NoneInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("testSingleIterablePredicate") {
    expectQuery(
      """start a = node(1) match p=(a-[r]->b) where single(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), SemanticDirection.OUTGOING))).
        where(SingleInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  test("testParamAsStartNode") {
    expectQuery(
      """start pA = node({a}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamAsStartRel") {
    expectQuery(
      """start pA = relationship({a}) return pA""",
      Query.
        start(RelationshipById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testNumericParamNameAsStartNode") {
    expectQuery(
      """start pA = node({0}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("0"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamForWhereLiteral") {
    expectQuery(
      """start pA = node(1) where pA.name = {name} return pA""",
      Query.
        start(NodeById("pA", 1)).
        where(Equals(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("name")))
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamForIndexValue") {
    expectQuery(
      """start pA = node:idx(key = {Value}) return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), ParameterExpression("Value"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamForIndexQuery") {
    expectQuery(
      """start pA = node:idx({query}) return pA""",
      Query.
        start(NodeByIndexQuery("pA", "idx", ParameterExpression("query"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamForSkip") {
    expectQuery(
      """start pA = node(0) return pA skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamForLimit") {
    expectQuery(
      """start pA = node(0) return pA limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamForLimitAndSkip") {
    expectQuery(
      """start pA = node(0) return pA skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ("stop")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testQuotedParams") {
    expectQuery(
      """start pA = node({`id`}) where pA.name =~ {`regex`} return pA skip {`ski``pper`} limit {`stop`}""",
      Query.
        start(NodeById("pA", ParameterExpression("id"))).
        where(RegularExpression(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("regex")))
        skip("ski`pper")
        limit ("stop")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testParamForRegex") {
    expectQuery(
      """start pA = node(0) where pA.name =~ {regex} return pA""",
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("regex")))
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  test("testShortestPathWithMaxDepth") {
    expectQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), SemanticDirection.OUTGOING, false, Some(6), single = true, None)).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("testShortestPathWithMaxDepth and rel iterator") {
    expectQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[r*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), SemanticDirection.OUTGOING, false, Some(6), single = true, Some("r"))).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("testShortestPathWithType") {
    expectQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), SemanticDirection.OUTGOING, false, Some(6), single = true, relIterator = None)).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("testAllShortestPathsWithType") {
    expectQuery(
      """start a=node(0), b=node(1) match p = allShortestPaths( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), SemanticDirection.OUTGOING, false, Some(6), single = false, relIterator = None)).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("testShortestPathWithoutStart") {
    expectQuery(
      """match p = shortestPath( a-[*1..3]->b ) WHERE a.name = 'John' AND b.name = 'Sarah' return p""",
      Query.
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), SemanticDirection.OUTGOING, false, Some(3), single = true, None)).
        where(And(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("John")),
        Equals(Property(Identifier("b"), PropertyKey("name")), Literal("Sarah"))))
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("testShortestPathExpression") {
    expectQuery(
      """start a=node(0), b=node(1) return shortestPath(a-[:KNOWS*0..3]->b) AS path""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        returns(ReturnItem(ShortestPathExpression(
        ShortestPath("  UNNAMED34", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), SemanticDirection.OUTGOING, true, Some(3), single = true, relIterator = None)), "path")))
  }

  test("shortest path with 0 as min length and no max length") {
    expectQuery(
      """start a=node(0), b=node(1) return shortestPath(a-[:KNOWS*0..]->b) AS path""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        returns(ReturnItem(ShortestPathExpression(
        ShortestPath("  UNNAMED34", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), SemanticDirection.OUTGOING, true, None, single = true, relIterator = None)),
        "path")))
  }

  test("testForNull") {
    expectQuery(
      """start a=node(0) where a is null return a""",
      Query.
        start(NodeById("a", 0)).
        where(IsNull(Identifier("a")))
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  test("testForNotNull") {
    expectQuery(
      """start a=node(0) where a is not null return a""",
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(Identifier("a"))))
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  test("testCountDistinct") {
    expectQuery(
      """start a=node(0) return count(distinct a)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(Distinct(Count(Identifier("a")), Identifier("a"))).
        columns("count(distinct a)").
        returns (ReturnItem(Distinct(Count(Identifier("a")), Identifier("a")), "count(distinct a)"))
    )
  }


  test("supportsPatternExistsInTheWhereClause") {
    expectQuery(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED34", Seq(), SemanticDirection.OUTGOING, Map.empty))))).
        returns (ReturnItem(Identifier("a"), "a"))
    )

    expectQuery(
      """start a=node(0), b=node(1) where exists((a)-->(b)) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED43", Seq(), SemanticDirection.OUTGOING, Map.empty))))).
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  test("supportsPatternExistsInTheReturnClause") {
    expectQuery(
      """start a=node(0), b=node(1) return exists((a)-->(b)) AS result""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        returns(ReturnItem(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED44", Seq(), SemanticDirection.OUTGOING, Map.empty)))), "result"))
    )
  }

  test("supportsNotHasRelationshipInTheWhereClause") {
    expectQuery(
      """start a=node(0), b=node(1) where not(a-->()) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(Not(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED41"), "  UNNAMED38", Seq(), SemanticDirection.OUTGOING, Map.empty)))))).
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  test("shouldHandleLFAsWhiteSpace") {
    expectQuery(
      "start\na=node(0)\nwhere\na.prop=12\nreturn\na",
      Query.
        start(NodeById("a", 0)).
        where(Equals(Property(Identifier("a"), PropertyKey("prop")), Literal(12)))
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  test("shouldHandleUpperCaseDistinct") {
    expectQuery(
      "start s = NODE(1) return DISTINCT s",
      Query.
        start(NodeById("s", 1)).
        aggregation().
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("shouldParseMathFunctions") {
    expectQuery(
      "start s = NODE(0) return 5 % s.x, abs(-1), round(3.1415), 2 ^ s.x, sqrt(16), sign(1)",
      Query.
        start(NodeById("s", 0)).
        returns(
        ReturnItem(Modulo(Literal(5), Property(Identifier("s"), PropertyKey("x"))), "5 % s.x"),
        ReturnItem(AbsFunction(Literal(-1)), "abs(-1)"),
        ReturnItem(RoundFunction(Literal(3.1415)), "round(3.1415)"),
        ReturnItem(Pow(Literal(2), Property(Identifier("s"), PropertyKey("x"))), "2 ^ s.x"),
        ReturnItem(SqrtFunction(Literal(16)), "sqrt(16)"),
        ReturnItem(SignFunction(Literal(1)), "sign(1)")
      )
    )
  }

  test("shouldAllowCommentAtEnd") {
    expectQuery(
      "start s = NODE(1) return s // COMMENT",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("shouldAllowCommentAlone") {
    expectQuery(
      """start s = NODE(1) return s
      // COMMENT""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("shouldAllowCommentsInsideStrings") {
    expectQuery(
      "start s = NODE(1) where s.apa = '//NOT A COMMENT' return s",
      Query.
        start(NodeById("s", 1)).
        where(Equals(Property(Identifier("s"), PropertyKey("apa")), Literal("//NOT A COMMENT")))
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("shouldHandleCommentsFollowedByWhiteSpace") {
    expectQuery(
      """start s = NODE(1)
      //I can haz more comment?
      return s""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("first last and rest") {
    expectQuery(
      "start x = NODE(1) match p=x-[r]->z return head(nodes(p)), last(nodes(p)), tail(nodes(p))",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq.empty, SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, SemanticDirection.OUTGOING))).
        returns(
        ReturnItem(ContainerIndex(NodesFunction(Identifier("p")), Literal(0)), "head(nodes(p))"),
        ReturnItem(ContainerIndex(NodesFunction(Identifier("p")), Literal(-1)), "last(nodes(p))"),
        ReturnItem(CollectionSliceExpression(NodesFunction(Identifier("p")), Some(Literal(1)), None), "tail(nodes(p))"))
    )
  }

  test("filter") {
    expectQuery(
      "start x = NODE(1) match p=x-[r]->z return filter(x in nodes(p) WHERE x.prop = 123)",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq.empty, SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, SemanticDirection.OUTGOING))).
        returns(
        ReturnItem(FilterFunction(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("prop")), Literal(123))), "filter(x in nodes(p) WHERE x.prop = 123)"))
    )

    expectQuery(
      "start x = NODE(1) match p=x-[r]->z return [x in nodes(p) WHERE x.prop = 123]",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, SemanticDirection.OUTGOING))).
        returns(
        ReturnItem(FilterFunction(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("prop")), Literal(123))), "[x in nodes(p) WHERE x.prop = 123]"))
    )
  }

  test("extract") {
    expectQuery(
      "start x = NODE(1) match p=x-[r]->z return [x in nodes(p) | x.prop]",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, SemanticDirection.OUTGOING))).
        returns(
        ReturnItem(ExtractFunction(NodesFunction(Identifier("p")), "x", Property(Identifier("x"), PropertyKey("prop"))), "[x in nodes(p) | x.prop]"))
    )
  }

  test("listComprehension") {
    expectQuery(
      "start x = NODE(1) match p=x-[r]->z return [x in rels(p) WHERE x.prop > 123 | x.prop]",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, SemanticDirection.OUTGOING))).
        returns(
        ReturnItem(ExtractFunction(
          FilterFunction(RelationshipFunction(Identifier("p")), "x", GreaterThan(Property(Identifier("x"), PropertyKey("prop")), Literal(123))),
          "x",
          Property(Identifier("x"), PropertyKey("prop"))
        ), "[x in rels(p) WHERE x.prop > 123 | x.prop]"))
    )
  }

  test("collection literal") {
    expectQuery(
      "start x = NODE(1) return ['a','b','c']",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal("b"), Literal("c")), "['a','b','c']"))
    )
  }

  test("collection literal2") {
    expectQuery(
      "start x = NODE(1) return []",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(), "[]"))
    )
  }

  test("collection literal3") {
    expectQuery(
      "start x = NODE(1) return [1,2,3]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal(1), Literal(2), Literal(3)), "[1,2,3]"))
    )
  }

  test("collection literal4") {
    expectQuery(
      "start x = NODE(1) return ['a',2]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal(2)), "['a',2]"))
    )
  }

  test("in with collection literal") {
    expectQuery(
      "start x = NODE(1) where x.prop in ['a','b'] return x",
      Query.
        start(NodeById("x", 1)).
        where(AnyInCollection(Collection(Literal("a"), Literal("b")), "-_-INNER-_-", Equals(Property(Identifier("x"), PropertyKey("prop")), Identifier("-_-INNER-_-")))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("in with collection prop") {
    expectQuery(
      "start x = NODE(1) where x.prop in x.props return x",
      Query.
        start(NodeById("x", 1)).
        where(AnyInCollection(Property(Identifier("x"), PropertyKey("props")), "-_-INNER-_-", Equals(Property(Identifier("x"), PropertyKey("prop")), Identifier("-_-INNER-_-")))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("multiple relationship type in match") {
    expectQuery(
      "start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED25", Seq("REL1", "REL2", "REL3"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("multiple relationship type in varlength rel") {
    expectQuery(
      "start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED25", Seq("REL1", "REL2", "REL3"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("multiple relationship type in shortest path") {
    expectQuery(
      "start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED25", Seq("REL1", "REL2", "REL3"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("multiple relationship type in relationship predicate") {
    expectQuery(
      """start a=node(0), b=node(1) where a-[:KNOWS|:BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED34", Seq("KNOWS","BLOCKS"), SemanticDirection.BOTH, Map.empty))))).
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }


  test("first parsed pipe query") {
    expectQuery(
      "START x = node(1) WITH x WHERE x.foo = 42 RETURN x", {
      val secondQ = Query.
        start().
        where(Equals(Property(Identifier("x"), PropertyKey("foo")), Literal(42))).
        returns(ReturnItem(Identifier("x"), "x"))

      Query.
        start(NodeById("x", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("x"), "x"))
    })
  }

  test("read first and update next") {
    expectQuery(
      "start a = node(1) with a create (b {age : a.age * 2}) return b", {
      val secondQ = Query.
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), PropertyKey("age")), Literal(2.0))), Seq.empty))).
        returns(ReturnItem(Identifier("b"), "b"))

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  test("variable length path with collection for relationships") {
    expectQuery(
      "start a=node(0) optional match a -[r*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED33", SingleNode("a"), SingleNode("x"), Some(1), Some(3), Seq(), SemanticDirection.OUTGOING, Some("r"), Map.empty)).
        makeOptional().
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  test("binary precedence") {
    expectQuery(
      """start n=node(0) where n.a = 'x' and n.b = 'x' xor n.c = 'x' or n.d = 'x' return n""",
      Query.
        start(NodeById("n", 0)).
        where(
        Or(
          Xor(
            And(
              Equals(Property(Identifier("n"), PropertyKey("a")), Literal("x")),
              Equals(Property(Identifier("n"), PropertyKey("b")), Literal("x"))
            ),
            Equals(Property(Identifier("n"), PropertyKey("c")), Literal("x"))
          ),
          Equals(Property(Identifier("n"), PropertyKey("d")), Literal("x"))
        )
      ).returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("create node") {
    expectQuery(
      "create a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), Seq.empty))).
        returns()
    )
  }

  test("create node from param") {
    expectQuery(
      "create ({param})",
      Query.
        start(CreateNodeStartItem(CreateNode("  UNNAMED7", Map("*" -> ParameterExpression("param")), Seq.empty))).
        returns()
    )
  }

  test("create node with a property") {
    expectQuery(
      "create (a {name : 'Andres'})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty))).
        returns()
    )
  }

  test("create node with a property and return it") {
    expectQuery(
      "create (a {name : 'Andres'}) return a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("create node from map expression") {
    expectQuery(
      "create (a {param})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("*" -> ParameterExpression("param")), Seq.empty))).
        returns()
    )
  }

  test("create node with a label") {
    expectQuery(
      "create (a:FOO)",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO")))).
        returns()
    )
  }

  test("create node with multiple labels") {
    expectQuery(
      "create (a:FOO:BAR)",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO", "BAR")))).
        returns()
    )
  }

  test("create node with multiple labels with spaces") {
    expectQuery(
      "create (a :FOO :BAR)",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO", "BAR")))).
        returns()
    )
  }

  test("create nodes with labels and a rel") {
    expectQuery(
      "CREATE (n:Person:Husband)-[:FOO]->(x:Person)",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED25",
        RelationshipEndpoint(Identifier("n"),Map(), LabelSupport.labelCollection("Person", "Husband")),
        RelationshipEndpoint(Identifier("x"),Map(), LabelSupport.labelCollection("Person")), "FOO", Map()))).
        returns()
    )
  }

  test("start with two nodes and create relationship") {
    expectQuery(
      "start a=node(0), b=node(1) with a,b create a-[r:REL]->b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("b"),Map(), Seq.empty), "REL", Map()))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    })
  }

  test("start with two nodes and create relationship make outgoing") {
    expectQuery(
      "start a=node(0), b=node(1) create a<-[r:REL]-b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("a"),Map(), Seq.empty), "REL", Map()))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("start with two nodes and create relationship make outgoing named") {
    expectQuery(
      "start a=node(0), b=node(1) create p=a<-[r:REL]-b return p", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("a"),Map(), Seq.empty), "REL", Map()))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), SemanticDirection.INCOMING))).
        returns(ReturnItem(Identifier("p"), "p"))

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("create relationship with properties") {
    expectQuery(
      "start a=node(0), b=node(1) with a,b create a-[r:REL {why : 42, foo : 'bar'}]->b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"),Map(),Seq.empty),
        RelationshipEndpoint(Identifier("b"),Map(),Seq.empty), "REL", Map("why" -> Literal(42), "foo" -> Literal("bar"))))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    })
  }

  test("create relationship without identifier") {
    expectQuery(
      "create (a {a})-[:REL]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED14",
        RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")),Seq.empty),
        RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")),Seq.empty),
        "REL", Map()))).
        returns()
    )
  }

  test("create relationship with properties from map") {
    expectQuery(
      "create (a {a})-[:REL {param}]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED14",
        RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")), Seq.empty),
        RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")), Seq.empty),
        "REL", Map("*" -> ParameterExpression("param"))))).
        returns()
    )
  }

  test("create relationship without identifier2") {
    expectQuery(
      "create (a {a})-[:REL]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED14",
        RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")), Seq.empty),
        RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")), Seq.empty),
        "REL", Map()))).
        returns()
    )
  }

  test("delete node") {
    expectQuery(
      "start a=node(0) with a delete a", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("a"), forced = false)).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  test("simple delete node") {
    expectQuery(
      "start a=node(0) delete a", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("a"), forced = false)).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("delete rel") {
    expectQuery(
      "start a=node(0) match (a)-[r:REL]->(b) delete r", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("r"), forced = false)).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", SemanticDirection.OUTGOING)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("delete path") {
    expectQuery(
      "start a=node(0) match p=(a)-[r:REL]->(b) delete p", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("p"), forced = false)).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), SemanticDirection.OUTGOING))).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("set property on node") {
    expectQuery(
      "start a=node(0) with a set a.hello = 'world'", {
      val secondQ = Query.
        updates(PropertySetAction(Property(Identifier("a"), PropertyKey("hello")), Literal("world"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  test("set property on node from expression") {
    expectQuery(
      "start a=node(0) with a set (a).hello = 'world'", {
      val secondQ = Query.
        updates(PropertySetAction(Property(Identifier("a"), PropertyKey("hello")), Literal("world"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  test("set multiple properties on node") {
    expectQuery(
      "start a=node(0) with a set a.hello = 'world', a.foo = 'bar'", {
      val secondQ = Query.
        updates(
        PropertySetAction(Property(Identifier("a"), PropertyKey("hello")), Literal("world")),
        PropertySetAction(Property(Identifier("a"), PropertyKey("foo")), Literal("bar"))
      ).returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  test("update property with expression") {
    expectQuery(
      "start a=node(0) with a set a.salary = a.salary * 2 ", {
      val secondQ = Query.
        updates(PropertySetAction(Property(Identifier("a"), PropertyKey("salary")), Multiply(Property(Identifier("a"), PropertyKey("salary")), Literal(2.0)))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  test("remove property") {
    expectQuery(
      "start a=node(0) remove a.salary", {
      val secondQ = Query.
        updates(DeletePropertyAction(Identifier("a"), PropertyKey("salary"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("foreach on path") {
    expectQuery(
      "start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) | set n.touched = true ) ", {
      val secondQ = Query.
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), PropertyKey("touched")), True())))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), SemanticDirection.OUTGOING))).
        tail(secondQ).
        returns(ReturnItem(Identifier("p"), "p"))
    })
  }

  test("foreach on path with multiple updates") {
    expectQuery(
      "match n foreach(n in [1,2,3] | create (x)-[r1:HAS]->(z) create (x)-[r2:HAS]->(z2) )", {
      val secondQ = Query.
        updates(ForeachAction(Collection(Literal(1), Literal(2), Literal(3)), "n", Seq(
        CreateRelationship("r1", RelationshipEndpoint("x"), RelationshipEndpoint("z"), "HAS", Map.empty),
        CreateRelationship("r2", RelationshipEndpoint("x"), RelationshipEndpoint("z2"), "HAS", Map.empty)
      ))).
        returns()

      Query.
        matches(SingleNode("n")).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("simple read first and update next") {
    expectQuery(
      "start a = node(1) create (b {age : a.age * 2}) return b", {
      val secondQ = Query.
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), PropertyKey("age")), Literal(2.0))), Seq.empty))).
        returns(ReturnItem(Identifier("b"), "b"))

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("simple start with two nodes and create relationship") {
    expectQuery(
      "start a=node(0), b=node(1) create a-[r:REL]->b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty), "REL", Map()))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("simple create relationship with properties") {
    expectQuery(
      "start a=node(0), b=node(1) create a<-[r:REL {why : 42, foo : 'bar'}]-b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty), "REL",
        Map("why" -> Literal(42), "foo" -> Literal("bar"))
      ))).returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("simple set property on node") {
    expectQuery(
      "start a=node(0) set a.hello = 'world'", {
      val secondQ = Query.
        updates(PropertySetAction(Property(Identifier("a"), PropertyKey("hello")), Literal("world"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("simple update property with expression") {
    expectQuery(
      "start a=node(0) set a.salary = a.salary * 2 ", {
      val secondQ = Query.
        updates(PropertySetAction(Property(Identifier("a"), PropertyKey("salary")), Multiply(Property(Identifier("a"),PropertyKey( "salary")), Literal(2.0)))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("simple foreach on path") {
    expectQuery(
      "start a=node(0) match p = a-[r:REL]->b foreach(n in nodes(p) | set n.touched = true ) ", {
      val secondQ = Query.
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), PropertyKey("touched")), True())))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", SemanticDirection.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), SemanticDirection.OUTGOING))).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("returnAll") {
    expectQuery(
      "start s = NODE(1) return *",
      Query.
        start(NodeById("s", 1)).
        returns(AllIdentifiers()))
  }

  test("single create unique") {
    expectQuery(
      "start a = node(1), b=node(2) create unique a-[:reltype]->b", {
      val secondQ = Query.
        unique(UniqueLink("a", "b", "  UNNAMED44", "reltype", SemanticDirection.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("single create unique with rel") {
    expectQuery(
      "start a = node(1), b=node(2) create unique a-[r:reltype]->b", {
      val secondQ = Query.
        unique(UniqueLink("a", "b", "r", "reltype", SemanticDirection.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("single relate with empty parenthesis") {
    expectQuery(
      "start a = node(1), b=node(2) create unique a-[:reltype]->()", {
      val secondQ = Query.
        unique(UniqueLink("a", "  UNNAMED57", "  UNNAMED44", "reltype", SemanticDirection.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("create unique with two patterns") {
    expectQuery(
      "start a = node(1) create unique a-[:X]->b<-[:X]-c", {
      val secondQ = Query.
        unique(
        UniqueLink("a", "b", "  UNNAMED33", "X", SemanticDirection.OUTGOING),
        UniqueLink("c", "b", "  UNNAMED41", "X", SemanticDirection.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("relate with initial values for node") {
    expectQuery(
      "start a = node(1) create unique a-[:X]->(b {name:'Andres'})", {
      val secondQ = Query.
        unique(
        UniqueLink(
          NamedExpectation("a"),
          NamedExpectation("b", Map[String, Expression]("name" -> Literal("Andres"))),
          NamedExpectation("  UNNAMED33"), "X", SemanticDirection.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("create unique with initial values for rel") {
    expectQuery(
      "start a = node(1) create unique a-[:X {name:'Andres'}]->b", {
      val secondQ = Query.
        unique(
        UniqueLink(
          NamedExpectation("a"),
          NamedExpectation("b"),
          NamedExpectation("  UNNAMED33", Map[String, Expression]("name" -> Literal("Andres"))), "X", SemanticDirection.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("foreach with literal collection") {
    val tail = Query.
      updates(ForeachAction(Collection(Literal(1.0), Literal(2.0), Literal(3.0)), "x", Seq(CreateNode("a", Map("number" -> Identifier("x")), Seq.empty)))).
      returns()

    expectQuery(
      "create root foreach(x in [1,2,3] | create (a {number:x}))",
      Query.
        start(CreateNodeStartItem(CreateNode("root", Map.empty, Seq.empty))).
        tail(tail).
        returns(AllIdentifiers())
    )
  }

  test("string literals should not be mistaken for identifiers") {
    expectQuery(
      "create (tag1 {name:'tag2'}), (tag2 {name:'tag1'})",
      Query.
        start(
        CreateNodeStartItem(CreateNode("tag1", Map("name" -> Literal("tag2")), Seq.empty)),
        CreateNodeStartItem(CreateNode("tag2", Map("name" -> Literal("tag1")), Seq.empty))
      ).returns()
    )
  }

  test("relate with two rels to same node") {
    expectQuery(
      "start root=node(0) create unique x<-[r1:X]-root-[r2:Y]->x return x", {
      val returns = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(
        UniqueLink("root", "x", "r1", "X", SemanticDirection.OUTGOING),
        UniqueLink("root", "x", "r2", "Y", SemanticDirection.OUTGOING))))
        .returns(ReturnItem(Identifier("x"), "x"))

      Query.start(NodeById("root", 0)).tail(returns).returns(AllIdentifiers())
    })
  }

  test("optional shortest path") {
    expectQuery(
      """start a  = node(1), x = node(2,3)
         optional match p = shortestPath(a -[*]-> x)
         return *""",
      Query.
        start(NodeById("a", 1),NodeById("x", 2,3)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("x"), Seq(), SemanticDirection.OUTGOING, false, None, single = true, relIterator = None)).
        makeOptional().
        returns(AllIdentifiers())
    )
  }

  test("return paths") {
    expectQuery(
      "start a  = node(1) return a-->()",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED30"), "  UNNAMED27", Seq(), SemanticDirection.OUTGOING, Map.empty))), "a-->()"))
    )
  }

  test("not with parenthesis") {
    expectQuery(
      "start a  = node(1) where not(1=2) or 2=3 return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(Not(Equals(Literal(1), Literal(2))), Equals(Literal(2), Literal(3)))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("precedence of not without parenthesis") {
    expectQuery(
      "start a = node(1) where not true or false return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(Not(True()), Not(True()))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
    expectQuery(
      "start a = node(1) where not 1 < 2 return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(LessThan(Literal(1), Literal(2)))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("not with pattern") {
    def parsedQueryWithOffsets(offset1: Int, offset2: Int) = Query.
      matches(SingleNode("admin")).
      where(Not(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("admin"), SingleNode("  UNNAMED" + offset2), "  UNNAMED" + offset1, Seq("MEMBER_OF"), SemanticDirection.OUTGOING, Map.empty)))))).
      returns(ReturnItem(Identifier("admin"), "admin"))

    expectQuery(
      "MATCH (admin) WHERE NOT (admin)-[:MEMBER_OF]->() RETURN admin",
      parsedQueryWithOffsets(31, 46))

    expectQuery(
      "MATCH (admin) WHERE NOT ((admin)-[:MEMBER_OF]->()) RETURN admin",
      parsedQueryWithOffsets(32, 47))
  }

  test("full path in create") {
    expectQuery(
      "start a=node(1), b=node(2) create a-[r1:KNOWS]->()-[r2:LOVES]->b", {
      val secondQ = Query.
        start(
        CreateRelationshipStartItem(CreateRelationship("r1",
          RelationshipEndpoint(Identifier("a"), Map(), Seq.empty),
          RelationshipEndpoint(Identifier("  UNNAMED48"), Map(), Seq.empty), "KNOWS", Map())),
        CreateRelationshipStartItem(CreateRelationship("r2",
          RelationshipEndpoint(Identifier("  UNNAMED48"), Map(), Seq.empty),
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty), "LOVES", Map()))).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("create and assign to path identifier") {
    expectQuery(
      "create p = a-[r:KNOWS]->() return p",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("  UNNAMED24"), Map(), Seq.empty), "KNOWS", Map()))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "  UNNAMED24", Seq("KNOWS"), SemanticDirection.OUTGOING))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  test("relate and assign to path identifier") {
    expectQuery(
      "start a=node(0) create unique p = a-[r:KNOWS]->() return p", {
      val q2 = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink("a", "  UNNAMED47", "r", "KNOWS", SemanticDirection.OUTGOING)))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "  UNNAMED47", Seq("KNOWS"), SemanticDirection.OUTGOING))).
        returns(ReturnItem(Identifier("p"), "p"))

      Query.
        start(NodeById("a", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("use predicate as expression") {
    expectQuery(
      "start n=node(0) return id(n) = 0, n is null",
      Query.
        start(NodeById("n", 0)).
        returns(
        ReturnItem(Equals(IdFunction(Identifier("n")), Literal(0)), "id(n) = 0"),
        ReturnItem(IsNull(Identifier("n")), "n is null")
      ))
  }

  test("create unique should support parameter maps") {
    expectQuery(
      "START n=node(0) CREATE UNIQUE n-[:foo]->({param}) RETURN *", {
      val start = NamedExpectation("n")
      val rel = NamedExpectation("  UNNAMED31")
      val end = NamedExpectation("  UNNAMED40", Map("*" -> ParameterExpression("param")), Seq.empty)
      val secondQ = Query.
        unique(UniqueLink(start, end, rel, "foo", SemanticDirection.OUTGOING)).
        returns(AllIdentifiers())

      Query.
        start(NodeById("n", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("with limit") {
    expectQuery(
      "start n=node(0,1,2) with n limit 2 where ID(n) = 1 return n",
      Query.
        start(NodeById("n", 0, 1, 2)).
        limit(2).
        tail(Query.
        start().
        where(Equals(IdFunction(Identifier("n")), Literal(1))).
        returns(ReturnItem(Identifier("n"), "n"))
      ).
        returns(
        ReturnItem(Identifier("n"), "n")
      ))
  }

  test("with sort limit") {
    expectQuery(
      "start n=node(0,1,2) with n order by ID(n) desc limit 2 where ID(n) = 1 return n",
      Query.
        start(NodeById("n", 0, 1, 2)).
        orderBy(SortItem(IdFunction(Identifier("n")), false)).
        limit(2).
        tail(Query.
        start().
        where(Equals(IdFunction(Identifier("n")), Literal(1))).
        returns(ReturnItem(Identifier("n"), "n"))
      ).
        returns(
        ReturnItem(Identifier("n"), "n")
      ))
  }

  test("set to param") {
    val q2 = Query.
      start().
      updates(MapPropertySetAction(Identifier("n"), ParameterExpression("prop"), removeOtherProps = true)).
      returns()

    expectQuery(
      "start n=node(0) set n = {prop}",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers()))
  }

  test("inclusive set to param") {
    val q2 = Query.
      start().
      updates(MapPropertySetAction(Identifier("n"), ParameterExpression("prop"), removeOtherProps = false)).
      returns()

    expectQuery(
      "start n=node(0) set n += {prop}",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers()))
  }

  test("set to map") {
    expectQuery(
      "start n=node(0) set n = {key: 'value', foo: 1}", {
      val q2 = Query.
        start().
        updates(MapPropertySetAction(Identifier("n"), LiteralMap(Map("key" -> Literal("value"), "foo" -> Literal(1))), removeOtherProps = true)).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("add label") {
    expectQuery(
      "START n=node(0) set n:LabelName", {
      val q2 = Query.
        start().
        updates(LabelAction(Identifier("n"), LabelSetOp, List(KeyToken.Unresolved("LabelName", TokenType.Label)))).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("add short label") {
    expectQuery(
      "START n=node(0) SET n:LabelName", {
      val q2 = Query.
        start().
        updates(LabelAction(Identifier("n"), LabelSetOp, List(KeyToken.Unresolved("LabelName", TokenType.Label)))).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("add multiple labels") {
    expectQuery(
      "START n=node(0) set n :LabelName2 :LabelName3", {
      val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
      val q2   = Query.
        start().
        updates(LabelAction(Identifier("n"), LabelSetOp, coll)).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("add multiple short labels") {
    expectQuery(
      "START n=node(0) set n:LabelName2:LabelName3", {
      val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
      val q2   = Query.
        start().
        updates(LabelAction(Identifier("n"), LabelSetOp, coll)).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("add multiple short labels2") {
    expectQuery(
      "START n=node(0) SET n :LabelName2 :LabelName3", {
      val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
      val q2   = Query.
        start().
        updates(LabelAction(Identifier("n"), LabelSetOp, coll)).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("remove label") {
    expectQuery(
      "START n=node(0) REMOVE n:LabelName", {
      val q2 = Query.
        start().
        updates(LabelAction(Identifier("n"), LabelRemoveOp, LabelSupport.labelCollection("LabelName"))).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("remove multiple labels") {
    expectQuery(
      "START n=node(0) REMOVE n:LabelName2:LabelName3", {
      val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
      val q2   = Query.
        start().
        updates(LabelAction(Identifier("n"), LabelRemoveOp, coll)).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  test("filter by label in where") {
    expectQuery(
      "START n=node(0) WHERE (n):Foo RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(HasLabel(Identifier("n"), KeyToken.Unresolved("Foo", TokenType.Label))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("filter by label in where with expression") {
    expectQuery(
      "START n=node(0) WHERE (n):Foo RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(HasLabel(Identifier("n"), KeyToken.Unresolved("Foo", TokenType.Label))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("filter by labels in where") {
    expectQuery(
      "START n=node(0) WHERE n:Foo:Bar RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(And(HasLabel(Identifier("n"), KeyToken.Unresolved("Foo", TokenType.Label)), HasLabel(Identifier("n"), KeyToken.Unresolved("Bar", TokenType.Label)))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("create no index without properties") {
    evaluating {
      expectQuery(
        "create index on :MyLabel",
        CreateIndex("MyLabel", Seq()))
    } should produce[SyntaxException]
  }

  test("create index on single property") {
    expectQuery(
      "create index on :MyLabel(prop1)",
      CreateIndex("MyLabel", Seq("prop1")))
  }

  test("create index on multiple properties") {
    evaluating {
      expectQuery(
        "create index on :MyLabel(prop1, prop2)",
        CreateIndex("MyLabel", Seq("prop1", "prop2")))
    } should produce[SyntaxException]
  }

  test("match left with single label") {
    expectQuery(
      "start a = NODE(1) match (a:foo) -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a", Seq(UnresolvedLabel("foo"))), SingleNode("  UNNAMED47"), "r", Seq("MARRIED"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("match left with multiple labels") {
    expectQuery(
      "start a = NODE(1) match (a:foo:bar) -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a", Seq(UnresolvedLabel("foo"),UnresolvedLabel("bar"))), SingleNode("  UNNAMED51"), "r", Seq("MARRIED"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("match right with multiple labels") {
    expectQuery(
      "start a = NODE(1) match () -[r:MARRIED]-> (a:foo:bar) return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("  UNNAMED24", Seq()), SingleNode("a", Seq(UnresolvedLabel("foo"), UnresolvedLabel("bar"))), "r", Seq("MARRIED"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("match both with labels") {
    expectQuery(
      "start a = NODE(1) match (b:foo) -[r:MARRIED]-> (a:bar) return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("b", Seq(UnresolvedLabel("foo"))), SingleNode("a", Seq(UnresolvedLabel("bar"))), "r", Seq("MARRIED"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  test("union ftw") {
    val q1 = Query.
      start(NodeById("s", 1)).
      returns(ReturnItem(Identifier("s"), "s"))
    val q2 = Query.
      start(NodeById("t", 1)).
      returns(ReturnItem(Identifier("t"), "t"))
    val q3 = Query.
      start(NodeById("u", 1)).
      returns(ReturnItem(Identifier("u"), "u"))

    expectQuery(
      "start s = NODE(1) return s UNION all start t = NODE(1) return t UNION all start u = NODE(1) return u",
      Union(Seq(q1, q2, q3), QueryString.empty, distinct = false)
    )
  }

  test("union distinct") {
    val q = Query.
      start(NodeById("s", 1)).
      returns(ReturnItem(Identifier("s"), "s"))

    expectQuery(
      "start s = NODE(1) return s UNION start s = NODE(1) return s UNION start s = NODE(1) return s",
      Union(Seq(q, q, q), QueryString.empty, distinct = true)
    )
  }

  test("multiple unions") {
    val q = Query.
      matches(SingleNode("n")).
      limit(1).
      returns(ReturnItem(Identifier("n"), "(n)"))

    expectQuery(
      "MATCH (n) RETURN (n) LIMIT 1 UNION MATCH (n) RETURN (n) LIMIT 1 UNION MATCH (n) RETURN (n) LIMIT 1",
      Union(Seq(q, q, q), QueryString.empty, distinct = true)
    )
  }

  test("keywords in reltype and label") {
    expectQuery(
      "START n=node(0) MATCH (n:On)-[:WHERE]->() RETURN n",
      Query.
        start(NodeById("n", 0)).
        matches(RelatedTo(SingleNode("n", Seq(UnresolvedLabel("On"))), SingleNode("  UNNAMED39"), "  UNNAMED28", Seq("WHERE"), SemanticDirection.OUTGOING, Map.empty)).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("remove index on single property") {
    expectQuery(
      "drop index on :MyLabel(prop1)",
      DropIndex("MyLabel", Seq("prop1"))
    )
  }

  test("simple query with index hint") {
    expectQuery(
      "match (n:Person)-->() using index n:Person(name) where n.name = 'Andres' return n",
      Query.matches(RelatedTo(SingleNode("n", Seq(UnresolvedLabel("Person"))), SingleNode("  UNNAMED19"), "  UNNAMED16", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        where(Equals(Property(Identifier("n"), PropertyKey("name")), Literal("Andres"))).
        using(SchemaIndex("n", "Person", "name", AnyIndex, None)).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("simple in expression with index hint") {
    expectQuery(
      "match (n:Person)-->() using index n:Person(name) where n.name IN ['Andres'] return n",
      Query.matches(RelatedTo(SingleNode("n", Seq(UnresolvedLabel("Person"))), SingleNode("  UNNAMED19"), "  UNNAMED16", Seq(), SemanticDirection.OUTGOING, Map.empty)).
        where(AnyInCollection(Collection(Literal("Andres")), "-_-INNER-_-",  Equals(Property(Identifier("n"), PropertyKey("name")), Identifier("-_-INNER-_-")))).
        using(SchemaIndex("n", "Person", "name", AnyIndex, None)).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("single node match pattern") {
    expectQuery(
      "start s = node(*) match s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("awesome single labeled node match pattern") {
    expectQuery(
      "match (s:nostart) return s",
      Query.
        matches(SingleNode("s", Seq(UnresolvedLabel("nostart")))).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("single node match pattern path") {
    expectQuery(
      "start s = node(*) match p = s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        namedPaths(NamedPath("p", ParsedEntity("s"))).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  test("label scan hint") {
    expectQuery(
      "match (p:Person) using scan p:Person return p",
      Query.
        matches(SingleNode("p", Seq(UnresolvedLabel("Person")))).
        using(NodeByLabel("p", "Person")).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("varlength named path") {
    expectQuery(
      "start n=node(1) match p=n-[:KNOWS*..2]->x return p",
      Query.
        start(NodeById("n", 1)).
        matches(VarLengthRelatedTo("  UNNAMED25", SingleNode("n"), SingleNode("x"), None, Some(2), Seq("KNOWS"), SemanticDirection.OUTGOING, None, Map.empty)).
        namedPaths(NamedPath("p", ParsedVarLengthRelation("  UNNAMED25", Map.empty, ParsedEntity("n"), ParsedEntity("x"), Seq("KNOWS"), SemanticDirection.OUTGOING, optional = false, None, Some(2), None))).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("reduce function") {
    val collection = Collection(Literal(1), Literal(2), Literal(3))
    val expression = Add(Identifier("acc"), Identifier("x"))
    expectQuery(
      "start n=node(1) return reduce(acc = 0, x in [1,2,3] | acc + x)",
      Query.
        start(NodeById("n", 1)).
        returns(ReturnItem(ReduceFunction(collection, "x", expression, "acc", Literal(0)), "reduce(acc = 0, x in [1,2,3] | acc + x)"))
    )
  }

  test("start and endNode") {
    expectQuery(
      "start r=rel(1) return startNode(r), endNode(r)",
      Query.
        start(RelationshipById("r", 1)).
        returns(
        ReturnItem(RelationshipEndPoints(Identifier("r"), start = true), "startNode(r)"),
        ReturnItem(RelationshipEndPoints(Identifier("r"), start = false), "endNode(r)"))
    )
  }

  test("mathy aggregation expressions") {
    val property = Property(Identifier("n"), PropertyKey("property"))
    val percentileCont = PercentileCont(property, Literal(0.4))
    val percentileDisc = PercentileDisc(property, Literal(0.5))
    val stdev = Stdev(property)
    val stdevP = StdevP(property)
    expectQuery(
      "match n return percentileCont(n.property, 0.4), percentileDisc(n.property, 0.5), stdev(n.property), stdevp(n.property)",
      Query.
        matches(SingleNode("n")).
        aggregation(percentileCont, percentileDisc, stdev, stdevP).
        returns(
        ReturnItem(percentileCont, "percentileCont(n.property, 0.4)"),
        ReturnItem(percentileDisc, "percentileDisc(n.property, 0.5)"),
        ReturnItem(stdev, "stdev(n.property)"),
        ReturnItem(stdevP, "stdevp(n.property)"))
    )
  }

  test("escaped identifier") {
    expectQuery(
      "match `Unusual identifier` return `Unusual identifier`.propertyName",
      Query.
        matches(SingleNode("Unusual identifier")).
        returns(
        ReturnItem(Property(Identifier("Unusual identifier"), PropertyKey("propertyName")), "`Unusual identifier`.propertyName"))
    )
  }

  test("aliased column does not keep escape symbols") {
    expectQuery(
      "match a return a as `Escaped alias`",
      Query.
        matches(SingleNode("a")).
        returns(
        ReturnItem(Identifier("a"), "Escaped alias"))
    )
  }

  test("create with labels and props with parens") {
    expectQuery(
      "CREATE (node :FOO:BAR {name: 'Stefan'})",
      Query.
        start(CreateNodeStartItem(CreateNode("node", Map("name"->Literal("Stefan")),
        LabelSupport.labelCollection("FOO", "BAR")))).
        returns()
    )
  }

  test("constraint creation") {
    expectQuery(
      "CREATE CONSTRAINT ON (id:Label) ASSERT id.property IS UNIQUE",
      CreateUniqueConstraint("id", "Label", "id", "property")
    )
  }

  test("node property existence constraint creation") {
    expectQuery(
      "CREATE CONSTRAINT ON (id:Label) ASSERT exists(id.property)",
      CreateNodePropertyExistenceConstraint("id", "Label", "id", "property")
    )
  }

  test("node property existence constraint deletion") {
    expectQuery(
      "DROP CONSTRAINT ON (id:Label) ASSERT exists(id.property)",
      DropNodePropertyExistenceConstraint("id", "Label", "id", "property")
    )
  }

  test("relationship property existence constraint creation") {
    expectQuery(
      "CREATE CONSTRAINT ON ()-[id:RelType]-() ASSERT exists(id.property)",
      CreateRelationshipPropertyExistenceConstraint("id", "RelType", "id", "property")
    )
    expectQuery(
      "CREATE CONSTRAINT ON ()-[id:RelType]->() ASSERT exists(id.property)",
      CreateRelationshipPropertyExistenceConstraint("id", "RelType", "id", "property")
    )
    expectQuery(
      "CREATE CONSTRAINT ON ()<-[id:RelType]-() ASSERT exists(id.property)",
      CreateRelationshipPropertyExistenceConstraint("id", "RelType", "id", "property")
    )
  }

  test("relationship property existence constraint deletion") {
    expectQuery(
      "DROP CONSTRAINT ON ()-[id:RelType]-() ASSERT exists(id.property)",
      DropRelationshipPropertyExistenceConstraint("id", "RelType", "id", "property")
    )
    expectQuery(
      "DROP CONSTRAINT ON ()-[id:RelType]->() ASSERT exists(id.property)",
      DropRelationshipPropertyExistenceConstraint("id", "RelType", "id", "property")
    )
    expectQuery(
      "DROP CONSTRAINT ON ()<-[id:RelType]-() ASSERT exists(id.property)",
      DropRelationshipPropertyExistenceConstraint("id", "RelType", "id", "property")
    )
  }

  test("named path with variable length path and named relationships collection") {
    expectQuery(
      "match p = (a)-[r*]->(b) return p",
      Query.
        matches(VarLengthRelatedTo("  UNNAMED13", SingleNode("a"), SingleNode("b"), None, None, Seq.empty, SemanticDirection.OUTGOING, Some("r"), Map.empty)).
        namedPaths(NamedPath("p", ParsedVarLengthRelation("  UNNAMED13", Map.empty, ParsedEntity("a"), ParsedEntity("b"), Seq.empty, SemanticDirection.OUTGOING, optional = false, None, None, Some("r")))).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  test("variable length relationship with rel collection") {
    expectQuery(
      "MATCH (a)-[rels*]->(b) WHERE ALL(r in rels WHERE r.prop = 42) RETURN rels",
      Query.
        matches(VarLengthRelatedTo("  UNNAMED9", SingleNode("a"), SingleNode("b"), None, None, Seq.empty, SemanticDirection.OUTGOING, Some("rels"), Map.empty)).
        where(AllInCollection(Identifier("rels"), "r", Equals(Property(Identifier("r"), PropertyKey("prop")), Literal(42)))).
        returns(ReturnItem(Identifier("rels"), "rels"))
    )
  }

  test("simple case statement") {
    expectQuery(
      "MATCH (a) RETURN CASE a.prop WHEN 1 THEN 'hello' ELSE 'goodbye' END AS result",
      Query.
        matches(SingleNode("a")).
        returns(
        ReturnItem(SimpleCase(Property(Identifier("a"), PropertyKey("prop")), Seq(
          (Literal(1), Literal("hello"))
        ), Some(Literal("goodbye"))), "result"))
    )
  }

  test("generic case statement") {
    expectQuery(
      "MATCH (a) RETURN CASE WHEN a.prop = 1 THEN 'hello' ELSE 'goodbye' END AS result",
      Query.
        matches(SingleNode("a")).
        returns(
        ReturnItem(GenericCase(Seq(
          (Equals(Property(Identifier("a"), PropertyKey("prop")), Literal(1)), Literal("hello"))
        ), Some(Literal("goodbye"))), "result"))
    )
  }

  test("genericCaseCoercesInWhen") {
    expectQuery(
      """MATCH (a) RETURN CASE WHEN (a)-[:LOVES]->() THEN 1 ELSE 0 END AS result""".stripMargin,
      Query.
        matches(SingleNode("a")).
        returns(
          ReturnItem(GenericCase(
            Seq((NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED41"), "  UNNAMED30", Seq("LOVES"), SemanticDirection.OUTGOING, Map.empty)))), Literal(1))),
            Some(Literal(0))
          ), "result")
        )
    )
  }

  test("shouldGroupCreateAndCreateUpdate") {
    expectQuery(
      """START me=node(0) MATCH p1 = me-[*2]-friendOfFriend CREATE p2 = me-[:MARRIED_TO]->(wife {name:"Gunhild"}) CREATE UNIQUE p3 = wife-[:KNOWS]-friendOfFriend RETURN p1,p2,p3""", {
      val thirdQ = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink("wife", "friendOfFriend", "  UNNAMED128", "KNOWS", SemanticDirection.BOTH)))).
        namedPaths(NamedPath("p3", ParsedRelation("  UNNAMED128", "wife", "friendOfFriend", Seq("KNOWS"), SemanticDirection.BOTH))).
        returns(ReturnItem(Identifier("p1"), "p1"), ReturnItem(Identifier("p2"), "p2"), ReturnItem(Identifier("p3"), "p3"))

      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED65",
        RelationshipEndpoint(Identifier("me"),Map.empty, Seq.empty),
        RelationshipEndpoint(Identifier("wife"), Map("name" -> Literal("Gunhild")), Seq.empty),
        "MARRIED_TO", Map()))).
        namedPaths(NamedPath("p2", new ParsedRelation("  UNNAMED65", Map(),
        ParsedEntity("me"),
        ParsedEntity("wife", Identifier("wife"), Map("name" -> Literal("Gunhild")), Seq.empty),
        Seq("MARRIED_TO"), SemanticDirection.OUTGOING, false))).
        tail(thirdQ).
        returns(AllIdentifiers())

      Query.start(NodeById("me", 0)).
        matches(VarLengthRelatedTo("  UNNAMED30", SingleNode("me"), SingleNode("friendOfFriend"), Some(2), Some(2), Seq.empty, SemanticDirection.BOTH, None, Map.empty)).
        namedPaths(NamedPath("p1", ParsedVarLengthRelation("  UNNAMED30", Map.empty, ParsedEntity("me"), ParsedEntity("friendOfFriend"), Seq.empty, SemanticDirection.BOTH, false, Some(2), Some(2), None))).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  test("return only query with literal map") {
    expectQuery(
      "RETURN { key: 'value' }",
      Query.
        matches().
        returns(
        ReturnItem(LiteralMap(Map("key"->Literal("value"))), "{ key: 'value' }"))
    )
  }

  test("access nested properties") {
    val tail = Query.
      matches().
      returns(
        ReturnItem(Property(Property(Identifier("person"), PropertyKey("address")), PropertyKey("city")), "person.address.city")
      )

    expectQuery(
      "WITH { name:'Alice', address: { city:'London', residential:true }} AS person RETURN person.address.city",
      Query.
        matches().
        tail(tail).
        returns(
          ReturnItem(LiteralMap(
            Map("name"->Literal("Alice"), "address"->LiteralMap(
              Map("city"->Literal("London"), "residential"->True())
            ))
          ), "person")
        )
    )
  }

  test("long match chain") {
    expectQuery("match (a)<-[r1:REL1]-(b)<-[r2:REL2]-(c) return a, b, c",
      Query.
        matches(
        RelatedTo("a", "b", "r1", Seq("REL1"), SemanticDirection.INCOMING),
        RelatedTo("b", "c", "r2", Seq("REL2"), SemanticDirection.INCOMING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Identifier("c"), "c"))
    )
  }

  test("long create chain") {
    expectQuery("create (a)<-[r1:REL1]-(b)<-[r2:REL2]-(c)",
      Query.
        start(
        CreateRelationshipStartItem(CreateRelationship("r1",
          RelationshipEndpoint(Identifier("b"), Map.empty, Seq.empty),
          RelationshipEndpoint(Identifier("a"), Map.empty, Seq.empty),
          "REL1", Map())),
        CreateRelationshipStartItem(CreateRelationship("r2",
          RelationshipEndpoint(Identifier("c"), Map.empty, Seq.empty),
          RelationshipEndpoint(Identifier("b"), Map.empty, Seq.empty),
          "REL2", Map()))).
        returns()
    )
  }

  test("test literal numbers") {
    expectQuery(
      "RETURN 0.5, .5, 50, -0.3, -33, 1E-10, -4.5E23, 0x45fd, -0xdc5e",
      Query.
        matches().
        returns(
          ReturnItem(Literal(0.5), "0.5"), ReturnItem(Literal(0.5), ".5"), ReturnItem(Literal(50), "50"),
          ReturnItem(Literal(-0.3), "-0.3"), ReturnItem(Literal(-33), "-33"),
          ReturnItem(Literal(1E-10), "1E-10"), ReturnItem(Literal(-4.5E23), "-4.5E23"),
          ReturnItem(Literal(0x45fd), "0x45fd"), ReturnItem(Literal(-0xdc5e), "-0xdc5e")
        )
    )
  }

  test("test unary plus minus") {
    expectQuery(
      "MATCH n WHERE n.prop=+2 RETURN -n.prop, +n.foo, 1 + -n.bar",
      Query.
        matches(SingleNode("n")).
        where(Equals(Property(Identifier("n"), PropertyKey("prop")), Literal(2))).
        returns(ReturnItem(Subtract(Literal(0), Property(Identifier("n"), PropertyKey("prop"))), "-n.prop"),
        ReturnItem(Property(Identifier("n"), PropertyKey("foo")), "+n.foo"),
        ReturnItem(Add(Literal(1), Subtract(Literal(0), Property(Identifier("n"), PropertyKey("bar")))), "1 + -n.bar"))
    )
  }

  test("compile query integration test") {
    val parsedQuery = parser.parse("create (a1) create (a2) create (a3) create (a4) create (a5) create (a6) create (a7)")
    val q = parsedQuery.asQuery(devNullLogger).asInstanceOf[commands.Query]
    assert(q.tail.nonEmpty, "wasn't compacted enough")
    val compacted = q.compact

    assert(compacted.tail.isEmpty, "wasn't compacted enough")
    assert(compacted.start.size === 7, "lost create commands")
  }

  test("should handle optional match") {
    expectQuery(
      "OPTIONAL MATCH n RETURN n",
      Query.
        optionalMatches(SingleNode("n")).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  test("compile query integration test 2") {
    val parsedQuery = parser.parse("create (a1) create (a2) create (a3) with a1 create (a4) return a1, a4")
    val q = parsedQuery.asQuery(devNullLogger).asInstanceOf[commands.Query]
    val compacted = q.compact
    var lastQ = compacted

    while (lastQ.tail.nonEmpty)
      lastQ = lastQ.tail.get

    assert(lastQ.returns.columns === List("a1", "a4"), "Lost the tail while compacting")
  }

  test("should handle optional match following optional match") {
    val last = Query.matches(RelatedTo("n", "c", "r2", Seq.empty, SemanticDirection.INCOMING)).makeOptional().returns(AllIdentifiers())
    val second = Query.matches(RelatedTo("n", "b", "r1", Seq.empty, SemanticDirection.OUTGOING)).makeOptional().tail(last).returns(AllIdentifiers())
    val first = Query.matches(SingleNode("n")).tail(second).returns(AllIdentifiers())

    expectQuery(
      "MATCH (n) OPTIONAL MATCH (n)-[r1]->(b) OPTIONAL MATCH (n)<-[r2]-(c) RETURN *",
      first)
  }

  test("should handle match properties pointing to other parts of pattern") {
    val nodeA = SingleNode("a", Seq.empty, Map("foo"->Property(Identifier("x"), PropertyKey("bar"))))
    expectQuery(
      "MATCH (a { foo:x.bar })-->(x) RETURN *",
      Query.
        matches(RelatedTo(nodeA, SingleNode("x"), "  UNNAMED23", Seq.empty, SemanticDirection.OUTGOING, Map.empty)).
        returns(AllIdentifiers()))
  }

  test("should allow both relationships and nodes to be set with maps") {
    expectQuery(
      "MATCH (a)-[r:KNOWS]->(b) SET r = { id: 42 }",
      Query.
        matches(RelatedTo("a", "b", "r", "KNOWS", SemanticDirection.OUTGOING)).
        tail(Query.updates(MapPropertySetAction(Identifier("r"), LiteralMap(Map("id"->Literal(42))), removeOtherProps = true)).returns()).
        returns(AllIdentifiers()))
  }

  test("should allow whitespace in multiple word operators") {
    expectQuery(
      "OPTIONAL\t MATCH (n) WHERE n  IS   NOT\n /* possibly */ NULL    RETURN n",
      Query.
        optionalMatches(SingleNode("n")).
        where(Not(IsNull(Identifier("n")))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("should allow append to empty collection") {
    expectQuery(
      "return [] + 1 AS result",
      Query.
        matches().
        returns(ReturnItem(Add(Collection(), Literal(1)), "result")))
  }

  test("should handle load and return as map") {
    expectQuery(
      "LOAD CSV WITH HEADERS FROM 'file:///tmp/file.cvs' AS line RETURN line.key",
      Query.
        start(LoadCSV(withHeaders = true, new Literal("file:///tmp/file.cvs"), "line", None)).
        returns(ReturnItem(Property(Identifier("line"), PropertyKey("key")), "line.key"))
    )
  }

  test("should handle LOAD CSV with the file URL specified as a parameter") {
    expectQuery(
      "LOAD CSV WITH HEADERS FROM {path} AS line RETURN line.key",
      Query.
        start(LoadCSV(withHeaders = true, new ParameterExpression("path"), "line", None)).
        returns(ReturnItem(Property(Identifier("line"), PropertyKey("key")), "line.key"))
    )
  }

  test("should handle LOAD CSV with the file URL specified being an arbitrary expression") {

    expectQuery(
      "MATCH n WITH n LOAD CSV WITH HEADERS FROM n.path AS line RETURN line.key",
      Query.
        matches(SingleNode("n")).
          tail(Query.
          start(LoadCSV(withHeaders = true, Property(Identifier("n"), PropertyKey("path")), "line", None)).
          returns(ReturnItem(Property(Identifier("line"), PropertyKey("key")), "line.key"))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  test("should handle load and return") {
    expectQuery(
      "LOAD CSV FROM 'file:///tmp/file.cvs' AS line RETURN line",
      Query.
        start(LoadCSV(withHeaders = false, new Literal("file:///tmp/file.cvs"), "line", None)).
        returns(ReturnItem(Identifier("line"), "line"))
    )
  }

  test("should parse a periodic commit query with size followed by LOAD CSV") {
    expectQuery(
      "USING PERIODIC COMMIT 10 LOAD CSV FROM 'file:///tmp/foo.csv' AS line CREATE x",
      PeriodicCommitQuery(
        Query.
          start(LoadCSV(withHeaders = false, url = new Literal("file:///tmp/foo.csv"), identifier = "line", fieldTerminator = None)).
          tail(Query.
            start(CreateNodeStartItem(CreateNode("x", Map.empty, Seq.empty))).
            returns()
          ).
          returns(AllIdentifiers()),
        Some(10)
      )
    )
  }

  test("should parse a periodic commit query without size followed by LOAD CSV") {
    expectQuery(
      "USING PERIODIC COMMIT LOAD CSV FROM 'file:///tmp/foo.csv' AS line CREATE x",
      PeriodicCommitQuery(
        Query.
          start(LoadCSV(withHeaders = false, url = new Literal("file:///tmp/foo.csv"), identifier = "line", fieldTerminator = None)).
          tail(Query.
            start(CreateNodeStartItem(CreateNode("x", Map.empty, Seq.empty))).
            returns()
          ).
          returns(AllIdentifiers()),
        None
      )
    )
  }

  test("should reject a periodic commit query not followed by LOAD CSV") {
    intercept[SyntaxException](parser.parse("USING PERIODIC COMMIT CREATE ()"))
  }

  test("should reject a periodic commit query followed by LOAD CSV and a union") {
    intercept[SyntaxException](parser.parse("USING PERIODIC COMMIT LOAD CSV  FROM 'file:///tmp/foo.csv' AS line CREATE x UNION MATCH n RETURN n"))
  }

  private def expectQuery(query: String, expectedQuery: AbstractQuery) {
    val parsedQuery = parser.parse(query)
    val abstractQuery = parsedQuery.asQuery(devNullLogger)
    try {
      assertThat(query, abstractQuery, equalTo(expectedQuery))
    } catch {
      case x: AssertionError => throw new AssertionError(x.getMessage.replace("WrappedArray", "List"))
    }
  }
}
