/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher

import internal.commands._
import expressions._
import expressions.Add
import expressions.Identifier
import expressions.Literal
import expressions.Property
import internal.mutation._
import org.junit.Assert._
import org.neo4j.graphdb.Direction
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Ignore
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers.equalTo

class CypherParserTest extends JUnitSuite with Assertions {
  @Test def shouldParseEasiestPossibleQuery() {
    testAll("start s = NODE(1) return s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def should_return_string_literal() {
    testAll("start s = node(1) return \"apa\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("apa"), "\"apa\"")))
  }

  @Test def should_return_string_literal_with_escaped_sequence_in() {
    testFrom_1_8("start s = node(1) return \"a\\tp\\\"a\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a\tp\"a"), "\"a\\tp\\\"a\"")))
  }

  @Test def allTheNodes() {
    testAll("start s = NODE(*) return s",
      Query.
        start(AllNodes("s")).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def allTheRels() {
    testAll("start r = relationship(*) return r",
      Query.
        start(AllRelationships("r")).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def shouldHandleAliasingOfColumnNames() {
    testAll("start s = NODE(1) return s as somethingElse",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "somethingElse", true)))
  }

  @Test def sourceIsAnIndex() {
    testAll(
      """start a = node:index(key = "value") return a""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def sourceIsAnNonParsedIndexQuery() {
    testAll(
      """start a = node:index("key:value") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Ignore
  @Test def sourceIsParsedAdvancedLuceneQuery() {
    testAll(
      """start a = node:index(key="value" AND otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" AND otherKey:\"otherValue\""))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Ignore
  @Test def parsedOrIdxQuery() {
    testAll(
      """start a = node:index(key="value" or otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" OR otherKey:\"otherValue\""))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    testAll(
      "start s = relationship(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseEasiestPossibleRelationshipQueryShort() {
    testAll(
      "start s = rel(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def sourceIsARelationshipIndex() {
    testAll(
      """start a = rel:index(key = "value") return a""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def escapedNamesShouldNotContainEscapeChars() {
    testAll(
      """start `a a` = rel:`index a`(`key s` = "value") return `a a`""",
      Query.
        start(RelationshipByIndex("a a", "index a", Literal("key s"), Literal("value"))).
        returns(ReturnItem(Identifier("a a"), "a a")))
  }

  @Test def keywordsShouldBeCaseInsensitive() {
    testAll(
      "START s = NODE(1) RETURN s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseMultipleNodes() {
    testAll(
      "start s = NODE(1,2,3) return s",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseMultipleInputs() {
    testAll(
      "start a = node(1), b = NODE(2) return a,b",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def shouldFilterOnProp() {
    testAll(
      "start a = NODE(1) where a.name = \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), "name"), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldReturnLiterals() {
    testAll(
      "start a = NODE(1) return 12",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Literal(12), "12")))
  }

  @Test def shouldReturnAdditions() {
    testAll(
      "start a = NODE(1) return 12+2",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Add(Literal(12), Literal(2)), "12+2")))
  }

  @Test def arithmeticsPrecedence() {
    testAll(
      "start a = NODE(1) return 12/4*3-2*4",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(
        Subtract(
          Multiply(
            Divide(
              Literal(12),
              Literal(4)),
            Literal(3)),
          Multiply(
            Literal(2),
            Literal(4)))
        , "12/4*3-2*4")))
  }

  @Test def shouldFilterOnPropWithDecimals() {
    testAll(
      "start a = node(1) where a.extractReturnItems = 3.1415 return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), "extractReturnItems"), Literal(3.1415))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleNot() {
    testAll(
      "start a = node(1) where not(a.name = \"andres\") return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), "name"), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleNotEqualTo() {
    testAll(
      "start a = node(1) where a.name <> \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), "name"), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleLessThan() {
    testAll(
      "start a = node(1) where a.name < \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(Property(Identifier("a"), "name"), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleGreaterThan() {
    testAll(
      "start a = node(1) where a.name > \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(Property(Identifier("a"), "name"), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleLessThanOrEqual() {
    testAll(
      "start a = node(1) where a.name <= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(Property(Identifier("a"), "name"), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleRegularComparisonOld() {
    test_1_7(
      "start a = node(1) where \"Andres\" =~ /And.*/ return a",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleRegularComparison() {
    testFrom_1_8(
      "start a = node(1) where \"Andres\" =~ 'And.*' return a",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleMultipleRegularComparison1_7() {
    test_1_7(
      """start a = node(1) where a.name =~ /And.*/ AnD a.name =~ /And.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And.*")), LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And.*")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleMultipleRegularComparison() {
    testFrom_1_8(
      """start a = node(1) where a.name =~ 'And.*' AnD a.name =~ 'And.*' return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And.*")), LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And.*")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleEscapedRegexs1_7() {
    test_1_7(
      """start a = node(1) where a.name =~ /And\/.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And\\/.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleEscapedRegexs() {
    testFrom_1_8(
      """start a = node(1) where a.name =~ 'And\\/.*' return a""",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And\\/.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleGreaterThanOrEqual() {
    testAll(
      "start a = node(1) where a.name >= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThanOrEqual(Property(Identifier("a"), "name"), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }


  @Test def booleanLiterals() {
    testAll(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(true), Literal(false))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldFilterOnNumericProp() {
    testAll(
      "start a = NODE(1) where 35 = a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), Property(Identifier("a"), "age"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }


  @Test def shouldHandleNegativeLiteralsAsExpected() {
    testAll(
      "start a = NODE(1) where -35 = a.age AND a.age > -1.2 return a",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Literal(-35), Property(Identifier("a"), "age")),
        GreaterThan(Property(Identifier("a"), "age"), Literal(-1.2)))
      ).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldCreateNotEqualsQuery() {
    testAll(
      "start a = NODE(1) where 35 <> a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), Property(Identifier("a"), "age")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def multipleFilters() {
    testAll(
      "start a = NODE(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(Property(Identifier("a"), "name"), Literal("andres")),
        Equals(Property(Identifier("a"), "name"), Literal("mattias")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def relatedTo17() {
    test_1_7(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def relatedTo() {
    testFrom_1_8(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq("KNOWS"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def relatedToWithoutRelType17() {
    test_1_7(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def relatedToWithoutRelType() {
    testFrom_1_8(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testAll(
      "start a = NODE(1) match a-[r]->b return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def relatedToTheOtherWay() {
    testOlderParsers(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.INCOMING, false, True())).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def relatedToTheOtherWay1_8() {
    testFrom_1_8(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", "  UNNAMED3", Seq("KNOWS"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def shouldOutputVariables() {
    testAll(
      "start a = NODE(1) return a.name",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Property(Identifier("a"), "name"), "a.name")))
  }

  @Test def shouldHandleAndPredicates() {
    testAll(
      "start a = NODE(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Property(Identifier("a"), "name"), Literal("andres")),
        Equals(Property(Identifier("a"), "lastname"), Literal("taylor")))).
        returns(ReturnItem(Property(Identifier("a"), "name"), "a.name")))
  }

  @Test def relatedToWithRelationOutput() {
    testAll(
      "start a = NODE(1) match a -[rel:KNOWS]-> (b) return rel",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Seq("KNOWS"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("rel"), "rel")))
  }

  @Test def relatedToWithoutEndName17() {
    test_1_7(
      "start a = NODE(1) match a -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED1", "r", Seq("MARRIED"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def relatedToWithoutEndName() {
    testFrom_1_8(
      "start a = NODE(1) match a -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED3", "r", Seq("MARRIED"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def relatedInTwoSteps1_7() {
    test_1_7(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING, false, True()),
        RelatedTo("b", "c", "  UNNAMED2", Seq("FRIEND"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("c"), "c"))
    )
  }

  @Test def relatedInTwoSteps() {
    testFrom_1_8(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED5", Seq("KNOWS"), Direction.OUTGOING, false, True()),
        RelatedTo("b", "c", "  UNNAMED6", Seq("FRIEND"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("c"), "c"))
    )
  }

  @Test def djangoRelationshipType() {
    testAll(
      "start a = NODE(1) match a -[r:`<<KNOWS>>`]-> b return c",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("<<KNOWS>>"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("c"), "c")))
  }

  @Test def countTheNumberOfHits1_7() {
    test_1_7(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(CountStar(), "count(*)")))
  }

  @Test def countTheNumberOfHits() {
    testFrom_1_8(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false, True())).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(CountStar(), "count(*)")))
  }

  @Test def countStar() {
    testAll(
      "start a = NODE(1) return count(*) order by count(*)",
      Query.
        start(NodeById("a", 1)).
        aggregation(CountStar()).
        columns("count(*)").
        orderBy(SortItem(CountStar(), true)).
        returns(ReturnItem(CountStar(), "count(*)")))
  }

  @Test def distinct1_7() {
    test_1_7(
      "start a = NODE(1) match a --> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation().
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def distinct() {
    testFrom_1_8(
      "start a = NODE(1) match a -[r]-> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, True())).
        aggregation().
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def sumTheAgesOfPeople() {
    testAll(
      "start a = NODE(1) match a -[r]-> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Sum(Property(Identifier("a"), "age"))).
        columns("a", "b", "sum(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Sum(Property(Identifier("a"), "age")), "sum(a.age)")))
  }

  @Test def avgTheAgesOfPeople() {
    testFrom_1_8(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Avg(Property(Identifier("a"), "age"))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Avg(Property(Identifier("a"), "age")), "avg(a.age)")))
  }

  @Test def avgTheAgesOfPeople1_7() {
    test_1_7(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Avg(Property(Identifier("a"), "age"))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Avg(Property(Identifier("a"), "age")), "avg(a.age)")))
  }

  @Test def minTheAgesOfPeople1_7() {
    test_1_7(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Min(Property(Identifier("a"), "age"))).
        columns("a", "b", "min(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Min(Property(Identifier("a"), "age")), "min(a.age)")))
  }

  @Test def minTheAgesOfPeople() {
    testFrom_1_8(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Min(Property(Identifier("a"), "age"))).
        columns("a", "b", "min(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Min(Property(Identifier("a"), "age")), "min(a.age)")))
  }

  @Test def maxTheAgesOfPeople() {
    testFrom_1_8(
      "start a = NODE(1) match a --> b return a, b, max(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Max((Property(Identifier("a"), "age")))).
        columns("a", "b", "max(a.age)").
        returns(
        ReturnItem(Identifier("a"), "a"),
        ReturnItem(Identifier("b"), "b"),
        ReturnItem(Max((Property(Identifier("a"), "age"))), "max(a.age)")
      ))
  }

  @Test def maxTheAgesOfPeople1_7() {
    test_1_7(
      "start a = NODE(1) match a --> b return a, b, max(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Max((Property(Identifier("a"), "age")))).
        columns("a", "b", "max(a.age)").
        returns(
        ReturnItem(Identifier("a"), "a"),
        ReturnItem(Identifier("b"), "b"),
        ReturnItem(Max((Property(Identifier("a"), "age"))), "max(a.age)")
      ))
  }

  @Test def singleColumnSorting() {
    testAll(
      "start a = NODE(1) return a order by a.name",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), "name"), true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def sortOnAggregatedColumn() {
    testAll(
      "start a = NODE(1) return a order by avg(a.name)",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Avg(Property(Identifier("a"), "name")), true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleTwoSortColumns() {
    testAll(
      "start a = NODE(1) return a order by a.name, a.age",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property(Identifier("a"), "name"), true),
        SortItem(Property(Identifier("a"), "age"), true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleTwoSortColumnsAscending() {
    testAll(
      "start a = NODE(1) return a order by a.name ASCENDING, a.age ASC",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property(Identifier("a"), "name"), true),
        SortItem(Property(Identifier("a"), "age"), true)).
        returns(ReturnItem(Identifier("a"), "a")))

  }

  @Test def orderByDescending() {
    testAll(
      "start a = NODE(1) return a order by a.name DESCENDING",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), "name"), false)).
        returns(ReturnItem(Identifier("a"), "a")))

  }

  @Test def orderByDesc() {
    testAll(
      "start a = NODE(1) return a order by a.name desc",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), "name"), false)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def nullableProperty() {
    testAll(
      "start a = NODE(1) return a.name?",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Nullable(Property(Identifier("a"), "name")), "a.name?")))
  }

  @Test def nestedBooleanOperatorsAndParentesis() {
    testAll(
      """start n = NODE(1,2,3) where (n.animal = "monkey" and n.food = "banana") or (n.animal = "cow" and n
      .food="grass") return n""",
      Query.
        start(NodeById("n", 1, 2, 3)).
        where(Or(
        And(
          Equals(Property(Identifier("n"), "animal"), Literal("monkey")),
          Equals(Property(Identifier("n"), "food"), Literal("banana"))),
        And(
          Equals(Property(Identifier("n"), "animal"), Literal("cow")),
          Equals(Property(Identifier("n"), "food"), Literal("grass"))))).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test def limit5() {
    testAll(
      "start n=NODE(1) return n limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test def skip5() {
    testAll(
      "start n=NODE(1) return n skip 5",
      Query.
        start(NodeById("n", 1)).
        skip(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test def skip5limit5() {
    testAll(
      "start n=NODE(1) return n skip 5 limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        skip(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test def relationshipType() {
    testAll(
      "start n=NODE(1) match n-[r]->(x) where type(r) = \"something\" return r",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        where(Equals(RelationshipTypeFunction(Identifier("r")), Literal("something"))).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def pathLength() {
    testAll(
      "start n=NODE(1) match p=(n-[r]->x) where LENGTH(p) = 10 return p",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True()))).
        where(Equals(LengthFunction(Identifier("p")), Literal(10.0))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def relationshipTypeOut() {
    testAll(
      "start n=NODE(1) match n-[r]->(x) return TYPE(r)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(RelationshipTypeFunction(Identifier("r")), "TYPE(r)")))
  }


  @Test def shouldBeAbleToParseCoalesce() {
    testAll(
      "start n=NODE(1) match n-[r]->(x) return COALESCE(r.name,x.name)",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(CoalesceFunction(Property(Identifier("r"), "name"), Property(Identifier("x"), "name")), "COALESCE(r.name,x.name)")))
  }

  @Test def relationshipsFromPathOutput() {
    testAll(
      "start n=NODE(1) match p=n-[r]->x return RELATIONSHIPS(p)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True()))).
        returns(ReturnItem(RelationshipFunction(Identifier("p")), "RELATIONSHIPS(p)")))
  }

  @Test def relationshipsFromPathInWhere() {
    testAll(
      "start n=NODE(1) match p=n-[r]->x where length(rels(p))=1 return p",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True()))).
        where(Equals(LengthFunction(RelationshipFunction(Identifier("p"))), Literal(1))).
        returns (ReturnItem(Identifier("p"), "p")))
  }

  @Test def countNonNullValues() {
    testAll(
      "start a = NODE(1) return a, count(a)",
      Query.
        start(NodeById("a", 1)).
        aggregation(Count(Identifier("a"))).
        columns("a", "count(a)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Count(Identifier("a")), "count(a)")))
  }

  @Test def shouldHandleIdBothInReturnAndWhere() {
    testAll(
      "start a = NODE(1) where id(a) = 0 return ID(a)",
      Query.
        start(NodeById("a", 1)).
        where(Equals(IdFunction(Identifier("a")), Literal(0)))
        returns (ReturnItem(IdFunction(Identifier("a")), "ID(a)")))
  }

  @Test def shouldBeAbleToHandleStringLiteralsWithApostrophe() {
    testAll(
      "start a = node:index(key = 'value') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleQuotationsInsideApostrophes() {
    testAll(
      "start a = node:index(key = 'val\"ue') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("val\"ue"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def simplePathExample17() {
    test_1_7(
      "start a = node(0) match p = a-->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def simplePathExample() {
    testFrom_1_8(
      "start a = node(0) match p = a-->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false, True()))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def threeStepsPath() {
    testAll(
      "start a = node(0) match p = ( a-[r1]->b-[r2]->c ) return a",
      Query.
        start(NodeById("a", 0)).
        matches(
          RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING, false, True()),
          RelatedTo("b", "c", "r2", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p",
          RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING, false, True()),
          RelatedTo("b", "c", "r2", Seq(), Direction.OUTGOING, false, True()))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def pathsShouldBePossibleWithoutParenthesis() {
    testAll(
      "start a = node(0) match p = a-[r]->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, True()))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def variableLengthPath17() {
    test_1_7("start a=node(0) match a -[:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPath() {
    testFrom_1_8("start a=node(0) match a -[:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelsIterable17() {
    test_1_7("start a=node(0) match a -[r:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), Seq("knows"), Direction.OUTGOING, Some("r"), false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelsIterable() {
    testFrom_1_8("start a=node(0) match a -[r:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(1), Some(3), Seq("knows"), Direction.OUTGOING, Some("r"), false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def fixedVarLengthPath1_7() {
    test_1_7("start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(3), Some(3), Seq(), Direction.OUTGOING, None, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def fixedVarLengthPath() {
    testFrom_1_8("start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(3), Some(3), Seq(), Direction.OUTGOING, None, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMinDepth17() {
    test_1_7("start a=node(0) match p = a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("p", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMinDepth() {
    testFrom_1_8("start a=node(0) match a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier17() {
    test_1_7("start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, Seq("knows"), Direction.OUTGOING, Some("r"), false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    testFrom_1_8("start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(2), None, Seq("knows"), Direction.OUTGOING, Some("r"), false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth17() {
    test_1_7("start a=node(0) match p = a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("p", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    testFrom_1_8("start a=node(0) match a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def unboundVariableLengthPath17() {
    test_1_7("start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def unboundVariableLengthPath() {
    testFrom_1_8("start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def optionalRelationship17() {
    test_1_7(
      "start a = node(1) match a -[?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def optionalRelationship() {
    testFrom_1_8(
      "start a = node(1) match a -[?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def questionMarkOperator() {
    testAll(
      "start a = node(1) where a.prop? = 42 return a",
      Query.
        start(NodeById("a", 1)).
        where(NullablePredicate(Equals(Nullable(Property(Identifier("a"), "prop")), Literal(42.0)), Seq((Nullable(Property(Identifier("a"), "prop")), true)))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def exclamationMarkOperator() {
    testAll(
      "start a = node(1) where a.prop! = 42 return a",
      Query.
        start(NodeById("a", 1)).
        where(NullablePredicate(Equals(Nullable(Property(Identifier("a"), "prop")), Literal(42)), Seq((Nullable(Property(Identifier("a"), "prop")), false)))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def optionalTypedRelationship17() {
    test_1_7(
      "start a = node(1) match a -[?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def optionalTypedRelationship() {
    testFrom_1_8(
      "start a = node(1) match a -[?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq("KNOWS"), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def optionalTypedAndNamedRelationship() {
    testAll(
      "start a = node(1) match a -[r?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("KNOWS"), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def optionalNamedRelationship() {
    testAll(
      "start a = node(1) match a -[r?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def testSingle() {
    testAll(
      """start a = node(1) where single(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(SingleInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), "name"),
        Literal("Andres"))))
        returns (ReturnItem(Identifier("b"), "b")))
  }

  @Test def testParamAsStartNode() {
    testAll(
      """start pA = node({a}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testNumericParamNameAsStartNode() {
    testAll(
      """start pA = node({0}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("0"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForWhereLiteral() {
    testAll(
      """start pA = node(1) where pA.name = {name} return pA""",
      Query.
        start(NodeById("pA", 1)).
        where(Equals(Property(Identifier("pA"), "name"), ParameterExpression("name")))
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForIndexKey() {
    testAll(
      """start pA = node:idx({key} = "Value") return pA""",
      Query.
        start(NodeByIndex("pA", "idx", ParameterExpression("key"), Literal("Value"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForIndexValue() {
    testAll(
      """start pA = node:idx(key = {Value}) return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), ParameterExpression("Value"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForIndexQuery() {
    testAll(
      """start pA = node:idx({query}) return pA""",
      Query.
        start(NodeByIndexQuery("pA", "idx", ParameterExpression("query"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForSkip() {
    testAll(
      """start pA = node(0) return pA skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForLimit() {
    testAll(
      """start pA = node(0) return pA limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForLimitAndSkip() {
    testAll(
      """start pA = node(0) return pA skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ("stop")
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForRegex() {
    testAll(
      """start pA = node(0) where pA.name =~ {regex} return pA""",
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(Property(Identifier("pA"), "name"), ParameterExpression("regex")))
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testShortestPathWithMaxDepth() {
    testAll(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq(), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def testShortestPathWithType() {
    testAll(
      """start a=node(0), b=node(1) match p = shortestPath( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq("KNOWS"), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def testForNull() {
    testAll(
      """start a=node(0) where a is null return a""",
      Query.
        start(NodeById("a", 0)).
        where(IsNull(Identifier("a")))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def testForNotNull() {
    testAll(
      """start a=node(0) where a is not null return a""",
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(Identifier("a"))))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def testCountDistinct() {
    testAll(
      """start a=node(0) return count(distinct a)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(Distinct(Count(Identifier("a")), Identifier("a"))).
        columns("count(distinct a)")
        returns (ReturnItem(Distinct(Count(Identifier("a")), Identifier("a")), "count(distinct a)")))
  }

  @Test def supportsHasRelationshipInTheWhereClauseOlder() {
    testOlderParsers(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Identifier("a"), Identifier("b"), Direction.OUTGOING, Seq()))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def supportsHasRelationshipInTheWhereClause() {
    testFrom_1_8(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo("a", "b", "  UNNAMED39", Seq(), Direction.OUTGOING, optional = false, predicate = True()))))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def supportsNotHasRelationshipInTheWhereClauseOlder() {
    testOlderParsers(
      """start a=node(0), b=node(1) where not(a-->()) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(Not(HasRelationship(Identifier("a"), Direction.OUTGOING, Seq()))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def supportsNotHasRelationshipInTheWhereClause() {
    testFrom_1_8(
      """start a=node(0), b=node(1) where not(a-->()) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(Not(NonEmpty(PathExpression(Seq(RelatedTo("a", "  UNNAMED143", "  UNNAMED144", Seq(), Direction.OUTGOING, optional = false, predicate = True())))))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleLFAsWhiteSpace() {
    testAll(
      "start\na=node(0)\nwhere\na.prop=12\nreturn\na",
      Query.
        start(NodeById("a", 0)).
        where(Equals(Property(Identifier("a"), "prop"), Literal(12)))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Ignore @Test def shouldAcceptRelationshipWithPredicate() {
    testAll(
      "start a = node(1) match a-[r WHERE r.foo = 'bar']->b return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, Equals(Property(Identifier("r"), "foo"), Literal("bar"))))
        returns (ReturnItem(Identifier("b"), "b")))
  }

  @Test def shouldHandleUpperCaseDistinct() {
    testAll("start s = NODE(1) return DISTINCT s",
      Query.
        start(NodeById("s", 1)).
        aggregation().
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseMathFunctions() {
    testAll("start s = NODE(0) return 5 % 4, abs(-1), round(3.1415), 2 ^ 8, sqrt(16), sign(1)",
      Query.
        start(NodeById("s", 0)).
        returns(
        ReturnItem(Modulo(Literal(5), Literal(4)), "5 % 4"),
        ReturnItem(AbsFunction(Literal(-1)), "abs(-1)"),
        ReturnItem(RoundFunction(Literal(3.1415)), "round(3.1415)"),
        ReturnItem(Pow(Literal(2), Literal(8)), "2 ^ 8"),
        ReturnItem(SqrtFunction(Literal(16)), "sqrt(16)"),
        ReturnItem(SignFunction(Literal(1)), "sign(1)")
      )
    )
  }

  @Test def shouldAllowCommentAtEnd() {
    testAll("start s = NODE(1) return s // COMMENT",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldAllowCommentAlone() {
    testAll("""start s = NODE(1) return s
    // COMMENT""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldAllowCommentsInsideStrings() {
    testAll("start s = NODE(1) where s.apa = '//NOT A COMMENT' return s",
      Query.
        start(NodeById("s", 1)).
        where(Equals(Property(Identifier("s"), "apa"), Literal("//NOT A COMMENT")))
        returns (ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldHandleCommentsFollowedByWhiteSpace() {
    testAll("""start s = NODE(1)
    //I can haz more comment?
    return s""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def first_last_and_rest() {
    val p1 = RelatedTo("x", "z", "r", Seq(), Direction.OUTGOING, false, True())
    testAll("start x = NODE(1) match p=x-[r]->z return head(nodes(p)), last(nodes(p)), tail(nodes(p))",
      Query.
        start(NodeById("x", 1)).
        matches(p1).
        namedPaths(NamedPath("p", p1)).
        returns(
        ReturnItem(HeadFunction(NodesFunction(Identifier("p"))), "head(nodes(p))"),
        ReturnItem(LastFunction(NodesFunction(Identifier("p"))), "last(nodes(p))"),
        ReturnItem(TailFunction(NodesFunction(Identifier("p"))), "tail(nodes(p))")
      ))
  }

  @Test def filter() {
    testAll("start x = NODE(1) match p=x-[r]->z return filter(x in p : x.prop = 123)",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "r", Seq(), Direction.OUTGOING, false, True())).
        namedPaths(NamedPath("p", RelatedTo("x", "z", "r", Seq(), Direction.OUTGOING, false, True()))).
        returns(
        ReturnItem(FilterFunction(Identifier("p"), "x", Equals(Property(Identifier("x"), "prop"), Literal(123))), "filter(x in p : x.prop = 123)")
      ))
  }

  @Test def collection_literal() {
    testAll("start x = NODE(1) return ['a','b','c']",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal("b"), Literal("c")), "['a','b','c']")
      ))
  }

  @Test def collection_literal2() {
    testAll("start x = NODE(1) return []",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(), "[]")
      ))
  }

  @Test def collection_literal3() {
    testAll("start x = NODE(1) return [1,2,3]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal(1), Literal(2), Literal(3)), "[1,2,3]")
      ))
  }

  @Test def collection_literal4() {
    testAll("start x = NODE(1) return ['a',2]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal(2)), "['a',2]")
      ))
  }

  @Test def in_with_collection_literal() {
    testAll("start x = NODE(1) where x.prop in ['a','b'] return x",
      Query.
        start(NodeById("x", 1)).
        where(AnyInCollection(Collection(Literal("a"), Literal("b")), "-_-INNER-_-", Equals(Property(Identifier("x"), "prop"), Identifier("-_-INNER-_-")))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_match17() {
    test_1_7("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED1", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_match() {
    testFrom_1_8("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_varlength_rel17() {
    test_1_7("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED1", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_varlength_rel() {
    testFrom_1_8("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_shortest_path17() {
    test_1_7("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED1", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_shortest_path() {
    testFrom_1_8("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_relationship_predicate_1_7() {
    test_1_7(
      """start a=node(0), b=node(1) where a-[:KNOWS|BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Identifier("a"), Identifier("b"), Direction.BOTH, Seq("KNOWS", "BLOCKS")))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def multiple_relationship_type_in_relationship_predicate() {
    testFrom_1_8(
      """start a=node(0), b=node(1) where a-[:KNOWS|BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo("a", "b", "  UNNAMED39", Seq("KNOWS","BLOCKS"), Direction.BOTH, optional = false, predicate = True())))))
        returns (ReturnItem(Identifier("a"), "a")))
  }


  @Test def first_parsed_pipe_query() {
    val secondQ = Query.
      start().
      where(Equals(Property(Identifier("x"), "foo"), Literal(42))).
      returns(ReturnItem(Identifier("x"), "x"))

    val q = Query.
      start(NodeById("x", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("x"), "x"))


    testFrom_1_8("START x = node(1) WITH x WHERE x.foo = 42 RETURN x", q)
  }

  @Test def read_first_and_update_next() {
    val secondQ = Query.
      start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), "age"), Literal(2.0)))))).
      returns(ReturnItem(Identifier("b"), "b"))

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"))


    testFrom_1_8("start a = node(1) with a create (b {age : a.age * 2}) return b", q)
  }

  @Test def variable_length_path_with_collection_for_relationships17() {
    test_1_7("start a=node(0) match a -[r?*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), Seq(), Direction.OUTGOING, Some("r"), true, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variable_length_path_with_collection_for_relationships() {
    testFrom_1_8("start a=node(0) match a -[r?*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(1), Some(3), Seq(), Direction.OUTGOING, Some("r"), true, True())).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def binary_precedence() {
    testAll("""start n=node(0) where n.a = 'x' and n.b = 'x' or n.c = 'x' return n""",
      Query.
        start(NodeById("n", 0)).
        where(
        Or(
          And(
            Equals(Property(Identifier("n"), "a"), Literal("x")),
            Equals(Property(Identifier("n"), "b"), Literal("x"))
          ),
          Equals(Property(Identifier("n"), "c"), Literal("x"))
        )
      ).returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test def create_node() {
    testFrom_1_8("create a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map()))).
        returns()
    )
  }

  @Test def create_node_with_a_property() {
    testFrom_1_8("create (a {name : 'Andres'})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres"))))).
        returns()
    )
  }

  @Test def create_node_with_a_property2() {
    testFrom_1_8("create a={name : 'Andres'}",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres"))))).
        returns()
    )
  }

  @Test def create_node_with_a_property_and_return_it() {
    testFrom_1_8("create (a {name : 'Andres'}) return a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres"))))).
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def create_two_nodes_with_a_property_and_return_it() {
    testFrom_1_8("create (a {name : 'Andres'}), b return a,b",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")))),
        CreateNodeStartItem(CreateNode("b", Map()))).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def create_node_from_map_expression() {
    testFrom_1_8("create (a {param})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("*" -> ParameterExpression("param"))))).
        returns()
    )
  }

  @Test def start_with_two_nodes_and_create_relationship() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r", (Identifier("a"), Map()), (Identifier("b"),Map()), "REL", Map()))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))


    testFrom_1_8("start a=node(0), b=node(1) with a,b create a-[r:REL]->b", q)
  }

  @Test def start_with_two_nodes_and_create_relationship_using_alternative_with_syntax() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r", (Identifier("a"),Map()), (Identifier("b"),Map()), "REL", Map()))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))


    testFrom_1_8("""
start a=node(0), b=node(1)
========= a,b ============
create a-[r:REL]->b
""", q)
  }

  @Test def create_relationship_with_properties() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
      (Identifier("a"),Map()),
      (Identifier("b"),Map()), "REL", Map("why" -> Literal(42), "foo" -> Literal("bar"))))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))


    testFrom_1_8("start a=node(0), b=node(1) with a,b create a-[r:REL {why : 42, foo : 'bar'}]->b", q)
  }

  @Test def create_relationship_without_identifier() {
    testFrom_1_8("create ({a})-[:REL]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED3", (ParameterExpression("a"),Map()), (ParameterExpression("a"),Map()), "REL", Map()))).
        returns())
  }

  @Test def create_relationship_with_properties_from_map() {
    testFrom_1_8("create ({a})-[:REL {param}]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED3", (ParameterExpression("a"),Map()), (ParameterExpression("a"),Map()), "REL", Map("*" -> ParameterExpression("param"))))).
        returns())
  }

  @Test def create_relationship_without_identifier2() {
    testFrom_1_8("create ({a})-[:REL]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED3", (ParameterExpression("a"),Map()), (ParameterExpression("a"),Map()), "REL", Map()))).
        returns())
  }

  @Test def delete_node() {
    val secondQ = Query.
      updates(DeleteEntityAction(Identifier("a"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"))

    testFrom_1_8("start a=node(0) with a delete a", q)
  }

  @Test def set_property_on_node() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "hello"), Literal("world"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"))

    testFrom_1_8("start a=node(0) with a set a.hello = 'world'", q)
  }

  @Test def update_property_with_expression() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "salary"), Multiply(Property(Identifier("a"), "salary"), Literal(2.0)))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"))

    testFrom_1_8("start a=node(0) with a set a.salary = a.salary * 2 ", q)
  }

  @Test def foreach_on_path() {
    val secondQ = Query.
      updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), "touched"), Literal(true))))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
      namedPaths(NamedPath("p", RelatedTo("a", "b", "r", "REL", Direction.OUTGOING))).
      tail(secondQ).
      returns(ReturnItem(Identifier("p"), "p"))

    testFrom_1_8("start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) : set n.touched = true ) ", q)
  }

  @Test def simple_read_first_and_update_next() {
    val secondQ = Query.
      start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), "age"), Literal(2.0)))))).
      returns(ReturnItem(Identifier("b"), "b"))

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testFrom_1_8("start a = node(1) create (b {age : a.age * 2}) return b", q)
  }

  @Test def simple_start_with_two_nodes_and_create_relationship() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r", (Identifier("a"), Map()), (Identifier("b"), Map()), "REL", Map()))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testFrom_1_8("start a=node(0), b=node(1) create a-[r:REL]->b", q)
  }

  @Test def simple_create_relationship_with_properties() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r", (Identifier("b"), Map()), (Identifier("a"), Map()), "REL",
      Map("why" -> Literal(42), "foo" -> Literal("bar"))
    ))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testFrom_1_8("start a=node(0), b=node(1) create a<-[r:REL {why : 42, foo : 'bar'}]-b", q)
  }

  @Test def simple_delete_node() {
    val secondQ = Query.
      updates(DeleteEntityAction(Identifier("a"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) delete a", q)
  }

  @Test def simple_set_property_on_node() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "hello"), Literal("world"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) set a.hello = 'world'", q)
  }

  @Test def simple_update_property_with_expression() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "salary"), Multiply(Property(Identifier("a"), "salary"), Literal(2.0)))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) set a.salary = a.salary * 2 ", q)
  }

  @Test def simple_foreach_on_path() {
    val secondQ = Query.
      updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), "touched"), Literal(true))))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
      namedPaths(NamedPath("p", RelatedTo("a", "b", "r", "REL", Direction.OUTGOING))).
      tail(secondQ).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) match p = a-[r:REL]->b foreach(n in nodes(p) : set n.touched = true ) ", q)
  }

  @Test def returnAll() {
    testFrom_1_8("start s = NODE(1) return *",
      Query.
        start(NodeById("s", 1)).
        returns(AllIdentifiers()))
  }

  @Test def single_create_unique() {
    val secondQ = Query.
      unique(UniqueLink("a", "b", "  UNNAMED1", "reltype", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1), b=node(2) create unique a-[:reltype]->b", q)
  }

  @Test def single_create_unique_with_rel() {
    val secondQ = Query.
      unique(UniqueLink("a", "b", "r", "reltype", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1), b=node(2) create unique a-[r:reltype]->b", q)
  }

  @Test def single_relate_with_empty_parenthesis() {
    val secondQ = Query.
      unique(UniqueLink("a", "  UNNAMED1", "  UNNAMED2", "reltype", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1), b=node(2) create unique a-[:reltype]->()", q)
  }

  @Test def two_relates() {
    val secondQ = Query.
      unique(
      UniqueLink("a", "b", "  UNNAMED1", "X", Direction.OUTGOING),
      UniqueLink("c", "b", "  UNNAMED2", "X", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1) create unique a-[:X]->b<-[:X]-c", q)
  }

  @Test def relate_with_initial_values_for_node() {
    val secondQ = Query.
      unique(
      UniqueLink(NamedExpectation("a"), NamedExpectation("b", Map[String, Expression]("name" -> Literal("Andres"))), NamedExpectation("  UNNAMED1"), "X", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1) create unique a-[:X]->(b {name:'Andres'})", q)
  }

  @Test def relate_with_initial_values_for_rel() {
    val secondQ = Query.
      unique(
      UniqueLink(NamedExpectation("a"), NamedExpectation("b"), NamedExpectation("  UNNAMED1", Map[String, Expression]("name" -> Literal("Andres"))), "X", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1) create unique a-[:X {name:'Andres'}]->b", q)
  }

  @Test def foreach_with_literal_collection() {

    val q2 = Query.updates(
      ForeachAction(Collection(Literal(1.0), Literal(2.0), Literal(3.0)), "x", Seq(CreateNode("a", Map("number" -> Identifier("x")))))
    ).returns()

    testFrom_1_8(
      "create root foreach(x in [1,2,3] : create (a {number:x}))",
      Query.
        start(CreateNodeStartItem(CreateNode("root", Map.empty))).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def string_literals_should_not_be_mistaken_for_identifiers() {
    testFrom_1_8(
      "create (tag1 {name:'tag2'}), (tag2 {name:'tag1'})",
      Query.
        start(
        CreateNodeStartItem(CreateNode("tag1", Map("name"->Literal("tag2")))),
        CreateNodeStartItem(CreateNode("tag2", Map("name"->Literal("tag1"))))
      ).returns()
    )
  }

  @Test def relate_with_two_rels_to_same_node() {
    val returns = Query.
      start(CreateUniqueStartItem(CreateUniqueAction(
      UniqueLink("root", "x", "r1", "X", Direction.OUTGOING),
      UniqueLink("root", "x", "r2", "Y", Direction.OUTGOING))))
      .returns(ReturnItem(Identifier("x"), "x"))

    val q = Query.start(NodeById("root", 0)).tail(returns).returns(AllIdentifiers())

    testFrom_1_8(
      "start root=node(0) create unique x<-[r1:X]-root-[r2:Y]->x return x",
      q
    )
  }

  @Test def optional_shortest_path() {
    testFrom_1_8(
      """start a  = node(1), x = node(2,3)
         match p = shortestPath(a -[?*]-> x)
         return *""",
      Query.
        start(NodeById("a", 1),NodeById("x", 2,3)).
        matches(ShortestPath("p", "a", "x", Seq(), Direction.OUTGOING, None, optional = true, single = true, relIterator = None)).
        returns(AllIdentifiers())
    )
  }

  @Test def return_paths() {
    testFrom_1_8("start a  = node(1) return a-->()",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(PathExpression(Seq(RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Seq(), Direction.OUTGOING, optional = false, predicate = True()))), "a-->()"))
    )
  }

  @Test def not_with_parenthesis() {
    testFrom_1_8("start a  = node(1) where not(1=2) or 2=3 return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(Not(Equals(Literal(1), Literal(2))), Equals(Literal(2), Literal(3)))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def full_path_in_create() {
    val secondQ = Query.
      start(
      CreateRelationshipStartItem(CreateRelationship("r1", (Identifier("a"), Map()), (Identifier("  UNNAMED1"), Map()), "KNOWS", Map())),
      CreateRelationshipStartItem(CreateRelationship("r2", (Identifier("  UNNAMED1"), Map()), (Identifier("b"), Map()), "LOVES", Map()))).
      returns()
    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())


    testFrom_1_8("start a=node(1), b=node(2) create a-[r1:KNOWS]->()<-[r2:LOVES]->b", q)
  }


  @Test def create_and_assign_to_path_identifier() {
    testFrom_1_8(
      "create p = a-[r:KNOWS]->() return p",
      Query.
      start(CreateRelationshipStartItem(CreateRelationship("r", (Identifier("a"), Map()), (Identifier("  UNNAMED1"), Map()), "KNOWS", Map()))).
      namedPaths(NamedPath("p", RelatedTo("a", "  UNNAMED1", "r", "KNOWS", Direction.OUTGOING, optional = false, predicate = True()))).
      returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def undirected_relationship() {
    testFrom_1_8(
      "create (a {name:'A'})-[:KNOWS]-(b {name:'B'})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED1", (Identifier("a"), Map("name" -> Literal("A"))), (Identifier("b"), Map("name" -> Literal("B"))), "KNOWS", Map()))).
        returns())
  }

  @Test def relate_and_assign_to_path_identifier() {
    val q2 = Query.
      start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink("a", "  UNNAMED1", "r", "KNOWS", Direction.OUTGOING)))).
      namedPaths(NamedPath("p", RelatedTo("a", "  UNNAMED1", "r", "KNOWS", Direction.OUTGOING, optional = false, predicate = True()))).
      returns(ReturnItem(Identifier("p"), "p"))

    val q = Query.
      start(NodeById("a", 0)).
      tail(q2).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) create unique p = a-[r:KNOWS]->() return p", q)
  }

  @Test(expected = classOf[SyntaxException]) def assign_to_path_inside_foreach_should_work() {
    testFrom_1_8(
"""start n=node(0)
foreach(x in [1,2,3] :
  create p = ({foo:x})-[:X]->()
  foreach( i in p :
    set i.touched = true))""",
      Query.
      start(CreateRelationshipStartItem(CreateRelationship("r", (Identifier("a"), Map()), (Identifier("  UNNAMED1"), Map()), "KNOWS", Map()))).
      namedPaths(NamedPath("p", RelatedTo("a", "  UNNAMED1", "r", "KNOWS", Direction.OUTGOING, optional = false, predicate = True()))).
      returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def use_predicate_as_expression() {
    testFrom_1_9("start n=node(0) return id(n) = 0, n is null",
      Query.
        start(NodeById("n", 0)).
        returns(
        ReturnItem(Equals(IdFunction(Identifier("n")), Literal(0)), "id(n) = 0"),
        ReturnItem(IsNull(Identifier("n")), "n is null")
      ))
  }

  @Test def create_unique_should_support_parameter_maps() {
    val start = NamedExpectation("n")
    val rel = NamedExpectation("  UNNAMED2")
    val end = NamedExpectation("  UNNAMED1", ParameterExpression("param"), Map.empty)

    val secondQ = Query.
                  unique(UniqueLink(start, end, rel, "foo", Direction.OUTGOING)).
                  returns(AllIdentifiers())

    testFrom_1_9("START n=node(0) CREATE UNIQUE n-[:foo]->({param}) RETURN *",
                 Query.
                 start(NodeById("n", 0)).
                 tail(secondQ).
                 returns(AllIdentifiers()))
  }

  @Test def with_limit() {
    testFrom_1_9("start n=node(0,1,2) with n limit 2 where ID(n) = 1 return n",
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

  @Test def with_sort_limit() {
    testFrom_1_9("start n=node(0,1,2) with n order by ID(n) desc limit 2 where ID(n) = 1 return n",
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

  @Test def set_to_map() {
    val q2 = Query.
      start().
      updates(MapPropertySetAction(Identifier("n"), ParameterExpression("prop"))).
      returns()

    testFrom_1_9("start n=node(0) set n = {prop}",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers()))
  }


  @Ignore("slow test") @Test def multi_thread_parsing() {
    val q = """start root=node(0) return x"""
    val parser = new CypherParser()

    val runners = (1 to 10).toList.map(x => {
      run(() => parser.parse(q))
    })

    val threads = runners.map(new Thread(_))
    threads.foreach(_.start())
    threads.foreach(_.join())

    runners.foreach(_.report())
  }

  @Test def single_node_match_pattern() {
    testFrom_1_9("start s = node(*) match s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def single_node_match_pattern_path() {
    testFrom_1_9("start s = node(*) match p = s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        namedPaths(NamedPath("p", SingleNode("s"))).
        returns(ReturnItem(Identifier("s"), "s")))
  }


  private def run(f: () => Unit) =
    new Runnable() {
      var error: Option[Throwable] = None

      def run() {
        try {
          (1 until 500).foreach(x => f())
        } catch {
          case e: Throwable => error = Some(e)
        }
      }

      def report() {
        error.foreach(e => throw e)
      }
    }


  def test_1_9(query: String, expectedQuery: Query) {
    testQuery(None, query, expectedQuery)
    testQuery(None, query + ";", expectedQuery)
  }

  def test_1_8(query: String, expectedQuery: Query) {
    testQuery(Some("1.8"), query, expectedQuery)
    testQuery(Some("1.8"), query + ";", expectedQuery)
  }

  def test_1_7(query: String, expectedQuery: Query) {
    testQuery(Some("1.7"), query, expectedQuery)
  }

  def testFrom_1_8(query: String, expectedQuery: Query) {
    test_1_8(query, expectedQuery)
    test_1_9(query, expectedQuery)
  }

  def testFrom_1_9(query: String, expectedQuery: Query) {
    test_1_9(query, expectedQuery)
  }

  def testAll(query: String, expectedQuery: Query) {
    test_1_7(query, expectedQuery)
    test_1_8(query, expectedQuery)
    test_1_9(query, expectedQuery)
  }

  def testOlderParsers(queryText: String, queryAst: Query) {
    test_1_7(queryText, queryAst)
  }

  def testQuery(version: Option[String], query: String, expectedQuery: Query) {
    val parser = new CypherParser()

    val (qWithVer, message) = version match {
      case None => (query, "Using the default parser")
      case Some(ver) => ("cypher %s %s".format(ver, query), "Using parser version " + ver)
    }

    val ast = parser.parse(qWithVer)

    try {
      assertThat(message, ast, equalTo(expectedQuery))
    } catch {
      case x: AssertionError => throw new AssertionError(x.getMessage.replace("WrappedArray", "List"))
    }
  }
}
