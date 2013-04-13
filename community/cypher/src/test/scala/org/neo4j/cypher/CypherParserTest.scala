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
import internal.helpers.LabelSupport
import internal.mutation._
import org.junit.Assert._
import org.neo4j.graphdb.Direction
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Ignore
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers.equalTo
import values.LabelName

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
    testAll("start s = node(1) return \"a\\tp\\\"a\"",
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


  @Test def shouldHandleRegularComparison() {
    testAll(
      "start a = node(1) where \"Andres\" =~ 'And.*' return a",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }


  @Test def shouldHandleMultipleRegularComparison() {
    testAll(
      """start a = node(1) where a.name =~ 'And.*' AnD a.name =~ 'And.*' return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And.*")), LiteralRegularExpression(Property(Identifier("a"), "name"), Literal("And.*")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleEscapedRegexs() {
    testAll(
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

  @Test def booleanLiteralsOld() {
    testPre2_0(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(true), Literal(false))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def booleanLiterals() {
    testFrom2_0(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(True(), Not(True()))).
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

  @Test def relatedTo() {
    val string = "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b"
    def query(DIFFERENCE: String): Query = {
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", DIFFERENCE, Seq("KNOWS"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    }

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED26" -> testFrom2_0
    )
  }

  @Test def relatedToWithoutRelType() {
    val string = "start a = NODE(1) match a --> (b) return a, b"
    def query(relName: String): Query = {
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", relName, Seq(), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    }

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED26" -> testFrom2_0)
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testAll(
      "start a = NODE(1) match a-[r]->b return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def relatedToTheOtherWay() {
    val string = "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b"
    def query(DIFFERENCE: String): Query = {
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", DIFFERENCE, Seq("KNOWS"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    }

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED26" -> testFrom2_0)
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
        matches(RelatedTo("a", "b", "rel", Seq("KNOWS"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("rel"), "rel")))
  }


  @Test def relatedToWithoutEndName() {
    val string = "start a = NODE(1) match a -[r:MARRIED]-> () return a"
    def query(DIFFERENCE: String): Query = {
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", DIFFERENCE, "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"))
    }

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED42" -> testFrom2_0)
  }

  @Test def relatedInTwoSteps() {
    val string = "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c"
    def query(DIFFERENCE: (String, String)): Query = {
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", DIFFERENCE._1, Seq("KNOWS"), Direction.OUTGOING, false),
        RelatedTo("b", "c", DIFFERENCE._2, Seq("FRIEND"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("c"), "c"))
    }

    tests(string, query,
      ("  UNNAMED5", "  UNNAMED6") -> testPre2_0,
      ("  UNNAMED26", "  UNNAMED40") -> testFrom2_0
    )
  }

  @Test def djangoRelationshipType() {
    testAll(
      "start a = NODE(1) match a -[r:`<<KNOWS>>`]-> b return c",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("<<KNOWS>>"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("c"), "c")))
  }

  @Test def countTheNumberOfHitsOld() {
    testPre2_0(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false)).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(CountStar(), "count(*)")))
  }

  @Test def countTheNumberOfHits() {
    testFrom2_0(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), Direction.OUTGOING, false)).
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

  @Test def distinct() {
    testAll(
      "start a = NODE(1) match a -[r]-> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false)).
        aggregation().
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def sumTheAgesOfPeople() {
    testAll(
      "start a = NODE(1) match a -[r]-> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false)).
        aggregation(Sum(Property(Identifier("a"), "age"))).
        columns("a", "b", "sum(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Sum(Property(Identifier("a"), "age")), "sum(a.age)")))
  }

  @Test def avgTheAgesOfPeopleOld() {
    testPre2_0(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false)).
        aggregation(Avg(Property(Identifier("a"), "age"))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Avg(Property(Identifier("a"), "age")), "avg(a.age)")))
  }

  @Test def avgTheAgesOfPeople() {
    testFrom2_0(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), Direction.OUTGOING, false)).
        aggregation(Avg(Property(Identifier("a"), "age"))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Avg(Property(Identifier("a"), "age")), "avg(a.age)")))
  }

  @Test def minTheAgesOfPeople() {
    val string = "start a = NODE(1) match (a) --> b return a, b, min(a.age)"

    def query(DIFFERENCE: String): Query =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", DIFFERENCE, Seq(), Direction.OUTGOING, false)).
        aggregation(Min(Property(Identifier("a"), "age"))).
        columns("a", "b", "min(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Min(Property(Identifier("a"), "age")), "min(a.age)"))

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED28"-> testFrom2_0
    )
  }

  private def tests[A](query: String, queryProducer: A => Query, tsts: (A, (String, AbstractQuery) => Unit)*) {
    tsts.foreach {
      case (input, testMethod) =>
        val q = queryProducer(input)
        testMethod(query, q)
    }
  }

  @Test def maxTheAgesOfPeople() {
    val query = "start a = NODE(1) match a --> b return a, b, max(a.age)"

    testFrom2_0(
      query,
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), Direction.OUTGOING, false)).
        aggregation(Max((Property(Identifier("a"), "age")))).
        columns("a", "b", "max(a.age)").
        returns(
        ReturnItem(Identifier("a"), "a"),
        ReturnItem(Identifier("b"), "b"),
        ReturnItem(Max((Property(Identifier("a"), "age"))), "max(a.age)")
      ))

    testPre2_0(
      query,
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING, false)).
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
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false)).
        where(Equals(RelationshipTypeFunction(Identifier("r")), Literal("something"))).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def pathLength() {
    testAll(
      "start n=NODE(1) match p=(n-[r]->x) where LENGTH(p) = 10 return p",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false))).
        where(Equals(LengthFunction(Identifier("p")), Literal(10.0))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def relationshipTypeOut() {
    testAll(
      "start n=NODE(1) match n-[r]->(x) return TYPE(r)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false)).
        returns(ReturnItem(RelationshipTypeFunction(Identifier("r")), "TYPE(r)")))
  }


  @Test def shouldBeAbleToParseCoalesce() {
    testAll(
      "start n=NODE(1) match n-[r]->(x) return COALESCE(r.name,x.name)",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false)).
        returns(ReturnItem(CoalesceFunction(Property(Identifier("r"), "name"), Property(Identifier("x"), "name")), "COALESCE(r.name,x.name)")))
  }

  @Test def relationshipsFromPathOutput() {
    testAll(
      "start n=NODE(1) match p=n-[r]->x return RELATIONSHIPS(p)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false))).
        returns(ReturnItem(RelationshipFunction(Identifier("p")), "RELATIONSHIPS(p)")))
  }

  @Test def relationshipsFromPathInWhere() {
    testAll(
      "start n=NODE(1) match p=n-[r]->x where length(rels(p))=1 return p",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false))).
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

  @Test def simplePathExample() {
    val string = "start a = node(0) match p = a-->b return a"
    def query(relName: String): Query =
      Query.
      start(NodeById("a", 0)).
      matches(RelatedTo("a", "b", relName, Seq(), Direction.OUTGOING, false)).
      namedPaths(NamedPath("p", RelatedTo("a", "b", relName, Seq(), Direction.OUTGOING, false))).
      returns(ReturnItem(Identifier("a"), "a"))

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED29" -> testFrom2_0)
  }

  @Test def threeStepsPath() {
    testAll(
      "start a = node(0) match p = ( a-[r1]->b-[r2]->c ) return a",
      Query.
        start(NodeById("a", 0)).
        matches(
          RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING, false),
          RelatedTo("b", "c", "r2", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p",
          RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING, false),
          RelatedTo("b", "c", "r2", Seq(), Direction.OUTGOING, false))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def pathsShouldBePossibleWithoutParenthesis() {
    testAll(
      "start a = node(0) match p = a-[r]->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def variableLengthPath() {
    val string = "start a=node(0) match a -[:knows*1..3]-> x return x"
    def query(pathName: String): Query = {
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo(pathName, "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    }

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED24" -> testFrom2_0)
  }

  @Test def variableLengthPathWithRelsIterable() {
    val string = "start a=node(0) match a -[r:knows*1..3]-> x return x"
    def query(pathName: String): Query = {
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo(pathName, "a", "x", Some(1), Some(3), Seq("knows"), Direction.OUTGOING, Some("r"), false)).
        returns(ReturnItem(Identifier("x"), "x"))
    }

    tests(string, query,
    "  UNNAMED3" -> testPre2_0,
    "  UNNAMED24" -> testFrom2_0)
  }

  @Test def fixedVarLengthPath() {
    val string = "start a=node(0) match a -[*3]-> x return x"

    def queryWith(DIFFERENCE: String): Query = {
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo(DIFFERENCE, "a", "x", Some(3), Some(3), Seq(), Direction.OUTGOING, None, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    }

    testPre2_0(string, queryWith("  UNNAMED3"))

    testFrom2_0(string, queryWith("  UNNAMED24"))
  }

  @Test def variableLengthPathWithoutMinDepth() {
    val string = "start a=node(0) match a -[:knows*..3]-> x return x"
    def query(pathName: String): Query =
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo(pathName, "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED24" -> testFrom2_0)
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    val string = "start a=node(0) match a -[r:knows*2..]-> x return x"
    def query(pathName: String): Query =
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo(pathName, "a", "x", Some(2), None, Seq("knows"), Direction.OUTGOING, Some("r"), false)).
        returns(ReturnItem(Identifier("x"), "x"))
    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED24" -> testFrom2_0)
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    val string = "start a=node(0) match a -[:knows*2..]-> x return x"
    def query(pathName: String): Query =
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo(pathName, "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED24" -> testFrom2_0)
  }

  @Test def unboundVariableLengthPath_Old() {
    testPre2_0("start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def unboundVariableLengthPath() {
    testFrom2_0("start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def optionalRelationship() {
    val string = "start a = node(1) match a -[?]-> (b) return b"
    def query(DIFFERENCE: String): Query = {
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", DIFFERENCE, Seq(), Direction.OUTGOING, true)).
        returns(ReturnItem(Identifier("b"), "b"))
    }

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED26" -> testFrom2_0)
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

  @Test def optionalTypedRelationship() {
    val string = "start a = node(1) match a -[?:KNOWS]-> (b) return b"
    def query(DIFFERENCE: String): Query = {
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", DIFFERENCE, Seq("KNOWS"), Direction.OUTGOING, true)).
        returns(ReturnItem(Identifier("b"), "b"))
    }

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED26" -> testFrom2_0)
  }

  @Test def optionalTypedAndNamedRelationship() {
    testAll(
      "start a = node(1) match a -[r?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("KNOWS"), Direction.OUTGOING, true)).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def optionalNamedRelationship() {
    testAll(
      "start a = node(1) match a -[r?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, true)).
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


  @Test def supportedHasRelationshipInTheWhereClause() {
    testPre2_0(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo("a", "b", "  UNNAMED39", Seq(), Direction.OUTGOING, optional = false))))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def supportsHasRelationshipInTheWhereClause() {
    testFrom2_0(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(PatternPredicate(Seq(RelatedTo("a", "b", "  UNNAMED34", Seq(), Direction.OUTGOING, optional = false)))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def supportedNotHasRelationshipInTheWhereClause() {
    testPre2_0(
      """start a=node(0), b=node(1) where not(a-->()) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(Not(NonEmpty(PathExpression(Seq(RelatedTo("a", "  UNNAMED143", "  UNNAMED144", Seq(), Direction.OUTGOING, optional = false)))))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def supportsNotHasRelationshipInTheWhereClause() {
    testFrom2_0(
      """start a=node(0), b=node(1) where not(a-->()) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(Not(PatternPredicate(Seq(RelatedTo("a", "  UNNAMED42", "  UNNAMED38", Seq(), Direction.OUTGOING, optional = false))))).
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
    val p1 = RelatedTo("x", "z", "r", Seq(), Direction.OUTGOING, false)
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
        matches(RelatedTo("x", "z", "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", RelatedTo("x", "z", "r", Seq(), Direction.OUTGOING, false))).
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

  @Test def multiple_relationship_type_in_matchOld() {
    val query = {
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    }

    testPre2_0("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x", query)
  }

  @Test def multiple_relationship_type_in_match() {
    val query = {
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED25", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    }

    testFrom2_0("start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x", query)
  }


  @Test def multiple_relationship_type_in_varlength_relOld() {
    val q=
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))

    testPre2_0("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x", q)
  }

  @Test def multiple_relationship_type_in_varlength_rel() {
    val q=
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED25", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))

    testFrom2_0("start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x", q)
  }

  @Test def multiple_relationship_type_in_shortest_pathOld() {
    def q =
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))

    testPre2_0("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x", q)
  }

  @Test def multiple_relationship_type_in_shortest_path() {
    def q =
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED25", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))

    testFrom2_0("start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x", q)
  }

  @Test def multiple_relationship_type_in_relationship_predicate_back_in_the_day() {
    testPre2_0(
      """start a=node(0), b=node(1) where a-[:KNOWS|BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo("a", "b", "  UNNAMED39", Seq("KNOWS","BLOCKS"), Direction.BOTH, optional = false)))))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def multiple_relationship_type_in_relationship_predicate() {
    testFrom2_0(
      """start a=node(0), b=node(1) where a-[:KNOWS|:BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(PatternPredicate(Seq(RelatedTo("a", "b", "  UNNAMED34", Seq("KNOWS","BLOCKS"), Direction.BOTH, optional = false))))
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


    testAll("START x = node(1) WITH x WHERE x.foo = 42 RETURN x", q)
  }

  @Test def read_first_and_update_next() {
    val string = "start a = node(1) with a create (b {age : a.age * 2}) return b"

    def query(bare: Boolean) = {
      val secondQ = Query.
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), "age"), Literal(2.0))), Seq.empty, bare))).
        returns(ReturnItem(Identifier("b"), "b"))

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    }

    tests(string, query,
      true->testPre2_0,
      false->testFrom2_0)
  }

  @Test def variable_length_path_with_collection_for_relationships() {
    val string = "start a=node(0) match a -[r?*1..3]-> x return x"
    def query(pathName: String): Query =
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo(pathName, "a", "x", Some(1), Some(3), Seq(), Direction.OUTGOING, Some("r"), optional = true)).
        returns(ReturnItem(Identifier("x"), "x"))

    tests(string, query,
      "  UNNAMED3" -> testPre2_0,
      "  UNNAMED24" -> testFrom2_0)
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
    testAll("create a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), Seq.empty))).
        returns()
    )
  }

  @Test def create_node_from_param() {
    testFrom2_0("create ({param})",
      Query.
        start(CreateNodeStartItem(CreateNode("  UNNAMED8", Map("*" -> ParameterExpression("param")), Seq.empty))).
        returns()
    )
  }

  @Test def create_node_with_a_property() {
    val string = "create (a {name : 'Andres'})"
    def query(bare: Boolean): Query =
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, bare))).
        returns()

    tests(string, query,
      true -> testPre2_0,
      false -> testFrom2_0)
  }

  @Test def create_node_with_a_property2O() {
    val string = "create a={name : 'Andres'}"
    def query(bare: Boolean): Query =
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, bare))).
        returns()
    tests(string, query,
      true -> testPre2_0,
      false -> testFrom2_0)
  }

  @Test def create_node_using_the_VALUES_keyword() {
    testFrom2_0("create a VALUES {name : 'Andres'}",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, false))).
        returns()
    )
  }

  @Test def create_node_using_LABEL_keyword_and_EQ() {
    testFrom2_0("create a:fii = {name : 'Andres'}",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), LabelSupport.labelCollection("fii"), false))).
        returns()
    )
  }

  @Test def create_node_using_LABEL_and_VALUES_keyword() {
    testFrom2_0("create a :fii VALUES {name : 'Andres'}",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), LabelSupport.labelCollection("fii"), false))).
        returns()
    )
  }

  @Test def create_node_using_LABEL_and_VALUES_keyword2() {
    testFrom2_0("create a:fii VALUES {name : 'Andres'}",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), LabelSupport.labelCollection("fii"), false))).
        returns()
    )
  }

  @Test def create_node_with_a_property_and_return_it() {
    val string = "create (a {name : 'Andres'}) return a"
    def query(bare: Boolean): Query =
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, bare))).
        returns(ReturnItem(Identifier("a"), "a"))

    tests(string, query,
      true -> testPre2_0,
      false -> testFrom2_0)
  }

  @Test def create_node_from_map_expressionOld() {
    testPre2_0("create (a {param})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("*" -> ParameterExpression("param")), Seq.empty))).
        returns()
    )
  }

  @Test def create_node_from_map_expression() {
    testFrom2_0("create (a {param})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("*" -> ParameterExpression("param")), Seq.empty, false))).
        returns()
    )
  }

  @Test def create_node_with_a_label() {
    testFrom2_0("create a:FOO",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO"), false))).
        returns()
    )
  }

  @Test def create_node_with_multiple_labels() {
    testFrom2_0("create a:FOO:BAR",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO", "BAR"), false))).
        returns()
    )
  }

  @Test def create_node_with_multiple_labels_with_spaces() {
    testFrom2_0("create a :FOO :BAR",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO", "BAR"), false))).
        returns()
    )
  }

  @Test def create_nodes_with_labels_and_a_rel() {
    testFrom2_0("CREATE (n:Person:Husband)-[:FOO]->x:Person",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED25",
        RelationshipEndpoint(Identifier("n"),Map(), LabelSupport.labelCollection("Person", "Husband"), false),
        RelationshipEndpoint(Identifier("x"),Map(), LabelSupport.labelCollection("Person"), false), "FOO", Map()))).
        returns()
    )
  }

  @Test def start_with_two_nodes_and_create_relationship() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
      RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
      RelationshipEndpoint(Identifier("b"),Map(), Seq.empty, true), "REL", Map()))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))


    testAll("start a=node(0), b=node(1) with a,b create a-[r:REL]->b", q)
  }

  @Test def start_with_two_nodes_and_create_relationship_using_alternative_with_syntax() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"),Map(), Seq.empty, true),
        RelationshipEndpoint(Identifier("b"),Map(), Seq.empty, true), "REL", Map()))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))


    testAll("""
start a=node(0), b=node(1)
========= a,b ============
create a-[r:REL]->b
""", q)
  }

  @Test def create_relationship_with_properties() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
      RelationshipEndpoint(Identifier("a"),Map(),Seq.empty, true),
      RelationshipEndpoint(Identifier("b"),Map(),Seq.empty, true), "REL", Map("why" -> Literal(42), "foo" -> Literal("bar"))))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))


    testAll("start a=node(0), b=node(1) with a,b create a-[r:REL {why : 42, foo : 'bar'}]->b", q)
  }

  @Test def create_relationship_without_identifierOld() {
    testPre2_0("create ({a})-[:REL]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED3",
          RelationshipEndpoint(ParameterExpression("a"),Map(),Seq.empty, true),
          RelationshipEndpoint(ParameterExpression("a"),Map(),Seq.empty, true), "REL", Map()))).
        returns())
  }

  @Test def create_relationship_without_identifier() {
    testFrom2_0("create ({a})-[:REL]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED12",
        RelationshipEndpoint(ParameterExpression("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(ParameterExpression("a"), Map(), Seq.empty, true), "REL", Map()))).
        returns())
  }

  @Test def create_relationship_with_properties_from_map_old() {
    testPre2_0("create ({a})-[:REL {param}]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED3",
          RelationshipEndpoint(ParameterExpression("a"),Map(),Seq.empty, true),
          RelationshipEndpoint(ParameterExpression("a"),Map(),Seq.empty, true),
        "REL", Map("*" -> ParameterExpression("param"))))).
        returns())
  }

  @Test def create_relationship_with_properties_from_map() {
    testFrom2_0("create ({a})-[:REL {param}]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED12",
        RelationshipEndpoint(ParameterExpression("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(ParameterExpression("a"), Map(), Seq.empty, true),
        "REL", Map("*" -> ParameterExpression("param"))))).
        returns())
  }

  @Test def create_relationship_without_identifier2Old() {
    testPre2_0("create ({a})-[:REL]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED3",
          RelationshipEndpoint(ParameterExpression("a"),Map(),Seq.empty, true),
          RelationshipEndpoint(ParameterExpression("a"),Map(),Seq.empty, true), "REL", Map()))).
        returns())
  }

  @Test def create_relationship_without_identifier2() {
    testFrom2_0("create ({a})-[:REL]->({a})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED12",
        RelationshipEndpoint(ParameterExpression("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(ParameterExpression("a"), Map(), Seq.empty, true), "REL", Map()))).
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

    testAll("start a=node(0) with a delete a", q)
  }

  @Test def set_property_on_node() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "hello"), Literal("world"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"))

    testAll("start a=node(0) with a set a.hello = 'world'", q)
  }

  @Test def update_property_with_expression() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "salary"), Multiply(Property(Identifier("a"), "salary"), Literal(2.0)))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Identifier("a"), "a"))

    testAll("start a=node(0) with a set a.salary = a.salary * 2 ", q)
  }

  @Test def foreach_on_pathOld() {
    val secondQ = Query.
      updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), "touched"), Literal(true))))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
      namedPaths(NamedPath("p", RelatedTo("a", "b", "r", "REL", Direction.OUTGOING))).
      tail(secondQ).
      returns(ReturnItem(Identifier("p"), "p"))

    testPre2_0("start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) : set n.touched = true ) ", q)
  }

  @Test def foreach_on_path() {
    val secondQ = Query.
      updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), "touched"), True())))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
      namedPaths(NamedPath("p", RelatedTo("a", "b", "r", "REL", Direction.OUTGOING))).
      tail(secondQ).
      returns(ReturnItem(Identifier("p"), "p"))

    testFrom2_0("start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) : set n.touched = true ) ", q)
  }

  @Test def simple_read_first_and_update_next() {
    val string = "start a = node(1) create (b {age : a.age * 2}) return b"
    def query(bare: Boolean): Query = {
      val secondQ = Query.
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), "age"), Literal(2.0))), Seq.empty, bare))).
        returns(ReturnItem(Identifier("b"), "b"))

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    }
    tests(string, query,
      true -> testPre2_0,
      false -> testFrom2_0)
  }

  @Test def simple_start_with_two_nodes_and_create_relationship() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true), "REL", Map()))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testAll("start a=node(0), b=node(1) create a-[r:REL]->b", q)
  }

  @Test def simple_create_relationship_with_properties() {
    val secondQ = Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true),
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true), "REL",
      Map("why" -> Literal(42), "foo" -> Literal("bar"))
    ))).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testAll("start a=node(0), b=node(1) create a<-[r:REL {why : 42, foo : 'bar'}]-b", q)
  }

  @Test def simple_delete_node() {
    val secondQ = Query.
      updates(DeleteEntityAction(Identifier("a"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testAll("start a=node(0) delete a", q)
  }

  @Test def simple_set_property_on_node() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "hello"), Literal("world"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testAll("start a=node(0) set a.hello = 'world'", q)
  }

  @Test def simple_update_property_with_expression() {
    val secondQ = Query.
      updates(PropertySetAction(Property(Identifier("a"), "salary"), Multiply(Property(Identifier("a"), "salary"), Literal(2.0)))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testAll("start a=node(0) set a.salary = a.salary * 2 ", q)
  }

  @Test def simple_foreach_on_path() {
    val string = "start a=node(0) match p = a-[r:REL]->b foreach(n in nodes(p) : set n.touched = true ) "

    def query(literal: Expression): Query = {
      val secondQ = Query.
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), "touched"), literal)))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "r", "REL", Direction.OUTGOING))).
        tail(secondQ).
        returns(AllIdentifiers())
    }

    tests(string, query,
      Literal(true) -> testPre2_0,
      True() -> testFrom2_0)
  }

  @Test def returnAll() {
    testAll("start s = NODE(1) return *",
      Query.
        start(NodeById("s", 1)).
        returns(AllIdentifiers()))
  }

  @Test def single_create_unique() {
    val string = "start a = node(1), b=node(2) create unique a-[:reltype]->b"
    def query(relName: String): Query = {
      val secondQ = Query.
        unique(UniqueLink("a", "b", relName, "reltype", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    }

    tests(string, query,
      "  UNNAMED1" -> testPre2_0,
      "  UNNAMED44" -> testFrom2_0)
  }

  @Test def single_create_unique_with_rel() {
    val secondQ = Query.
      unique(UniqueLink("a", "b", "r", "reltype", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    testAll_Except_Experimental("start a = node(1), b=node(2) create unique a-[r:reltype]->b", q)
  }

  @Test def single_relate_with_empty_parenthesis() {
    val string = "start a = node(1), b=node(2) create unique a-[:reltype]->()"

    def query(DIFFERENCE: (String, String)): Query = {
    val secondQ = Query.
      unique(UniqueLink("a", DIFFERENCE._1, DIFFERENCE._2, "reltype", Direction.OUTGOING)).
      returns()

    Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    }

    tests(string, query,
      ("  UNNAMED1", "  UNNAMED2") -> testPre2_0,
      ("  UNNAMED58", "  UNNAMED44") -> testFrom2_0)
  }

  @Test def two_relates() {
    val string = "start a = node(1) create unique a-[:X]->b<-[:X]-c"
    def query(DIFFERENCE: (String, String)): Query = {
      val secondQ = Query.
        unique(
        UniqueLink("a", "b", DIFFERENCE._1, "X", Direction.OUTGOING),
        UniqueLink("c", "b", DIFFERENCE._2, "X", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    }

    tests(string, query,
      ("  UNNAMED1", "  UNNAMED2") -> testPre2_0,
      ("  UNNAMED33", "  UNNAMED41") -> testFrom2_0)
  }

  @Test def relate_with_initial_values_for_node() {
    val string = "start a = node(1) create unique a-[:X]->(b {name:'Andres'})"
    def query(DIFFERENCE: (String, Boolean)) = {
      val secondQ = Query.
        unique(
        UniqueLink(
          NamedExpectation("a", true),
          NamedExpectation("b", Map[String, Expression]("name" -> Literal("Andres")), DIFFERENCE._2),
          NamedExpectation(DIFFERENCE._1, true), "X", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    }

    tests(string, query,
      ("  UNNAMED1", true) -> testPre2_0,
      ("  UNNAMED33", false) -> testFrom2_0)
  }

  @Test def relate_with_initial_values_for_rel() {
    val string = "start a = node(1) create unique a-[:X {name:'Andres'}]->b"
    def query(DIFFERENCE: String) = {
      val secondQ = Query.
        unique(
        UniqueLink(
          NamedExpectation("a", true),
          NamedExpectation("b", true),
          NamedExpectation(DIFFERENCE, Map[String, Expression]("name" -> Literal("Andres")), true), "X", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    }
    tests(string, query,
      "  UNNAMED1" -> testPre2_0,
      "  UNNAMED33" -> testFrom2_0)
  }

  @Test def foreach_with_literal_collectionOld() {

    val q2 = Query.updates(
      ForeachAction(Collection(Literal(1.0), Literal(2.0), Literal(3.0)), "x", Seq(CreateNode("a", Map("number" -> Identifier("x")), Seq.empty)))
    ).returns()

    testPre2_0(
      "create root foreach(x in [1,2,3] : create (a {number:x}))",
      Query.
        start(CreateNodeStartItem(CreateNode("root", Map.empty, Seq.empty))).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def foreach_with_literal_collection() {
    testFrom2_0(
      "create root foreach(x in [1,2,3] : create (a {number:x}))",
      Query.
        start(CreateNodeStartItem(CreateNode("root", Map.empty, Seq.empty))).
        updates(ForeachAction(Collection(Literal(1.0), Literal(2.0), Literal(3.0)), "x", Seq(CreateNode("a", Map("number" -> Identifier("x")), Seq.empty, false)))).
        returns()
    )
  }

  @Test def string_literals_should_not_be_mistaken_for_identifiers() {
    val string = "create (tag1 {name:'tag2'}), (tag2 {name:'tag1'})"
    def query(bare: Boolean): Query = {
      Query.
        start(
        CreateNodeStartItem(CreateNode("tag1", Map("name" -> Literal("tag2")), Seq.empty, bare)),
        CreateNodeStartItem(CreateNode("tag2", Map("name" -> Literal("tag1")), Seq.empty, bare))
      ).returns()
    }
    tests(string, query,
      true -> testPre2_0,
      false -> testFrom2_0)
  }

  @Test def relate_with_two_rels_to_same_node() {
    val returns = Query.
      start(CreateUniqueStartItem(CreateUniqueAction(
      UniqueLink("root", "x", "r1", "X", Direction.OUTGOING),
      UniqueLink("root", "x", "r2", "Y", Direction.OUTGOING))))
      .returns(ReturnItem(Identifier("x"), "x"))

    val q = Query.start(NodeById("root", 0)).tail(returns).returns(AllIdentifiers())

    testAll_Except_Experimental(
      "start root=node(0) create unique x<-[r1:X]-root-[r2:Y]->x return x",
      q
    )
  }

  @Test def optional_shortest_path() {
    testAll(
      """start a  = node(1), x = node(2,3)
         match p = shortestPath(a -[?*]-> x)
         return *""",
      Query.
        start(NodeById("a", 1),NodeById("x", 2,3)).
        matches(ShortestPath("p", "a", "x", Seq(), Direction.OUTGOING, None, optional = true, single = true, relIterator = None)).
        returns(AllIdentifiers())
    )
  }

  @Test def return_paths_back_in_the_day() {
    testPre2_0("start a  = node(1) return a-->()",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(PathExpression(Seq(RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Seq(), Direction.OUTGOING, optional = false))), "a-->()"))
    )
  }

  @Test def return_paths() {
    testFrom2_0("start a  = node(1) return a-->()",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(PatternPredicate(Seq(RelatedTo("a", "  UNNAMED31", "  UNNAMED27", Seq(), Direction.OUTGOING, optional = false))), "a-->()"))
    )
  }

  @Test def not_with_parenthesis() {
    testAll("start a  = node(1) where not(1=2) or 2=3 return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(Not(Equals(Literal(1), Literal(2))), Equals(Literal(2), Literal(3)))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def full_path_in_create() {
    def query(DIFFERENCE: String) = {
      val secondQ = Query.
        start(
        CreateRelationshipStartItem(CreateRelationship("r1",
          RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier(DIFFERENCE), Map(), Seq.empty, true), "KNOWS", Map())),
        CreateRelationshipStartItem(CreateRelationship("r2",
          RelationshipEndpoint(Identifier(DIFFERENCE), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true), "LOVES", Map()))).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    }

    val string = "start a=node(1), b=node(2) create a-[r1:KNOWS]->()<-[r2:LOVES]->b"

    testFrom2_0(string, query("  UNNAMED49"))
    testPre2_0(string, query("  UNNAMED1"))
  }


  @Test def create_and_assign_to_path_identifierOld() {
    testPre2_0(
      "create p = a-[r:KNOWS]->() return p",
      Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Seq.empty, true), "KNOWS", Map()))).
      namedPaths(NamedPath("p", RelatedTo("a", "  UNNAMED1", "r", "KNOWS", Direction.OUTGOING, optional = false))).
      returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def create_and_assign_to_path_identifier() {
    testFrom2_0(
      "create p = a-[r:KNOWS]->() return p",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(Identifier("  UNNAMED25"), Map(), Seq.empty, true), "KNOWS", Map()))).
        namedPaths(NamedPath("p", RelatedTo("a", "  UNNAMED25", "r", "KNOWS", Direction.OUTGOING, optional = false))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def undirected_relationship() {
    val string = "create (a {name:'A'})-[:KNOWS]-(b {name:'B'})"
    def query(DIFFERENCE: (String, Boolean)): Query = Query.
      start(CreateRelationshipStartItem(CreateRelationship(DIFFERENCE._1,
      RelationshipEndpoint(Identifier("a"), Map("name" -> Literal("A")), Seq.empty, DIFFERENCE._2),
      RelationshipEndpoint(Identifier("b"), Map("name" -> Literal("B")), Seq.empty, DIFFERENCE._2), "KNOWS", Map()))).
      returns()

    tests(string, query,
      ("  UNNAMED1", true) -> testPre2_0,
      ("  UNNAMED21", false) -> testFrom2_0)
  }

  @Test def relate_and_assign_to_path_identifier() {
    val string = "start a=node(0) create unique p = a-[r:KNOWS]->() return p"
    def query(DIFFERENCE: String) = {
      val q2 = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink("a", DIFFERENCE, "r", "KNOWS", Direction.OUTGOING)))).
        namedPaths(NamedPath("p", RelatedTo("a", DIFFERENCE, "r", "KNOWS", Direction.OUTGOING, optional = false))).
        returns(ReturnItem(Identifier("p"), "p"))

      Query.
        start(NodeById("a", 0)).
        tail(q2).
        returns(AllIdentifiers())
    }

    tests(string, query,
    "  UNNAMED1"->testPre2_0,
    "  UNNAMED48"->testFrom2_0)
  }

  @Test def use_predicate_as_expression() {
    testFrom1_9("start n=node(0) return id(n) = 0, n is null",
      Query.
        start(NodeById("n", 0)).
        returns(
        ReturnItem(Equals(IdFunction(Identifier("n")), Literal(0)), "id(n) = 0"),
        ReturnItem(IsNull(Identifier("n")), "n is null")
      ))
  }

  @Test def create_unique_should_support_parameter_maps() {
    val start = NamedExpectation("n", true)
    val rel = NamedExpectation("  UNNAMED31", true)
    val end = new NamedExpectation("  UNNAMED41", ParameterExpression("param"), Map.empty, Seq.empty, true)

    val secondQ = Query.
                  unique(UniqueLink(start, end, rel, "foo", Direction.OUTGOING)).
                  returns(AllIdentifiers())

    testFrom2_0("START n=node(0) CREATE UNIQUE n-[:foo]->({param}) RETURN *",
                 Query.
                 start(NodeById("n", 0)).
                 tail(secondQ).
                 returns(AllIdentifiers()))
  }

  @Test def create_unique_should_support_parameter_maps_1_9() {
    val start = NamedExpectation("n", bare = true)
    val rel = NamedExpectation("  UNNAMED2", bare = true)
    val end = new NamedExpectation("  UNNAMED1", ParameterExpression("param"), Map.empty, Seq.empty, true)

    val secondQ = Query.
      unique(UniqueLink(start, end, rel, "foo", Direction.OUTGOING)).
      returns(AllIdentifiers())

    test1_9("START n=node(0) CREATE UNIQUE n-[:foo]->({param}) RETURN *",
      Query.
        start(NodeById("n", 0)).
        tail(secondQ).
        returns(AllIdentifiers()))
  }


  @Test def with_limit() {
    testFrom1_9("start n=node(0,1,2) with n limit 2 where ID(n) = 1 return n",
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
    testFrom1_9("start n=node(0,1,2) with n order by ID(n) desc limit 2 where ID(n) = 1 return n",
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

    testFrom1_9("start n=node(0) set n = {prop}",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers()))
  }

  @Test def add_label() {
    val q2 = Query.
      start().
      updates(LabelAction(Identifier("n"), LabelSetOp, List(LabelName("LabelName")))).
      returns()

    testFrom2_0("START n=node(0) set n:LabelName",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def add_short_label() {
    val q2 = Query.
      start().
      updates(LabelAction(Identifier("n"), LabelSetOp, List(LabelName("LabelName")))).
      returns()

    testFrom2_0("START n=node(0) SET n:LabelName",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def add_multiple_labels() {
    val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
    val q2   = Query.
      start().
      updates(LabelAction(Identifier("n"), LabelSetOp, coll)).
      returns()

    testFrom2_0("START n=node(0) set n :LabelName2 :LabelName3",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def add_multiple_short_labels() {
    val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
    val q2   = Query.
      start().
      updates(LabelAction(Identifier("n"), LabelSetOp, coll)).
      returns()

    testFrom2_0("START n=node(0) set n:LabelName2:LabelName3",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def add_multiple_short_labels2() {
    val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
    val q2   = Query.
      start().
      updates(LabelAction(Identifier("n"), LabelSetOp, coll)).
      returns()

    testFrom2_0("START n=node(0) SET n :LabelName2 :LabelName3",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def remove_label() {
    val q2 = Query.
      start().
      updates(LabelAction(Identifier("n"), LabelRemoveOp, LabelSupport.labelCollection("LabelName"))).
      returns()

    testFrom2_0("START n=node(0) REMOVE n:LabelName",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def remove_multiple_labels() {
    val coll = LabelSupport.labelCollection("LabelName2", "LabelName3")
    val q2   = Query.
      start().
      updates(LabelAction(Identifier("n"), LabelRemoveOp, coll)).
      returns()

    testFrom2_0("START n=node(0) REMOVE n:LabelName2:LabelName3",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def filter_by_label_in_where() {
    testFrom2_0("START n=node(0) WHERE n:Foo RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(HasLabel(Identifier("n"), Seq(LabelName("Foo")))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test def filter_by_labels_in_where() {
    testFrom2_0("START n=node(0) WHERE n:Foo:Bar RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(HasLabel(Identifier("n"), Seq(LabelName("Foo"), LabelName("Bar")))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test(expected = classOf[SyntaxException]) def create_no_index_without_properties() {
    testFrom2_0("create index on :MyLabel",
      CreateIndex("MyLabel", Seq()))
  }

  @Test def create_index_on_single_property() {
    testFrom2_0("create index on :MyLabel(prop1)",
      CreateIndex("MyLabel", Seq("prop1")))
  }

  @Test(expected = classOf[SyntaxException]) def create_index_on_multiple_properties() {
    testFrom2_0("create index on :MyLabel(prop1, prop2)",
      CreateIndex("MyLabel", Seq("prop1", "prop2")))
  }

  @Test def match_left_with_single_label() {
    val query    = "start a = NODE(1) match a:foo -[r:MARRIED]-> () return a"
    val pred     = HasLabel(Identifier("a"), Seq(LabelName("foo")))
    val expected =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED46", "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        where(pred).
        returns(ReturnItem(Identifier("a"), "a"))

    testFrom2_0(query, expected)
  }

  @Test def match_left_with_multiple_labels() {
    val query    = "start a = NODE(1) match a:foo:bar -[r:MARRIED]-> () return a"
    val pred     = HasLabel(Identifier("a"), Seq(LabelName("foo"), LabelName("bar")))
    val expected =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED50", "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        where(pred).
        returns(ReturnItem(Identifier("a"), "a"))

    testFrom2_0(query, expected)
  }

  @Test def match_right_with_multiple_labels() {
    val query    = "start a = NODE(1) match () -[r:MARRIED]-> a:foo:bar return a"
    val pred     = HasLabel(Identifier("a"), Seq(LabelName("foo"), LabelName("bar")))
    val expected =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("  UNNAMED25", "a", "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        where(pred).
        returns(ReturnItem(Identifier("a"), "a"))

    testFrom2_0(query, expected)
  }

  @Test def match_both_with_labels() {
    val query    = "start a = NODE(1) match b:foo -[r:MARRIED]-> a:bar return a"
    val pred     = And(HasLabel(Identifier("b"), Seq(LabelName("foo"))),
                       HasLabel(Identifier("a"), Seq(LabelName("bar"))))
    val expected =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        where(pred).
        returns(ReturnItem(Identifier("a"), "a"))

    testFrom2_0(query, expected)
  }

  @Test def match_left_with_label_choice() {
    val query    = "start a = NODE(1) match a:foo:bar|:baz -[r:MARRIED]-> () return a"
    val pred     = Or(
      HasLabel(Identifier("a"), Seq(LabelName("foo"), LabelName("bar"))),
      HasLabel(Identifier("a"), Seq(LabelName("baz"))))
    val expected =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED55", "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        where(pred).
        returns(ReturnItem(Identifier("a"), "a"))

    testFrom2_0(query, expected)
  }

  @Test def match_right_with_label_choice() {
    val query    = "start a = NODE(1) match () -[r:MARRIED]-> a:foo:bar|:baz return a"
    val pred     = Or(
      HasLabel(Identifier("a"), Seq(LabelName("foo"), LabelName("bar"))),
      HasLabel(Identifier("a"), Seq(LabelName("baz"))))
    val expected =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("  UNNAMED25", "a", "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        where(pred).
        returns(ReturnItem(Identifier("a"), "a"))

    testFrom2_0(query, expected)
  }

  @Test def match_both_with_label_choice() {
    val query    = "start a = NODE(1) match b:foo|:red -[r:MARRIED]-> a:bar|:blue return a"
    val pred     = And(Or(
                        HasLabel(Identifier("b"), Seq(LabelName("foo"))),
                        HasLabel(Identifier("b"), Seq(LabelName("red")))),
                       Or(
                         HasLabel(Identifier("a"), Seq(LabelName("bar"))),
                         HasLabel(Identifier("a"), Seq(LabelName("blue")))))
    val expected =
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        where(pred).
        returns(ReturnItem(Identifier("a"), "a"))

    testFrom2_0(query, expected)
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

  @Test def union_ftw() {
    val q = Query.
      start(NodeById("s", 1)).
      returns(ReturnItem(Identifier("s"), "s"))

    testFrom2_0("start s = NODE(1) return s UNION all start s = NODE(1) return s",
      Union(Seq(q, q), QueryString.empty, distinct = false))
  }

  @Test def union_distinct() {
    val q = Query.
      start(NodeById("s", 1)).
      returns(ReturnItem(Identifier("s"), "s"))

    testFrom2_0("start s = NODE(1) return s UNION start s = NODE(1) return s",
      Union(Seq(q, q), QueryString.empty, distinct = true))
  }

  @Test def keywords_in_reltype_and_label() {
    testFrom2_0("START n=node(0) MATCH n:On-[:WHERE]->() RETURN n",
      Query.
        start(NodeById("n", 0)).
        matches(RelatedTo("n", "  UNNAMED38", "  UNNAMED26", Seq("WHERE"), Direction.OUTGOING, false)).
        where(HasLabel(Identifier("n"), Seq(LabelName("On")))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test def remove_index_on_single_property() {
    testFrom2_0("drop index on :MyLabel(prop1)",
      DropIndex("MyLabel", Seq("prop1")))
  }

  @Test def simple_query_with_index_hint() {
    testFrom2_0("match n:Person-->() using index n:Person(name) where n.name = 'Andres' return n",
      Query.matches(RelatedTo("n", "  UNNAMED18", "  UNNAMED14", Seq(), Direction.OUTGOING, optional = false)).
        where(And(Equals(Property(Identifier("n"), "name"), Literal("Andres")), HasLabel(Identifier("n"), Seq(LabelName("Person"))))).
        usingIndex(SchemaIndex("n", "Person", "name", None)).
        returns(ReturnItem(Identifier("n"), "n", renamed = false)))
  }

  @Test def single_node_match_pattern() {
    testFrom1_9("start s = node(*) match s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        returns(ReturnItem(Identifier("s"), "s")))
  }


  @Test def awesome_single_labeled_node_match_pattern() {
    testFrom2_0("match s:nostart return s",
      Query.
        matches(SingleNode("s")).
        where(HasLabel(Identifier("s"), Seq(LabelName("nostart")))).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def single_node_match_pattern_path() {
    testFrom1_9("start s = node(*) match p = s return s",
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


  def test1_9(query: String, expectedQuery: AbstractQuery) {
    testQuery(Some("1.9"), query, expectedQuery)
  }

  def testFrom1_9(query: String, expectedQuery: AbstractQuery) {
    test1_9(query, expectedQuery)
    test2_0(query, expectedQuery)
  }

  def testFrom2_0(query: String, expectedQuery: AbstractQuery) {
    test2_0(query, expectedQuery)
  }

  def test2_0(query: String, expectedQuery: AbstractQuery) {
    testQuery(None, query, expectedQuery)
    testQuery(None, query + ";", expectedQuery)
  }

  def testExperimental(query:String, expectedQuery: AbstractQuery) {
    testQuery(Some("experimental"), query, expectedQuery)
    testQuery(Some("experimental"), query + ";", expectedQuery)
  }


  def testAll(query: String, expectedQuery: AbstractQuery) {
    testAll_Except_Experimental(query, expectedQuery)
    testExperimental(query, expectedQuery)
  }

  def testAll_Except_Experimental(query: String, expectedQuery: AbstractQuery) {
    test2_0(query, expectedQuery)
    test1_9(query, expectedQuery)
  }

  def testPre2_0(query: String, expectedQuery: AbstractQuery) {
    test1_9(query, expectedQuery)
  }

  def testQuery(version: Option[String], query: String, expectedQuery: AbstractQuery) {
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
