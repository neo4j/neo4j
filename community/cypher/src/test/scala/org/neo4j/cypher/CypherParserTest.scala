/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import internal.mutation._
import internal.parser.v1_6.ConsoleCypherParser
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
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def allTheNodes() {
    testFrom_1_7("start s = NODE(*) return s",
      Query.
        start(AllNodes("s")).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def allTheRels() {
    testFrom_1_7("start r = relationship(*) return r",
      Query.
        start(AllRelationships("r")).
        returns(ReturnItem(Entity("r"), "r")))
  }

  @Test def shouldHandleAliasingOfColumnNames() {
    test_1_6("start s = NODE(1) return s as somethingElse",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Entity("s"), "somethingElse", true)))
  }

  @Test def sourceIsAnIndex() {
    testAll(
      """start a = node:index(key = "value") return a""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def sourceIsAnNonParsedIndexQuery() {
    testAll(
      """start a = node:index("key:value") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Ignore
  @Test def sourceIsParsedAdvancedLuceneQuery() {
    testAll(
      """start a = node:index(key="value" AND otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" AND otherKey:\"otherValue\""))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Ignore
  @Test def parsedOrIdxQuery() {
    testAll(
      """start a = node:index(key="value" or otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" OR otherKey:\"otherValue\""))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    testAll(
      "start s = relationship(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def shouldParseEasiestPossibleRelationshipQueryShort() {
    testAll(
      "start s = rel(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def sourceIsARelationshipIndex() {
    testAll(
      """start a = rel:index(key = "value") return a""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def escapedNamesShouldNotContainEscapeChars() {
    testAll(
      """start `a a` = rel:`index a`(`key s` = "value") return `a a`""",
      Query.
        start(RelationshipByIndex("a a", "index a", Literal("key s"), Literal("value"))).
        returns(ReturnItem(Entity("a a"), "a a")))
  }

  @Test def keywordsShouldBeCaseInsensitive() {
    testAll(
      "START s = NODE(1) RETURN s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def shouldParseMultipleNodes() {
    testAll(
      "start s = NODE(1,2,3) return s",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def shouldParseMultipleInputs() {
    testAll(
      "start a = node(1), b = NODE(2) return a,b",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b")))
  }

  @Test def shouldFilterOnProp() {
    testAll(
      "start a = NODE(1) where a.name = \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property("a", "name"), Literal("andres"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldReturnLiterals1_6() {
    test_1_6(
      "start a = NODE(1) return 12",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Literal(12L), "12.0")))
  }

  @Test def shouldReturnLiterals() {
    testFrom_1_7(
      "start a = NODE(1) return 12",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Literal(12), "12")))
  }

  @Test def shouldReturnAdditions() {
    testFrom_1_7(
      "start a = NODE(1) return 12+2",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Add(Literal(12), Literal(2)), "12+2")))
  }

  @Test def arithmeticsPrecedence() {
    testFrom_1_7(
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
        where(Equals(Property("a", "extractReturnItems"), Literal(3.1415))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleNot() {
    testAll(
      "start a = node(1) where not(a.name = \"andres\") return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property("a", "name"), Literal("andres")))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleNotEqualTo() {
    testAll(
      "start a = node(1) where a.name <> \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property("a", "name"), Literal("andres")))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleLessThan() {
    testAll(
      "start a = node(1) where a.name < \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(Property("a", "name"), Literal("andres"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleGreaterThan() {
    testAll(
      "start a = node(1) where a.name > \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(Property("a", "name"), Literal("andres"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleLessThanOrEqual() {
    testAll(
      "start a = node(1) where a.name <= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(Property("a", "name"), Literal("andres"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleRegularComparisonOlder() {
    test_1_6(
      "start a = node(1) where \"Andres\" =~ /And.*/ return a",
      Query.
        start(NodeById("a", 1)).
        where(RegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleRegularComparison() {
    testFrom_1_7(
      "start a = node(1) where \"Andres\" =~ /And.*/ return a",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ReturnItem(Entity("a"), "a"))
    )
  }

  @Test def shouldHandleMultipleRegularComparison1_6() {
    testFrom_1_7(
      """start a = node(1) where a.name =~ /And.*/ AnD a.name =~ /And.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(LiteralRegularExpression(Property("a", "name"), Literal("And.*")), LiteralRegularExpression(Property("a", "name"), Literal("And.*")))).
        returns(ReturnItem(Entity("a"), "a"))
    )
  }

  @Test def shouldHandleMultipleRegularComparison() {
    test_1_6(
      """start a = node(1) where a.name =~ /And.*/ AnD a.name =~ /And.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(RegularExpression(Property("a", "name"), Literal("And.*")), RegularExpression(Property("a", "name"), Literal("And.*")))).
        returns(ReturnItem(Entity("a"), "a"))
    )
  }

  @Test def shouldHandleEscapedRegexs_older() {
    test_1_6(
      """start a = node(1) where a.name =~ /And\/.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(RegularExpression(Property("a", "name"), Literal("And\\/.*"))).
        returns(ReturnItem(Entity("a"), "a"))
    )
  }

  @Test def shouldHandleEscapedRegexs() {
    testFrom_1_7(
      """start a = node(1) where a.name =~ /And\/.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Property("a", "name"), Literal("And\\/.*"))).
        returns(ReturnItem(Entity("a"), "a"))
    )
  }

  @Test def shouldHandleGreaterThanOrEqual() {
    testAll(
      "start a = node(1) where a.name >= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThanOrEqual(Property("a", "name"), Literal("andres"))).
        returns(ReturnItem(Entity("a"), "a")))
  }


  @Test def booleanLiterals() {
    testAll(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(true), Literal(false))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldFilterOnNumericProp() {
    testAll(
      "start a = NODE(1) where 35 = a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), Property("a", "age"))).
        returns(ReturnItem(Entity("a"), "a")))
  }


  @Test def shouldHandleNegativeLiteralsAsExpected() {
    test_1_6(
      "start a = NODE(1) where -35 = a.age AND a.age > -1.2 return a",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Literal(-35), Property("a", "age")),
        GreaterThan(Property("a", "age"), Literal(-1.2)))
      ).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldCreateNotEqualsQuery() {
    testAll(
      "start a = NODE(1) where 35 <> a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), Property("a", "age")))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def multipleFilters() {
    testAll(
      "start a = NODE(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(Property("a", "name"), Literal("andres")),
        Equals(Property("a", "name"), Literal("mattias")))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def relatedTo() {
    testAll(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b")))
  }

  @Test def relatedToWithoutRelType() {
    testAll(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b")))
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testAll(
      "start a = NODE(1) match a -[r]-> (b) return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("r"), "r")))
  }

  @Test def relatedToTheOtherWay() {
    testAll(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.INCOMING, false, True())).
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b")))
  }

  @Test def shouldOutputVariables() {
    testAll(
      "start a = NODE(1) return a.name",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Property("a", "name"), "a.name")))
  }

  @Test def shouldHandleAndPredicates() {
    testAll(
      "start a = NODE(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Property("a", "name"), Literal("andres")),
        Equals(Property("a", "lastname"), Literal("taylor")))).
        returns(ReturnItem(Property("a", "name"), "a.name")))
  }

  @Test def relatedToWithRelationOutput() {
    testAll(
      "start a = NODE(1) match a -[rel:KNOWS]-> (b) return rel",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Seq("KNOWS"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("rel"), "rel")))
  }

  @Test def relatedToWithoutEndName() {
    testAll(
      "start a = NODE(1) match a -[:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Seq("MARRIED"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def relatedInTwoSteps() {
    testAll(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING, false, True()),
        RelatedTo("b", "c", "  UNNAMED2", Seq("FRIEND"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("c"), "c"))
    )
  }

  @Test def djangoRelationshipType() {
    testAll(
      "start a = NODE(1) match a -[:`<<KNOWS>>`]-> b return c",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq("<<KNOWS>>"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("c"), "c")))
  }

  @Test def countTheNumberOfHits() {
    testAll(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"), ReturnItem(CountStar(), "count(*)")))
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
      "start a = NODE(1) match a --> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation().
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b")))
  }

  @Test def sumTheAgesOfPeople() {
    testAll(
      "start a = NODE(1) match a --> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Sum(Property("a", "age"))).
        columns("a", "b", "sum(a.age)").
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"), ReturnItem(Sum(Property("a", "age")), "sum(a.age)")))
  }

  @Test def avgTheAgesOfPeople() {
    testAll(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Avg(Property("a", "age"))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"), ReturnItem(Avg(Property("a", "age")), "avg(a.age)")))
  }

  @Test def minTheAgesOfPeople() {
    testAll(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Min(Property("a", "age"))).
        columns("a", "b", "min(a.age)").
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"), ReturnItem(Min(Property("a", "age")), "min(a.age)")))
  }

  @Test def maxTheAgesOfPeople() {
    testAll(
      "start a = NODE(1) match a --> b return a, b, max(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())).
        aggregation(Max((Property("a", "age")))).
        columns("a", "b", "max(a.age)").
        returns(
        ReturnItem(Entity("a"), "a"),
        ReturnItem(Entity("b"), "b"),
        ReturnItem(Max((Property("a", "age"))), "max(a.age)")
      ))
  }

  @Test def singleColumnSorting() {
    testAll(
      "start a = NODE(1) return a order by a.name",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property("a", "name"), true)).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def sortOnAggregatedColumn() {
    testAll(
      "start a = NODE(1) return a order by avg(a.name)",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Avg(Property("a", "name")), true)).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleTwoSortColumns() {
    testAll(
      "start a = NODE(1) return a order by a.name, a.age",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property("a", "name"), true),
        SortItem(Property("a", "age"), true)).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleTwoSortColumnsAscending() {
    testAll(
      "start a = NODE(1) return a order by a.name ASCENDING, a.age ASC",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property("a", "name"), true),
        SortItem(Property("a", "age"), true)).
        returns(ReturnItem(Entity("a"), "a")))

  }

  @Test def orderByDescending() {
    testAll(
      "start a = NODE(1) return a order by a.name DESCENDING",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property("a", "name"), false)).
        returns(ReturnItem(Entity("a"), "a")))

  }

  @Test def orderByDesc() {
    testAll(
      "start a = NODE(1) return a order by a.name desc",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property("a", "name"), false)).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def nullableProperty() {
    testAll(
      "start a = NODE(1) return a.name?",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Nullable(Property("a", "name")), "a.name?")))
  }

  @Test def nestedBooleanOperatorsAndParentesis() {
    testAll(
      """start n = NODE(1,2,3) where (n.animal = "monkey" and n.food = "banana") or (n.animal = "cow" and n
      .food="grass") return n""",
      Query.
        start(NodeById("n", 1, 2, 3)).
        where(Or(
        And(
          Equals(Property("n", "animal"), Literal("monkey")),
          Equals(Property("n", "food"), Literal("banana"))),
        And(
          Equals(Property("n", "animal"), Literal("cow")),
          Equals(Property("n", "food"), Literal("grass"))))).
        returns(ReturnItem(Entity("n"), "n")))
  }

  @Test def limit5() {
    testAll(
      "start n=NODE(1) return n limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        returns(ReturnItem(Entity("n"), "n")))
  }

  @Test def skip5() {
    testAll(
      "start n=NODE(1) return n skip 5",
      Query.
        start(NodeById("n", 1)).
        skip(5).
        returns(ReturnItem(Entity("n"), "n")))
  }

  @Test def skip5limit5() {
    testAll(
      "start n=NODE(1) return n skip 5 limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        skip(5).
        returns(ReturnItem(Entity("n"), "n")))
  }

  @Test def relationshipType() {
    testAll(
      "start n=NODE(1) match n-[r]->(x) where type(r) = \"something\" return r",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        where(Equals(RelationshipTypeFunction(Entity("r")), Literal("something"))).
        returns(ReturnItem(Entity("r"), "r")))
  }

  @Test def pathLength() {
    testAll(
      "start n=NODE(1) match p=(n-->x) where LENGTH(p) = 10 return p",
      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()))).
        where(Equals(LengthFunction(Entity("p")), Literal(10.0))).
        returns(ReturnItem(Entity("p"), "p")))
  }

  @Test def relationshipTypeOut() {
    testAll(
      "start n=NODE(1) match n-[r]->(x) return TYPE(r)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(RelationshipTypeFunction(Entity("r")), "TYPE(r)")))
  }


  @Test def shouldBeAbleToParseCoalesce() {
    test_1_6(
      "start n=NODE(1) match n-[r]->(x) return COALESCE(r.name,x.name)",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING, false, True())).
        returns(ReturnItem(CoalesceFunction(Property("r", "name"), Property("x", "name")), "COALESCE(r.name,x.name)")))
  }

  @Test def relationshipsFromPathOutput() {
    testAll(
      "start n=NODE(1) match p=n-->x return RELATIONSHIPS(p)",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()))).
        returns(ReturnItem(RelationshipFunction(Entity("p")), "RELATIONSHIPS(p)")))
  }

  @Test def relationshipsFromPathInWhere() {
    testAll(
      "start n=NODE(1) match p=n-->x where length(rels(p))=1 return p",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()))).
        where(Equals(LengthFunction(RelationshipFunction(Entity("p"))), Literal(1)))
        returns (ReturnItem(Entity("p"), "p")))
  }

  @Test def countNonNullValues() {
    testAll(
      "start a = NODE(1) return a, count(a)",
      Query.
        start(NodeById("a", 1)).
        aggregation(Count(Entity("a"))).
        columns("a", "count(a)").
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Count(Entity("a")), "count(a)")))
  }

  @Test def shouldHandleIdBothInReturnAndWhere() {
    testAll(
      "start a = NODE(1) where id(a) = 0 return ID(a)",
      Query.
        start(NodeById("a", 1)).
        where(Equals(IdFunction(Entity("a")), Literal(0)))
        returns (ReturnItem(IdFunction(Entity("a")), "ID(a)")))
  }

  @Test def shouldBeAbleToHandleStringLiteralsWithApostrophe() {
    testAll(
      "start a = node:index(key = 'value') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleQuotationsInsideApostrophes() {
    testAll(
      "start a = node:index(key = 'val\"ue') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("val\"ue"))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def simplePathExample() {
    testAll(
      "start a = node(0) match p = ( a-->b ) return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def threeStepsPath() {
    testAll(
      "start a = node(0) match p = ( a-->b-->c ) return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p",
        RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()),
        RelatedTo("b", "c", "  UNNAMED2", Seq(), Direction.OUTGOING, false, True())
      ))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def pathsShouldBePossibleWithoutParenthesis() {
    testAll(
      "start a = node(0) match p = a-->b return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True())))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def variableLengthPath() {
    testAll("start a=node(0) match a -[:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelsIterable() {
    testAll("start a=node(0) match a -[r:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), Seq("knows"), Direction.OUTGOING, Some("r"), false, True())).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def fixedVarLengthPath() {
    testAll("start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(3), Some(3), Seq(), Direction.OUTGOING, None, false, True())).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMinDepth() {
    testAll("start a=node(0) match a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    testAll("start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, Seq("knows"), Direction.OUTGOING, Some("r"), false, True())).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    testAll("start a=node(0) match a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def unboundVariableLengthPath() {
    testAll("start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def optionalRelationship() {
    testAll(
      "start a = node(1) match a -[?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Entity("b"), "b")))
  }

  @Test def questionMarkOperator() {
    testAll(
      "start a = node(1) where a.prop? = 42 return a",
      Query.
        start(NodeById("a", 1)).
        where(NullablePredicate(Equals(Nullable(Property("a", "prop")), Literal(42.0)), Seq((Nullable(Property("a", "prop")), true)))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def exclamationMarkOperator() {
    testFrom_1_7(
      "start a = node(1) where a.prop! = 42 return a",
      Query.
        start(NodeById("a", 1)).
        where(NullablePredicate(Equals(Nullable(Property("a", "prop")), Literal(42)), Seq((Nullable(Property("a", "prop")), false)))).
        returns(ReturnItem(Entity("a"), "a")))
  }

  @Test def optionalTypedRelationship() {
    testAll(
      "start a = node(1) match a -[?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Entity("b"), "b")))
  }

  @Test def optionalTypedAndNamedRelationship() {
    testAll(
      "start a = node(1) match a -[r?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("KNOWS"), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Entity("b"), "b")))
  }

  @Test def optionalNamedRelationship() {
    testAll(
      "start a = node(1) match a -[r?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, true, True())).
        returns(ReturnItem(Entity("b"), "b")))
  }

  @Test def testOnAllNodesInAPath() {
    test_1_6(
      """start a = node(1) match p = a --> b --> c where ALL(n in NODES(p) where n.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        namedPaths(
        NamedPath("p",
          RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()),
          RelatedTo("b", "c", "  UNNAMED2", Seq(), Direction.OUTGOING, false, True()))).
        where(AllInIterable(NodesFunction(Entity("p")), "n", Equals(Property("n", "name"), Literal("Andres"))))
        returns (ReturnItem(Entity("b"), "b")))
  }

  @Test def extractNameFromAllNodes() {
    test_1_6(
      """start a = node(1) match p = a --> b --> c return extract(n in NODES(p) : n.name)""",
      Query.
        start(NodeById("a", 1)).
        namedPaths(
        NamedPath("p",
          RelatedTo("a", "b", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()),
          RelatedTo("b", "c", "  UNNAMED2", Seq(), Direction.OUTGOING, false, True()))).
        returns(ReturnItem(ExtractFunction(NodesFunction(Entity("p")), "n", Property("n", "name")), "extract(n in NODES(p) : n.name)")))
  }


  @Test def testAny() {
    test_1_6(
      """start a = node(1) where ANY(x in NODES(p) where x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(AnyInIterable(NodesFunction(Entity("p")), "x", Equals(Property("x", "name"), Literal("Andres"))))
        returns (ReturnItem(Entity("b"), "b")))
  }

  @Test def testNone() {
    test_1_6(
      """start a = node(1) where none(x in nodes(p) where x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(NoneInIterable(NodesFunction(Entity("p")), "x", Equals(Property("x", "name"), Literal("Andres"))))
        returns (ReturnItem(Entity("b"), "b")))
  }

  @Test def testSingle() {
    test_1_6(
      """start a = node(1) where single(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(SingleInIterable(NodesFunction(Entity("p")), "x", Equals(Property("x", "name"),
        Literal("Andres"))))
        returns (ReturnItem(Entity("b"), "b")))
  }

  @Test def testParamAsStartNode() {
    testAll(
      """start pA = node({a}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testNumericParamNameAsStartNode() {
    testAll(
      """start pA = node({0}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("0"))).
        returns(ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForWhereLiteral() {
    testAll(
      """start pA = node(1) where pA.name = {name} return pA""",
      Query.
        start(NodeById("pA", 1)).
        where(Equals(Property("pA", "name"), ParameterExpression("name")))
        returns (ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForIndexKey() {
    testAll(
      """start pA = node:idx({key} = "Value") return pA""",
      Query.
        start(NodeByIndex("pA", "idx", ParameterExpression("key"), Literal("Value"))).
        returns(ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForIndexValue() {
    testAll(
      """start pA = node:idx(key = {Value}) return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), ParameterExpression("Value"))).
        returns(ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForIndexQuery() {
    testAll(
      """start pA = node:idx({query}) return pA""",
      Query.
        start(NodeByIndexQuery("pA", "idx", ParameterExpression("query"))).
        returns(ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForSkip() {
    testAll(
      """start pA = node(0) return pA skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns (ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForLimit() {
    testAll(
      """start pA = node(0) return pA limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns (ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForLimitAndSkip() {
    testAll(
      """start pA = node(0) return pA skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ("stop")
        returns (ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testParamForRegex() {
    testAll(
      """start pA = node(0) where pA.name =~ {regex} return pA""",
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(Property("pA", "name"), ParameterExpression("regex")))
        returns (ReturnItem(Entity("pA"), "pA")))
  }

  @Test def testShortestPath() {
    test_1_6(
      """start a=node(0), b=node(1) match p = shortestPath( a-->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq(), Direction.OUTGOING, Some(1), false, true, None))
        returns (ReturnItem(Entity("p"), "p")))
  }

  @Test def testShortestPathWithMaxDepth() {
    test_1_6(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq(), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Entity("p"), "p")))
  }

  @Test def testShortestPathWithType() {
    test_1_6(
      """start a=node(0), b=node(1) match p = shortestPath( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq("KNOWS"), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Entity("p"), "p")))
  }

  @Test def testShortestPathBiDirectional() {
    test_1_6(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]-b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq(), Direction.BOTH, Some(6), false, true, None)).
        returns(ReturnItem(Entity("p"), "p")))
  }

  @Test def testShortestPathOptional() {
    test_1_6(
      """start a=node(0), b=node(1) match p = shortestPath( a-[?*..6]-b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq(), Direction.BOTH, Some(6), true, true, None)).
        returns(ReturnItem(Entity("p"), "p")))
  }

  @Test def testAllShortestPath() {
    test_1_6(
      """start a=node(0), b=node(1) match p = allShortestPaths( a-[*]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Seq(), Direction.OUTGOING, None, false, false, None)).
        returns(ReturnItem(Entity("p"), "p")))
  }

  @Test def testForNull() {
    testAll(
      """start a=node(0) where a is null return a""",
      Query.
        start(NodeById("a", 0)).
        where(IsNull(Entity("a")))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def testForNotNull() {
    testAll(
      """start a=node(0) where a is not null return a""",
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(Entity("a"))))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def testCountDistinct() {
    testAll(
      """start a=node(0) return count(distinct a)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(Distinct(Count(Entity("a")), Entity("a"))).
        columns("count(distinct a)")
        returns (ReturnItem(Distinct(Count(Entity("a")), Entity("a")), "count(distinct a)")))
  }

  @Test def consoleModeParserShouldOutputNullableProperties() {
    val query = "start a = node(1) return a.name"
    val parser = new ConsoleCypherParser()
    val executionTree = parser.parse(query)

    assertEquals(
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Nullable(Property("a", "name")), "a.name?")),
      executionTree)
  }

  @Test def supportsHasRelationshipInTheWhereClause() {
    test_1_6(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Entity("a"), Entity("b"), Direction.OUTGOING, Seq()))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def supportsHasRelationshipWithoutDirectionInTheWhereClause() {
    test_1_6(
      """start a=node(0), b=node(1) where a-[:KNOWS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Entity("a"), Entity("b"), Direction.BOTH, Seq("KNOWS")))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def supportsHasRelationshipWithoutDirectionInTheWhereClause2() {
    test_1_6(
      """start a=node(0), b=node(1) where a--b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Entity("a"), Entity("b"), Direction.BOTH, Seq()))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldSupportHasRelationshipToAnyNode() {
    test_1_6(
      """start a=node(0) where a-->() return a""",
      Query.
        start(NodeById("a", 0)).
        where(HasRelationship(Entity("a"), Direction.OUTGOING, Seq()))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldHandleLFAsWhiteSpace() {
    testAll(
      "start\na=node(0)\nwhere\na.prop=12\nreturn\na",
      Query.
        start(NodeById("a", 0)).
        where(Equals(Property("a", "prop"), Literal(12)))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def shouldAcceptRelationshipWithPredicate() {
    testFrom_1_7(
      "start a = node(1) match a-[r WHERE r.foo = 'bar']->b return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING, false, Equals(Property("r", "foo"), Literal("bar"))))
        returns (ReturnItem(Entity("b"), "b")))
  }

  @Test def shouldHandleUpperCaseDistinct() {
    testAll("start s = NODE(1) return DISTINCT s",
      Query.
        start(NodeById("s", 1)).
        aggregation().
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def shouldParseMathFunctions() {
    testFrom_1_7("start s = NODE(0) return 5 % 4, abs(-1), round(3.1415), 2 ^ 8, sqrt(16), sign(1)",
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
    testFrom_1_7("start s = NODE(1) return s // COMMENT",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def shouldAllowCommentAlone() {
    testFrom_1_7("""start s = NODE(1) return s
    // COMMENT""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def shouldAllowCommentsInsideStrings() {
    testFrom_1_7("start s = NODE(1) where s.apa = '//NOT A COMMENT' return s",
      Query.
        start(NodeById("s", 1)).
        where(Equals(Property("s", "apa"), Literal("//NOT A COMMENT")))
        returns (ReturnItem(Entity("s"), "s")))
  }

  @Test def shouldHandleCommentsFollowedByWhiteSpace() {
    testFrom_1_7("""start s = NODE(1)
    //I can haz more comment?
    return s""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Entity("s"), "s")))
  }

  @Test def first_last_and_rest() {
    testFrom_1_7("start x = NODE(1) match p=x-->z return head(nodes(p)), last(nodes(p)), tail(nodes(p))",
      Query.
        start(NodeById("x", 1)).
        namedPaths(NamedPath("p", RelatedTo("x", "z", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()))).
        returns(
        ReturnItem(HeadFunction(NodesFunction(Entity("p"))), "head(nodes(p))"),
        ReturnItem(LastFunction(NodesFunction(Entity("p"))), "last(nodes(p))"),
        ReturnItem(TailFunction(NodesFunction(Entity("p"))), "tail(nodes(p))")
      ))
  }

  @Test def filter() {
    testFrom_1_7("start x = NODE(1) match p=x-->z return filter(x in p : x.prop = 123)",
      Query.
        start(NodeById("x", 1)).
        namedPaths(NamedPath("p", RelatedTo("x", "z", "  UNNAMED1", Seq(), Direction.OUTGOING, false, True()))).
        returns(
        ReturnItem(FilterFunction(Entity("p"), "x", Equals(Property("x", "prop"), Literal(123))), "filter(x in p : x.prop = 123)")
      ))
  }

  @Test def collection_literal() {
    testFrom_1_7("start x = NODE(1) return ['a','b','c']",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal("b"), Literal("c")), "['a','b','c']")
      ))
  }

  @Test def collection_literal2() {
    testFrom_1_7("start x = NODE(1) return []",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(), "[]")
      ))
  }

  @Test def collection_literal3() {
    testFrom_1_7("start x = NODE(1) return [1,2,3]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal(1), Literal(2), Literal(3)), "[1,2,3]")
      ))
  }

  @Test def collection_literal4() {
    testFrom_1_7("start x = NODE(1) return ['a',2]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal(2)), "['a',2]")
      ))
  }

  @Test def in_with_collection_literal() {
    testFrom_1_7("start x = NODE(1) where x.prop in ['a','b'] return x",
      Query.
        start(NodeById("x", 1)).
        where(AnyInIterable(Collection(Literal("a"), Literal("b")), "-_-INNER-_-", Equals(Property("x", "prop"), Entity("-_-INNER-_-")))).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_match() {
    testFrom_1_7("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED1", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_varlength_rel() {
    testFrom_1_7("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED1", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_shortest_path() {
    testFrom_1_7("start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo("x", "z", "  UNNAMED1", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false, True())).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def mutliple_relationship_type_in_relationship_predicate() {
    testFrom_1_7(
      """start a=node(0), b=node(1) where a-[:KNOWS|BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Entity("a"), Entity("b"), Direction.BOTH, Seq("KNOWS", "BLOCKS")))
        returns (ReturnItem(Entity("a"), "a")))
  }

  @Test def first_parsed_pipe_query() {
    val secondQ = Query.
      start().
      where(Equals(Property("x", "foo"), Literal(42))).
      returns(ReturnItem(Entity("x"), "x"))

    val q = Query.
      start(NodeById("x", 1)).
      tail(secondQ).
      returns(ReturnItem(Entity("x"), "x"))


    testFrom_1_8("START x = node(1) WITH x WHERE x.foo = 42 RETURN x", q)
  }

  @Test def read_first_and_update_next() {
    val secondQ = Query.
      start(CreateNodeStartItem("b", Map("age" -> Multiply(Property("a", "age"), Literal(2.0))))).
      returns(ReturnItem(Entity("b"), "b"))

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(ReturnItem(Entity("a"), "a"))


    testFrom_1_8("start a = node(1) with a create (b {age : a.age * 2}) return b", q)
  }

  @Test def variable_length_path_with_iterable_name() {
    testAll("start a=node(0) match a -[r?*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), Seq(), Direction.OUTGOING, Some("r"), true, True())).
        returns(ReturnItem(Entity("x"), "x"))
    )
  }

  @Test def binary_precedence() {
    testAll("""start n=node(0) where n.a = 'x' and n.b = 'x' or n.c = 'x' return n""",
      Query.
        start(NodeById("n", 0)).
        where(
        Or(
          And(
            Equals(Property("n", "a"), Literal("x")),
            Equals(Property("n", "b"), Literal("x"))
          ),
          Equals(Property("n", "c"), Literal("x"))
        )
      ).returns(ReturnItem(Entity("n"), "n"))
    )
  }

  @Test def create_node() {
    testFrom_1_8("create a",
      Query.
        start(CreateNodeStartItem("a", Map()))
        returns()
    )
  }

  @Test def create_node_with_a_property() {
    testFrom_1_8("create (a {name : 'Andres'})",
      Query.
        start(CreateNodeStartItem("a", Map("name" -> Literal("Andres"))))
        returns()
    )
  }

  @Test def create_node_with_a_property_and_return_it() {
    testFrom_1_8("create (a {name : 'Andres'}) return a",
      Query.
        start(CreateNodeStartItem("a", Map("name" -> Literal("Andres"))))
        returns (ReturnItem(Entity("a"), "a"))
    )
  }

  @Test def create_two_nodes_with_a_property_and_return_it() {
    testFrom_1_8("create (a {name : 'Andres'}), b return a,b",
      Query.
        start(CreateNodeStartItem("a", Map("name" -> Literal("Andres"))), CreateNodeStartItem("b", Map()))
        returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"))
    )
  }

  @Test def create_node_from_map_expression() {
    testFrom_1_8("create (a {param})",
      Query.
        start(CreateNodeStartItem("a", Map("*" -> ParameterExpression("param"))))
        returns()
    )
  }

  @Test def start_with_two_nodes_and_create_relationship() {
    val secondQ = Query.
      start(CreateRelationshipStartItem("r", Entity("a"), Entity("b"), "REL", Map())).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"))


    testFrom_1_8("start a=node(0), b=node(1) with a,b create a-[r:REL]->b", q)
  }

  @Test def start_with_two_nodes_and_create_relationship_using_alternative_with_syntax() {
    val secondQ = Query.
      start(CreateRelationshipStartItem("r", Entity("a"), Entity("b"), "REL", Map())).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"))


    testFrom_1_8("""
start a=node(0), b=node(1)
========= a,b ============
create a-[r:REL]->b
""", q)
  }

  @Test def create_relationship_with_properties() {
    val secondQ = Query.
      start(CreateRelationshipStartItem("r", Entity("a"), Entity("b"), "REL",
      Map("why" -> Literal(42), "foo" -> Literal("bar"))
    )).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(ReturnItem(Entity("a"), "a"), ReturnItem(Entity("b"), "b"))


    testFrom_1_8("start a=node(0), b=node(1) with a,b create a-[r:REL {why : 42, foo : 'bar'}]->b", q)
  }

  @Test def create_relationship_without_identifier() {
    testFrom_1_8("create {a}-[:REL]->{a}",
      Query.
        start(CreateRelationshipStartItem("  UNNAMED1", ParameterExpression("a"), ParameterExpression("a"), "REL", Map())).
        returns())
  }

  @Test def create_relationship_with_properties_from_map() {
    testFrom_1_8("create {a}-[:REL {param}]->{a}",
      Query.
        start(CreateRelationshipStartItem("  UNNAMED1", ParameterExpression("a"), ParameterExpression("a"), "REL", Map("*" -> ParameterExpression("param")))).
        returns())
  }

  @Test def create_relationship_without_identifier2() {
    testFrom_1_8("create {a}-[:REL]->{a}",
      Query.
        start(CreateRelationshipStartItem("  UNNAMED1", ParameterExpression("a"), ParameterExpression("a"), "REL", Map())).
        returns())
  }

  @Test def delete_node() {
    val secondQ = Query.
      updates(DeleteEntityAction(Entity("a"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Entity("a"), "a"))

    testFrom_1_8("start a=node(0) with a delete a", q)
  }

  @Test def set_property_on_node() {
    val secondQ = Query.
      updates(PropertySetAction(Property("a", "hello"), Literal("world"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Entity("a"), "a"))

    testFrom_1_8("start a=node(0) with a set a.hello = 'world'", q)
  }

  @Test def update_property_with_expression() {
    val secondQ = Query.
      updates(PropertySetAction(Property("a", "salary"), Multiply(Property("a", "salary"), Literal(2.0)))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(ReturnItem(Entity("a"), "a"))

    testFrom_1_8("start a=node(0) with a set a.salary = a.salary * 2 ", q)
  }

  @Test def foreach_on_path() {
    val secondQ = Query.
      updates(ForeachAction(NodesFunction(Entity("p")), "n", Seq(PropertySetAction(Property("n", "touched"), Literal(true))))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      namedPaths(NamedPath("p", RelatedTo("a", "b", "r", "REL", Direction.OUTGOING))).
      tail(secondQ).
      returns(ReturnItem(Entity("p"), "p"))

    testFrom_1_8("start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) : set n.touched = true ) ", q)
  }

  @Test def simple_read_first_and_update_next() {
    val secondQ = Query.
      start(CreateNodeStartItem("b", Map("age" -> Multiply(Property("a", "age"), Literal(2.0))))).
      returns(ReturnItem(Entity("b"), "b"))

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testFrom_1_8("start a = node(1) create (b {age : a.age * 2}) return b", q)
  }

  @Test def simple_start_with_two_nodes_and_create_relationship() {
    val secondQ = Query.
      start(CreateRelationshipStartItem("r", Entity("a"), Entity("b"), "REL", Map())).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testFrom_1_8("start a=node(0), b=node(1) create a-[r:REL]->b", q)
  }

  @Test def simple_create_relationship_with_properties() {
    val secondQ = Query.
      start(CreateRelationshipStartItem("r", Entity("b"), Entity("a"), "REL",
      Map("why" -> Literal(42), "foo" -> Literal("bar"))
    )).
      returns()

    val q = Query.
      start(NodeById("a", 0), NodeById("b", 1)).
      tail(secondQ).
      returns(AllIdentifiers())


    testFrom_1_8("start a=node(0), b=node(1) create a<-[r:REL {why : 42, foo : 'bar'}]-b", q)
  }

  @Test def simple_delete_node() {
    val secondQ = Query.
      updates(DeleteEntityAction(Entity("a"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) delete a", q)
  }

  @Test def simple_set_property_on_node() {
    val secondQ = Query.
      updates(PropertySetAction(Property("a", "hello"), Literal("world"))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) set a.hello = 'world'", q)
  }

  @Test def simple_update_property_with_expression() {
    val secondQ = Query.
      updates(PropertySetAction(Property("a", "salary"), Multiply(Property("a", "salary"), Literal(2.0)))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
      tail(secondQ).
      returns(AllIdentifiers())

    testFrom_1_8("start a=node(0) set a.salary = a.salary * 2 ", q)
  }

  @Test def simple_foreach_on_path() {
    val secondQ = Query.
      updates(ForeachAction(NodesFunction(Entity("p")), "n", Seq(PropertySetAction(Property("n", "touched"), Literal(true))))).
      returns()

    val q = Query.
      start(NodeById("a", 0)).
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

  @Test def single_relate() {
    val secondQ = Query.
      relate(RelateLink("a", "b", "  UNNAMED1", "reltype", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1), b=node(2) relate a-[:reltype]->b", q)
  }

  @Test def single_relate_with_rel() {
    val secondQ = Query.
      relate(RelateLink("a", "b", "r", "reltype", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1), b=node(2) relate a-[r:reltype]->b", q)
  }

  @Test def single_relate_with_empty_parenthesis() {
    val secondQ = Query.
      relate(RelateLink("a", "  UNNAMED1", "  UNNAMED2", "reltype", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1), b=node(2) relate a-[:reltype]->()", q)
  }

  @Test def two_relates() {
    val secondQ = Query.
      relate(
      RelateLink("a", "b", "  UNNAMED1", "X", Direction.OUTGOING),
      RelateLink("b", "c", "  UNNAMED2", "X", Direction.INCOMING)).
      returns()

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1) relate a-[:X]->b<-[:X]-c", q)
  }

  @Test def relate_with_initial_values_for_node() {
    val secondQ = Query.
      relate(
      RelateLink(new NamedExpectation("a"), NamedExpectation("b", Map[String, Expression]("name" -> Literal("Andres"))), new NamedExpectation("  UNNAMED1"), "X", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1) relate a-[:X]->(b {name:'Andres'})", q)
  }

  @Test def relate_with_initial_values_for_rel() {
    val secondQ = Query.
      relate(
      RelateLink(new NamedExpectation("a"), new NamedExpectation("b"), NamedExpectation("  UNNAMED1", Map[String, Expression]("name" -> Literal("Andres"))), "X", Direction.OUTGOING)).
      returns()

    val q = Query.
      start(NodeById("a", 1)).
      tail(secondQ).
      returns(AllIdentifiers())
    testFrom_1_8("start a = node(1) relate a-[:X {name:'Andres'}]->b", q)
  }

  @Test def foreach_with_literal_collection() {

    val q2 = Query.updates(
      ForeachAction(Collection(Literal(1.0), Literal(2.0), Literal(3.0)), "x", Seq(CreateNodeStartItem("a", Map("number" -> Entity("x")))))
    ).returns()

    testFrom_1_8(
      "create root foreach(x in [1,2,3] : create (a {number:x}))",
      Query.
        start(CreateNodeStartItem("root", Map.empty)).
        tail(q2).
        returns(AllIdentifiers())
    )
  }

  @Test def string_literals_should_not_be_mistaken_for_identifiers() {
    testFrom_1_8(
      "create (tag1 {name:'tag2'}), (tag2 {name:'tag1'})",
      Query.
        start(
        CreateNodeStartItem("tag1", Map("name"->Literal("tag2"))),
        CreateNodeStartItem("tag2", Map("name"->Literal("tag1")))
      ).returns()
    )
  }

  @Test def relate_with_two_rels_to_same_node() {
    val returns = Query.
      updates(RelateAction(
      RelateLink("x", "root", "r1", "X", Direction.INCOMING),
      RelateLink("root", "x", "r2", "Y", Direction.OUTGOING)))
      .returns(ReturnItem(Entity("x"), "x"))

    val q = Query.start(NodeById("root", 0)).tail(returns).returns(AllIdentifiers())

    testFrom_1_8(
      "start root=node(0) relate x<-[r1:X]-root-[r2:Y]->x return x",
      q
    )
  }


  def test_1_8(query: String, expectedQuery: Query) {
    testQuery(None, query, expectedQuery)
    testQuery(None, query + ";", expectedQuery)
  }

  def test_1_6(query: String, expectedQuery: Query) {
    testQuery(Some("1.6"), query, expectedQuery)
  }

  def test_1_7(query: String, expectedQuery: Query) {
    testQuery(Some("1.7"), query, expectedQuery)
  }

  def testFrom_1_7(query: String, expectedQuery: Query) {
    test_1_7(query, expectedQuery)
    test_1_8(query, expectedQuery)
  }

  def testFrom_1_8(query: String, expectedQuery: Query) {
    test_1_8(query, expectedQuery)
  }

  def testAll(query: String, expectedQuery: Query) {
    test_1_6(query, expectedQuery)
    test_1_7(query, expectedQuery)
    test_1_8(query, expectedQuery)
  }

  def testOlderParsers(queryText: String, queryAst: Query) {
    test_1_6(queryText, queryAst)
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
