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
package org.neo4j.cypher.internal.parser.v1_9

import org.junit.Assert._
import org.neo4j.graphdb.Direction
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers.equalTo
import org.neo4j.cypher.internal.parser.{ParsedEntity, ParsedRelation}
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions._
import org.neo4j.cypher.internal.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.mutation._

class CypherParserTest extends JUnitSuite with Assertions {
  @Test def shouldParseEasiestPossibleQuery() {
    test(
      "start s = NODE(1) return s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def should_return_string_literal() {
    test(
      "start s = node(1) return \"apa\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("apa"), "\"apa\""))
    )
  }

  @Test def should_return_string_literal_with_escaped_sequence_in() {
    test(
      "start s = node(1) return \"a\\tp\\\"a\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a\tp\"a"), "\"a\\tp\\\"a\""))
    )
  }

  @Test def allTheNodes() {
    test(
      "start s = NODE(*) return s",
      Query.
        start(AllNodes("s")).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def allTheRels() {
    test(
      "start r = relationship(*) return r",
      Query.
        start(AllRelationships("r")).
        returns(ReturnItem(Identifier("r"), "r"))
    )
  }

  @Test def shouldHandleAliasingOfColumnNames() {
    test(
      "start s = NODE(1) return s as somethingElse",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "somethingElse", true))
    )
  }

  @Test def sourceIsAnIndex() {
    test(
      """start a = node:index(key = "value") return a""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def sourceIsAnNonParsedIndexQuery() {
    test(
      """start a = node:index("key:value") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    test(
      "start s = relationship(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def shouldParseEasiestPossibleRelationshipQueryShort() {
    test(
      "start s = rel(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def sourceIsARelationshipIndex() {
    test(
      """start a = rel:index(key = "value") return a""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def keywordsShouldBeCaseInsensitive() {
    test(
      "START s = NODE(1) RETURN s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def shouldParseMultipleNodes() {
    test(
      "start s = NODE(1,2,3) return s",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def shouldParseMultipleInputs() {
    test(
      "start a = node(1), b = NODE(2) return a,b",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def shouldFilterOnProp() {
    test(
      "start a = NODE(1) where a.name = \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldReturnLiterals() {
    test(
      "start a = NODE(1) return 12",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Literal(12), "12"))
    )
  }

  @Test def shouldReturnAdditions() {
    test(
      "start a = NODE(1) return 12+2",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Add(Literal(12), Literal(2)), "12+2"))
    )
  }

  @Test def arithmeticsPrecedence() {
    test(
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
        , "12/4*3-2*4"))
    )
  }

  @Test def shouldFilterOnPropWithDecimals() {
    test(
      "start a = node(1) where a.extractReturnItems = 3.1415 return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), PropertyKey("extractReturnItems")), Literal(3.1415))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleNot() {
    test(
      "start a = node(1) where not(a.name = \"andres\") return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleNotEqualTo() {
    test(
      "start a = node(1) where a.name <> \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleLessThan() {
    test(
      "start a = node(1) where a.name < \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleGreaterThan() {
    test(
      "start a = node(1) where a.name > \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleLessThanOrEqual() {
    test(
      "start a = node(1) where a.name <= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }


  @Test def shouldHandleRegularComparison() {
    test(
      "start a = node(1) where \"Andres\" =~ 'And.*' return a",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }


  @Test def shouldHandleMultipleRegularComparison() {
    test(
      """start a = node(1) where a.name =~ 'And.*' AnD a.name =~ 'And.*' return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(LiteralRegularExpression(Property(Identifier("a"), PropertyKey("name")), Literal("And.*")), LiteralRegularExpression(Property(Identifier("a"), PropertyKey("name")), Literal("And.*")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleEscapedRegexs() {
    test(
      """start a = node(1) where a.name =~ 'And\\/.*' return a""",
      Query.
        start(NodeById("a", 1)).
        where(LiteralRegularExpression(Property(Identifier("a"), PropertyKey("name")), Literal("And\\/.*"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleGreaterThanOrEqual() {
    test(
      "start a = node(1) where a.name >= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThanOrEqual(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def booleanLiteralsOld() {
    test(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(true), Literal(false))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldFilterOnNumericProp() {
    test(
      "start a = NODE(1) where 35 = a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), Property(Identifier("a"), PropertyKey("age")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }


  @Test def shouldHandleNegativeLiteralsAsExpected() {
    test(
      "start a = NODE(1) where -35 = a.age AND a.age > -1.2 return a",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Literal(-35), Property(Identifier("a"), PropertyKey("age"))),
        GreaterThan(Property(Identifier("a"), PropertyKey("age")), Literal(-1.2)))
      ).returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldCreateNotEqualsQuery() {
    test(
      "start a = NODE(1) where 35 <> a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), Property(Identifier("a"), PropertyKey("age"))))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def multipleFilters() {
    test(
      "start a = NODE(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("mattias")))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def relatedTo() {
    test(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def relatedToWithoutRelType() {
    test(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    test(
      "start a = NODE(1) match a-[r]->b return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("r"), "r"))
    )
  }

  @Test def relatedToTheOtherWay() {
    test(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", "  UNNAMED3", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def twoDoubleOptionalWithFourHalfs1_9() {
    test(
      "START a=node(1), b=node(2) MATCH a-[r1?]->X<-[r2?]-b, a<-[r3?]-Z-[r4?]->b return r1,r2,r3,r4 order by id(r1),id(r2),id(r3),id(r4)",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        matches(
            RelatedTo.optional("a", "X", "r1", Seq(), Direction.OUTGOING),
            RelatedTo.optional("b", "X", "r2", Seq(), Direction.OUTGOING),
            RelatedTo.optional("Z", "a", "r3", Seq(), Direction.OUTGOING),
            RelatedTo.optional("Z", "b", "r4", Seq(), Direction.OUTGOING)
        ).orderBy(
            SortItem(IdFunction(Identifier("r1")), true),
            SortItem(IdFunction(Identifier("r2")), true),
            SortItem(IdFunction(Identifier("r3")), true),
            SortItem(IdFunction(Identifier("r4")), true)
        ).returns(
            ReturnItem(Identifier("r1"), "r1"),
            ReturnItem(Identifier("r2"), "r2"),
            ReturnItem(Identifier("r3"), "r3"),
            ReturnItem(Identifier("r4"), "r4")
        )
    )
  }

  @Test def shouldOutputVariables() {
    test(
      "start a = NODE(1) return a.name",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "a.name"))
    )
  }

  @Test def shouldHandleAndPredicates() {
    test(
      "start a = NODE(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("lastname")), Literal("taylor")))).
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "a.name"))
    )
  }

  @Test def relatedToWithRelationOutput() {
    test(
      "start a = NODE(1) match a -[rel:KNOWS]-> (b) return rel",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("rel"), "rel"))
    )
  }


  @Test def relatedToWithoutEndName() {
    test(
      "start a = NODE(1) match a -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED3", "r", Seq("MARRIED"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def relatedInTwoSteps() {
    test(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED5", Seq("KNOWS"), Direction.OUTGOING),
        RelatedTo("b", "c", "  UNNAMED6", Seq("FRIEND"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("c"), "c"))
    )
  }

  @Test def djangoRelationshipType() {
    test(
      "start a = NODE(1) match a -[r:`<<KNOWS>>`]-> b return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("<<KNOWS>>"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def countTheNumberOfHitsOld() {
    test(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING)).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(CountStar(), "count(*)"))
    )
  }

  @Test def countStar() {
    test(
      "start a = NODE(1) return count(*) order by count(*)",
      Query.
        start(NodeById("a", 1)).
        aggregation(CountStar()).
        columns("count(*)").
        orderBy(SortItem(CountStar(), true)).
        returns(ReturnItem(CountStar(), "count(*)"))
    )
  }

  @Test def distinct() {
    test(
      "start a = NODE(1) match a -[r]-> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        aggregation().
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def sumTheAgesOfPeople() {
    test(
      "start a = NODE(1) match a -[r]-> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        aggregation(Sum(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "sum(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Sum(Property(Identifier("a"), PropertyKey("age"))), "sum(a.age)"))
    )
  }

  @Test def avgTheAgesOfPeopleOld() {
    test(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING)).
        aggregation(Avg(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Avg(Property(Identifier("a"), PropertyKey("age"))), "avg(a.age)"))
    )
  }

  @Test def minTheAgesOfPeople() {
    test(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING)).
        aggregation(Min(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "min(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Min(Property(Identifier("a"), PropertyKey("age"))), "min(a.age)"))
    )
  }

  @Test def maxTheAgesOfPeople() {
    test(
      "start a = NODE(1) match a --> b return a, b, max(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING)).
        aggregation(Max((Property(Identifier("a"), PropertyKey("age"))))).
        columns("a", "b", "max(a.age)").
        returns(
        ReturnItem(Identifier("a"), "a"),
        ReturnItem(Identifier("b"), "b"),
        ReturnItem(Max((Property(Identifier("a"), PropertyKey("age")))), "max(a.age)"))
    )
  }

  @Test def singleColumnSorting() {
    test(
      "start a = NODE(1) return a order by a.name",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def sortOnAggregatedColumn() {
    test(
      "start a = NODE(1) return a order by avg(a.name)",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Avg(Property(Identifier("a"), PropertyKey("name"))), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def sortOnAliasedAggregatedColumn() {
    test(
      "start n = node(0) match (n)-[r:KNOWS]-(c) return n, count(c) as cnt order by cnt",
      Query.
        start(NodeById("n", 0)).
        matches(RelatedTo("c", "n", "r", Seq("KNOWS"), Direction.BOTH)).
        orderBy(SortItem(Count(Identifier("c")), true)).
        aggregation(Count(Identifier("c"))).
        returns(ReturnItem(Identifier("n"), "n"), ReturnItem(Count(Identifier("c")), "cnt", true)))
  }

  @Test def shouldHandleTwoSortColumns() {
    test(
      "start a = NODE(1) return a order by a.name, a.age",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = true),
        SortItem(Property(Identifier("a"), PropertyKey("age")), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleTwoSortColumnsAscending() {
    test(
      "start a = NODE(1) return a order by a.name ASCENDING, a.age ASC",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = true),
        SortItem(Property(Identifier("a"), PropertyKey("age")), ascending = true)).
        returns(ReturnItem(Identifier("a"), "a")))

  }

  @Test def orderByDescending() {
    test(
      "start a = NODE(1) return a order by a.name DESCENDING",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = false)).
        returns(ReturnItem(Identifier("a"), "a")))

  }

  @Test def orderByDesc() {
    test(
      "start a = NODE(1) return a order by a.name desc",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(Property(Identifier("a"), PropertyKey("name")), ascending = false)).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def nullableProperty() {
    test(
      "start a = NODE(1) return a.name?",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Nullable(Property(Identifier("a"), PropertyKey("name"))), "a.name?"))
    )
  }

  @Test def nestedBooleanOperatorsAndParentesis() {
    test(
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

  @Test def limit5() {
    test(
      "start n=NODE(1) return n limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test def skip5() {
    test(
      "start n=NODE(1) return n skip 5",
      Query.
        start(NodeById("n", 1)).
        skip(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test def skip5limit5() {
    test(
      "start n=NODE(1) return n skip 5 limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        skip(5).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test def relationshipType() {
    test(
      "start n=NODE(1) match n-[r]->(x) where type(r) = \"something\" return r",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING)).
        where(Equals(RelationshipTypeFunction(Identifier("r")), Literal("something"))).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def pathLength() {
    test(
      "start n=NODE(1) match p=(n-[r]->x) where LENGTH(p) = 10 return p",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "n", "x", Seq.empty, Direction.OUTGOING))).
        where(Equals(LengthFunction(Identifier("p")), Literal(10.0))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def relationshipTypeOut() {
    test(
      "start n=NODE(1) match n-[r]->(x) return TYPE(r)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(RelationshipTypeFunction(Identifier("r")), "TYPE(r)")))
  }


  @Test def shouldBeAbleToParseCoalesce() {
    test(
      "start n=NODE(1) match n-[r]->(x) return COALESCE(r.name,x.name)",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(CoalesceFunction(Property(Identifier("r"), PropertyKey("name")), Property(Identifier("x"), PropertyKey("name"))), "COALESCE(r.name,x.name)")))
  }

  @Test def relationshipsFromPathOutput() {
    test(
      "start n=NODE(1) match p=n-[r]->x return RELATIONSHIPS(p)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "n", "x", Seq.empty, Direction.OUTGOING))).
        returns(ReturnItem(RelationshipFunction(Identifier("p")), "RELATIONSHIPS(p)")))
  }

  @Test def makeDirectionOutgoing() {
    test(
      "START a=node(1) match b<-[r]-a return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def keepDirectionForNamedPaths() {
    test(
      "START a=node(1) match p=b<-[r]-a return p",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", "r", Seq(), Direction.INCOMING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "b", "a", Seq(), Direction.INCOMING))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def relationshipsFromPathInWhere() {
    test(
      "start n=NODE(1) match p=n-[r]->x where length(rels(p))=1 return p",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", Seq(), Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "n", "x", Seq.empty, Direction.OUTGOING))).
        where(Equals(LengthFunction(RelationshipFunction(Identifier("p"))), Literal(1))).
        returns (ReturnItem(Identifier("p"), "p")))
  }

  @Test def countNonNullValues() {
    test(
      "start a = NODE(1) return a, count(a)",
      Query.
        start(NodeById("a", 1)).
        aggregation(Count(Identifier("a"))).
        columns("a", "count(a)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Count(Identifier("a")), "count(a)")))
  }

  @Test def shouldHandleIdBothInReturnAndWhere() {
    test(
      "start a = NODE(1) where id(a) = 0 return ID(a)",
      Query.
        start(NodeById("a", 1)).
        where(Equals(IdFunction(Identifier("a")), Literal(0)))
        returns (ReturnItem(IdFunction(Identifier("a")), "ID(a)")))
  }

  @Test def shouldBeAbleToHandleStringLiteralsWithApostrophe() {
    test(
      "start a = node:index(key = 'value') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleQuotationsInsideApostrophes() {
    test(
      "start a = node:index(key = 'val\"ue') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("val\"ue"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def simplePathExample() {
    test(
      "start a = node(0) match p = a-->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "  UNNAMED3", Seq(), Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("  UNNAMED3", "a", "b", Seq.empty, Direction.OUTGOING))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def threeStepsPath() {
    test(
      "start a = node(0) match p = ( a-[r1]->b-[r2]->c ) return a",
      Query.
        start(NodeById("a", 0)).
        matches(
          RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING),
          RelatedTo("b", "c", "r2", Seq(), Direction.OUTGOING)).
        namedPaths(NamedPath("p",
          ParsedRelation("r1", "a", "b", Seq.empty, Direction.OUTGOING),
          ParsedRelation("r2", "b", "c", Seq.empty, Direction.OUTGOING))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def pathsShouldBePossibleWithoutParenthesis() {
    test(
      "start a = node(0) match p = a-[r]->b return a",
      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq.empty, Direction.OUTGOING))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def variableLengthPath() {
    test(
      "start a=node(0) match a -[:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelsIterable() {
    test(
      "start a=node(0) match a -[r:knows*1..3]-> x return length(r)",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING, Some("r"))).
        returns(ReturnItem(LengthFunction(Identifier("r")), "length(r)"))
    )
  }

  @Test def fixedVarLengthPath() {
    test(
      "start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", SingleNode("a"), SingleNode("x"), Some(3), Some(3), Seq(),
        Direction.OUTGOING, None, optional = false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMinDepth() {
    test(
      "start a=node(0) match a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    test(
      "start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(2), None, "knows", Direction.OUTGOING, Some("r"))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    test(
      "start a=node(0) match a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def unboundVariableLengthPath_Old() {
    test(
      "start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def optionalRelationship1_9() {
    test(
      "start a = node(1) match a -[?]-> (b) return b",
      Query.
      start(NodeById("a", 1)).
      matches(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED3", Seq(), Direction.OUTGOING, true)).
      returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def questionMarkOperator() {
    test(
      "start a = node(1) where a.prop? = 42 return a",
      Query.
        start(NodeById("a", 1)).
        where(NullablePredicate(Equals(Nullable(Property(Identifier("a"), PropertyKey("prop"))), Literal(42.0)), Seq((Nullable(Property(Identifier("a"), PropertyKey("prop"))), true)))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def exclamationMarkOperator() {
    test(
      "start a = node(1) where a.prop! = 42 return a",
      Query.
        start(NodeById("a", 1)).
        where(NullablePredicate(Equals(Nullable(Property(Identifier("a"), PropertyKey("prop"))), Literal(42)), Seq((Nullable(Property(Identifier("a"), PropertyKey("prop"))), false)))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def optionalTypedRelationship1_9() {
    test(
      "start a = node(1) match a -[?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo.optional("a", "b", "  UNNAMED3", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def optionalTypedAndNamedRelationship1_9() {
    test(
      "start a = node(1) match a -[r?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo.optional("a", "b", "r", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def optionalNamedRelationship1_9() {
    test(
      "start a = node(1) match a -[r?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo.optional("a", "b", "r", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def testAllIterablePredicate() {
    test(
      """start a = node(1) match p=(a-[r]->b) where all(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), Direction.OUTGOING))).
        where(AllInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def testAnyIterablePredicate() {
    test(
      """start a = node(1) match p=(a-[r]->b) where any(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(SingleInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), Direction.OUTGOING))).
        where(AnyInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def testNoneIterablePredicate() {
    test(
      """start a = node(1) match p=(a-[r]->b) where none(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), Direction.OUTGOING))).
        where(NoneInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def testSingleIterablePredicate() {
    test(
      """start a = node(1) match p=(a-[r]->b) where single(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), Direction.OUTGOING))).
        where(SingleInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def testParamAsStartNode() {
    test(
      """start pA = node({a}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamAsStartRel() {
    test(
      """start pA = relationship({a}) return pA""",
      Query.
        start(RelationshipById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testNumericParamNameAsStartNode() {
    test(
      """start pA = node({0}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("0"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForWhereLiteral() {
    test(
      """start pA = node(1) where pA.name = {name} return pA""",
      Query.
        start(NodeById("pA", 1)).
        where(Equals(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("name")))
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForIndexKey() {
    test(
      """start pA = node:idx({key} = "Value") return pA""",
      Query.
        start(NodeByIndex("pA", "idx", ParameterExpression("key"), Literal("Value"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForIndexValue() {
    test(
      """start pA = node:idx(key = {Value}) return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), ParameterExpression("Value"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForIndexQuery() {
    test(
      """start pA = node:idx({query}) return pA""",
      Query.
        start(NodeByIndexQuery("pA", "idx", ParameterExpression("query"))).
        returns(ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForSkip() {
    test(
      """start pA = node(0) return pA skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForLimit() {
    test(
      """start pA = node(0) return pA limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForLimitAndSkip() {
    test(
      """start pA = node(0) return pA skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ("stop")
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testQuotedParams() {
    test(
      """start pA = node({`id`}) where pA.name =~ {`regex`} return pA skip {`ski``pper`} limit {`stop`}""",
      Query.
        start(NodeById("pA", ParameterExpression("id"))).
        where(RegularExpression(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("regex")))
        skip("ski`pper")
        limit ("stop")
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testParamForRegex() {
    test(
      """start pA = node(0) where pA.name =~ {regex} return pA""",
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("regex")))
        returns (ReturnItem(Identifier("pA"), "pA")))
  }

  @Test def testShortestPathWithMaxDepth() {
    test(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def testShortestPathWithType() {
    test(
      """start a=node(0), b=node(1) match p = shortestPath( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def testAllShortestPathsWithType() {
    test(
      """start a=node(0), b=node(1) match p = allShortestPaths( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), Direction.OUTGOING, Some(6), false, false, None)).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def testForNull() {
    test(
      """start a=node(0) where a is null return a""",
      Query.
        start(NodeById("a", 0)).
        where(IsNull(Identifier("a")))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def testForNotNull() {
    test(
      """start a=node(0) where a is not null return a""",
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(Identifier("a"))))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def testCountDistinct() {
    test(
      """start a=node(0) return count(distinct a)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(Distinct(Count(Identifier("a")), Identifier("a"))).
        columns("count(distinct a)")
        returns (ReturnItem(Distinct(Count(Identifier("a")), Identifier("a")), "count(distinct a)")))
  }


  @Test def supportedHasRelationshipInTheWhereClause() {
    test(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED39", Seq(), Direction.OUTGOING, optional = false))))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def supportedNotHasRelationshipInTheWhereClause() {
    test(
      """start a=node(0), b=node(1) where not(a-->()) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(Not(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED143"), "  UNNAMED144", Seq(), Direction.OUTGOING, optional = false)))))).
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleLFAsWhiteSpace() {
    test(
      "start\na=node(0)\nwhere\na.prop=12\nreturn\na",
      Query.
        start(NodeById("a", 0)).
        where(Equals(Property(Identifier("a"), PropertyKey("prop")), Literal(12)))
        returns (ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleUpperCaseDistinct() {
    test(
      "start s = NODE(1) return DISTINCT s",
      Query.
        start(NodeById("s", 1)).
        aggregation().
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseMathFunctions() {
    test(
      "start s = NODE(0) return 5 % 4, abs(-1), round(3.1415), 2 ^ 8, sqrt(16), sign(1)",
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
    test(
      "start s = NODE(1) return s // COMMENT",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldAllowCommentAlone() {
    test(
      """start s = NODE(1) return s
    // COMMENT""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldAllowCommentsInsideStrings() {
    test(
      "start s = NODE(1) where s.apa = '//NOT A COMMENT' return s",
      Query.
        start(NodeById("s", 1)).
        where(Equals(Property(Identifier("s"), PropertyKey("apa")), Literal("//NOT A COMMENT")))
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldHandleCommentsFollowedByWhiteSpace() {
    test(
      """start s = NODE(1)
    //I can haz more comment?
    return s""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def first_last_and_rest() {
    test(
      "start x = NODE(1) match p=x-[r]->z return head(nodes(p)), last(nodes(p)), tail(nodes(p))",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq.empty, Direction.OUTGOING, optional = false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, Direction.OUTGOING))).
        returns(
        ReturnItem(HeadFunction(NodesFunction(Identifier("p"))), "head(nodes(p))"),
        ReturnItem(LastFunction(NodesFunction(Identifier("p"))), "last(nodes(p))"),
        ReturnItem(TailFunction(NodesFunction(Identifier("p"))), "tail(nodes(p))")
      ))
  }

  @Test def filter() {
    test(
      "start x = NODE(1) match p=x-[r]->z return filter(x in p WHERE x.prop = 123)",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq.empty, Direction.OUTGOING, optional = false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, Direction.OUTGOING))).
        returns(
        ReturnItem(FilterFunction(Identifier("p"), "x", Equals(Property(Identifier("x"), PropertyKey("prop")), Literal(123))), "filter(x in p WHERE x.prop = 123)"))
    )
  }

  @Test def filterWithColon() {
    test(
      "start x = NODE(1) match p=x-[r]->z return filter(x in p : x.prop = 123)",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq.empty, Direction.OUTGOING, optional = false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, Direction.OUTGOING))).
        returns(
        ReturnItem(FilterFunction(Identifier("p"), "x", Equals(Property(Identifier("x"), PropertyKey("prop")), Literal(123))), "filter(x in p : x.prop = 123)"))
    )
  }

  @Test def collection_literal() {
    test(
      "start x = NODE(1) return ['a','b','c']",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal("b"), Literal("c")), "['a','b','c']"))
    )
  }

  @Test def collection_literal2() {
    test(
      "start x = NODE(1) return []",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(), "[]"))
    )
  }

  @Test def collection_literal3() {
    test(
      "start x = NODE(1) return [1,2,3]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal(1), Literal(2), Literal(3)), "[1,2,3]"))
    )
  }

  @Test def collection_literal4() {
    test(
      "start x = NODE(1) return ['a',2]",
      Query.
        start(NodeById("x", 1)).
        returns(ReturnItem(Collection(Literal("a"), Literal(2)), "['a',2]"))
    )
  }

  @Test def in_with_collection_literal() {
    // we internally use -_-INNER-_- as symbol name for these comprehensions
    test(
      "start x = NODE(1) where x.prop in ['a','b'] return x",
      Query.
        start(NodeById("x", 1)).
        where(AnyInCollection(Collection(Literal("a"), Literal("b")), "-_-INNER-_-", Equals(Property(Identifier("x"), PropertyKey("prop")), Identifier("-_-INNER-_-")))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_matchOld() {
    test(
      "start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_varlength_relOld() {
    test(
      "start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_shortest_pathOld() {
    test(
      "start x = NODE(1) match x-[:REL1|REL2|REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED3", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_relationship_predicate_back_in_the_day() {
    test(
      """start a=node(0), b=node(1) where a-[:KNOWS|BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED39", Seq("KNOWS","BLOCKS"), Direction.BOTH, optional = false)))))
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def first_parsed_pipe_query() {
    test(
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

  @Test def read_first_and_update_next() {
    test(
      "start a = node(1) with a create (b {age : a.age * 2}) return b", {
      val secondQ = Query.
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), PropertyKey("age")), Literal(2.0))), Seq.empty, true))).
        returns(ReturnItem(Identifier("b"), "b"))

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  @Test def variable_length_path_with_collection_for_relationships1_9() {
    test(
      "start a=node(0) match a -[r?*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED3", SingleNode("a"), SingleNode("x"), Some(1), Some(3), Seq(), Direction.OUTGOING, Some("r"), optional = true)).
        returns(ReturnItem(Identifier("x"), "x")))
  }

  @Test def create_node() {
    test(
      "create a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), Seq.empty))).
        returns()
    )
  }

  @Test def create_node_with_a_property() {
    test(
      "create (a {name : 'Andres'})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, true))).
        returns()
    )
  }

  @Test def create_node_with_a_property2O() {
    test(
      "create a={name : 'Andres'}",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, true))).
        returns()
    )
  }

  @Test def create_node_with_a_property_and_return_it() {
    test(
      "create (a {name : 'Andres'}) return a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, true))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def create_node_from_map_expressionOld() {
    test(
      "create (a {param})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("*" -> ParameterExpression("param")), Seq.empty))).
        returns()
    )
  }

  @Test def start_with_two_nodes_and_create_relationship() {
    test(
      "start a=node(0), b=node(1) with a,b create a-[r:REL]->b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
          RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("b"),Map(), Seq.empty, true), "REL", Map()))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    })
  }

  @Test def start_with_two_nodes_and_create_relationship_make_outgoing() {
    test(
      "start a=node(0), b=node(1) create a<-[r:REL]-b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("a"),Map(), Seq.empty, true), "REL", Map()))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def start_with_two_nodes_and_create_relationship_make_outgoing_named() {
    test(
      "start a=node(0), b=node(1) create p=a<-[r:REL]-b return p", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("a"),Map(), Seq.empty, true), "REL", Map()))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.INCOMING))).
        returns(ReturnItem(Identifier("p"), "p"))

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def create_relationship_with_properties() {
    test(
      "start a=node(0), b=node(1) with a,b create a-[r:REL {why : 42, foo : 'bar'}]->b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"),Map(),Seq.empty, true),
        RelationshipEndpoint(Identifier("b"),Map(),Seq.empty, true), "REL", Map("why" -> Literal(42), "foo" -> Literal("bar"))))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    })
  }

  @Test def create_relationship_without_identifierOld() {
    test(
      "create (a {a})-[:REL]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED1",
          RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")),Seq.empty, true),
          RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")),Seq.empty, true),
          "REL", Map()))).
        returns()
    )
  }

  @Test def create_relationship_with_properties_from_map_old() {
    test(
      "create (a {a})-[:REL {param}]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED1",
          RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")),Seq.empty, true),
          RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")),Seq.empty, true),
          "REL", Map("*" -> ParameterExpression("param"))))).
        returns()
    )
  }

  @Test def create_relationship_without_identifier2Old() {
    test(
      "create (a {a})-[:REL]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED1",
          RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")), Seq.empty, true),
          RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")), Seq.empty, true),
          "REL", Map()))).
        returns()
    )
  }

  @Test def delete_node() {
    test(
      "start a=node(0) with a delete a", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("a"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  @Test def simple_delete_node() {
    test(
      "start a=node(0) delete a", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("a"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def delete_rel() {
    test(
      "start a=node(0) match (a)-[r:REL]->(b) delete r", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("r"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def delete_path() {
    test(
      "start a=node(0) match p=(a)-[r:REL]->(b) delete p", {
      val secondQ = Query.
        updates(DeleteEntityAction(Identifier("p"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING))).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def set_property_on_node() {
    test(
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

  @Test def set_multiple_properties_on_node() {
    test(
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

  @Test def update_property_with_expression() {
    test(
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

  @Test def delete_property_old() {
    test(
      "start a=node(0) delete a.salary", {
      val secondQ = Query.
        updates(DeletePropertyAction(Identifier("a"), PropertyKey("salary"))).
        returns()

      Query.
        start(NodeById("a", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def foreach_on_pathOld() {
    test(
      "start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) | set n.touched = true ) ", {
      val secondQ = Query.
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), PropertyKey("touched")), Literal(true))))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING))).
        tail(secondQ).
        returns(ReturnItem(Identifier("p"), "p"))
    })
  }

  @Test def foreach_on_pathOld_with_colon() {
    test(
      "start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) : set n.touched = true ) ", {
      val secondQ = Query.
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), PropertyKey("touched")), Literal(true))))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING))).
        tail(secondQ).
        returns(ReturnItem(Identifier("p"), "p"))
    })
  }

  @Test def simple_read_first_and_update_next() {
    test(
      "start a = node(1) create (b {age : a.age * 2}) return b", {
      val secondQ = Query.
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), PropertyKey("age")), Literal(2.0))), Seq.empty, true))).
        returns(ReturnItem(Identifier("b"), "b"))

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def simple_start_with_two_nodes_and_create_relationship() {
    test(
      "start a=node(0), b=node(1) create a-[r:REL]->b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
          RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, bare = true),
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, bare = true), "REL", Map()))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def simple_create_relationship_with_properties() {
    test(
      "start a=node(0), b=node(1) create a<-[r:REL {why : 42, foo : 'bar'}]-b", {
      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true), "REL",
        Map("why" -> Literal(42), "foo" -> Literal("bar"))
      ))).
        returns()

      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def simple_set_property_on_node() {
    test(
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

  @Test def simple_update_property_with_expression() {
    test(
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

  @Test def simple_foreach_on_path() {
    test(
      "start a=node(0) match p = a-[r:REL]->b foreach(n in nodes(p) | set n.touched = true ) ", {
      val secondQ = Query.
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), PropertyKey("touched")), Literal(true))))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING))).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def returnAll() {
    test(
      "start s = NODE(1) return *",
      Query.
        start(NodeById("s", 1)).
        returns(AllIdentifiers())
    )
  }

  @Test def single_create_unique() {
    test(
      "start a = node(1), b=node(2) create unique a-[:reltype]->b", {
      val secondQ = Query.
        unique(UniqueLink("a", "b", "  UNNAMED1", "reltype", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def single_create_unique_with_rel() {
    test(
      "start a = node(1), b=node(2) create unique a-[r:reltype]->b", {
      val secondQ = Query.
        unique(UniqueLink("a", "b", "r", "reltype", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def single_relate_with_empty_parenthesis() {
    test(
      "start a = node(1), b=node(2) create unique a-[:reltype]->()", {
      val secondQ = Query.
        unique(UniqueLink("a", "  UNNAMED1", "  UNNAMED2", "reltype", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def create_unique_with_two_patterns() {
    test(
      "start a = node(1) create unique a-[:X]->b<-[:X]-c", {
      val secondQ = Query.
        unique(
        UniqueLink("a", "b", "  UNNAMED1", "X", Direction.OUTGOING),
        UniqueLink("c", "b", "  UNNAMED2", "X", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def relate_with_initial_values_for_node() {
    test(
      "start a = node(1) create unique a-[:X]->(b {name:'Andres'})", {
      val secondQ = Query.
        unique(
        UniqueLink(
          NamedExpectation("a", bare = true),
          NamedExpectation("b", Map[String, Expression]("name" -> Literal("Andres")), bare = false),
          NamedExpectation("  UNNAMED1", bare = true), "X", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def create_unique_with_initial_values_for_rel() {
    test(
      "start a = node(1) create unique a-[:X {name:'Andres'}]->b", {
      val secondQ = Query.
        unique(
        UniqueLink(
          NamedExpectation("a", bare = true),
          NamedExpectation("b", bare = true),
          NamedExpectation("  UNNAMED1", Map[String, Expression]("name" -> Literal("Andres")), bare = false), "X", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def foreach_with_literal_collectionOld() {
    test(
      "create root foreach(x in [1,2,3] | create (a {number:x}))", {
      val q2 = Query.updates(
        ForeachAction(Collection(Literal(1.0), Literal(2.0), Literal(3.0)), "x", Seq(CreateNode("a", Map("number" -> Identifier("x")), Seq.empty)))
      ).returns()

      Query.
        start(CreateNodeStartItem(CreateNode("root", Map.empty, Seq.empty))).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  @Test def string_literals_should_not_be_mistaken_for_identifiers() {
    test(
      "create (tag1 {name:'tag2'}), (tag2 {name:'tag1'})",
      Query.
        start(
        CreateNodeStartItem(CreateNode("tag1", Map("name" -> Literal("tag2")), Seq.empty, true)),
        CreateNodeStartItem(CreateNode("tag2", Map("name" -> Literal("tag1")), Seq.empty, true))
      ).returns()
    )
  }

  @Test def optional_shortest_path() {
    test(
      """start a  = node(1), x = node(2,3)
         match p = shortestPath(a -[?*]-> x)
         return *""",
      Query.
        start(NodeById("a", 1),NodeById("x", 2,3)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("x"), Seq(), Direction.OUTGOING, None, optional = true, single = true, relIterator = None)).
        returns(AllIdentifiers())
    )
  }

  @Test def return_paths_back_in_the_day() {
    test(
      "start a  = node(1) return a-->()",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED1"), "  UNNAMED2", Seq(), Direction.OUTGOING, optional = false))), "a-->()"))
    )
  }

  @Test def not_with_parenthesis() {
    test(
      "start a  = node(1) where not(1=2) or 2=3 return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(Not(Equals(Literal(1), Literal(2))), Equals(Literal(2), Literal(3)))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def full_path_in_create() {
    test(
      "start a=node(1), b=node(2) create a-[r1:KNOWS]->()-[r2:LOVES]->b", {
      val secondQ = Query.
        start(
        CreateRelationshipStartItem(CreateRelationship("r1",
          RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Seq.empty, true), "KNOWS", Map())),
        CreateRelationshipStartItem(CreateRelationship("r2",
          RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true), "LOVES", Map()))).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def create_and_assign_to_path_identifierOld() {
    test(
      "create p = a-[r:KNOWS]->() return p",
      Query.
      start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Seq.empty, true), "KNOWS", Map()))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING))).
      returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def undirected_relationship_1_9() {
    test(
      "create (a {name:'A'})-[:KNOWS]-(b {name:'B'})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED1",
        RelationshipEndpoint(Identifier("a"), Map("name" -> Literal("A")), Seq.empty, true),
        RelationshipEndpoint(Identifier("b"), Map("name" -> Literal("B")), Seq.empty, true), "KNOWS", Map()))).
        returns()
    )
  }

  @Test def relate_and_assign_to_path_identifier() {
    test(
      "start a=node(0) create unique p = a-[r:KNOWS]->() return p", {
      val q2 = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink("a", "  UNNAMED1", "r", "KNOWS", Direction.OUTGOING)))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "  UNNAMED1", Seq("KNOWS"), Direction.OUTGOING))).
        returns(ReturnItem(Identifier("p"), "p"))

      Query.
        start(NodeById("a", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  @Test def use_predicate_as_expression() {
    test(
      "start n=node(0) return id(n) = 0, n is null",
      Query.
        start(NodeById("n", 0)).
        returns(
        ReturnItem(Equals(IdFunction(Identifier("n")), Literal(0)), "id(n) = 0"),
        ReturnItem(IsNull(Identifier("n")), "n is null")
      ))
  }

  @Test def create_unique_should_support_parameter_maps_1_9() {
    val start = NamedExpectation("n", bare = true)
    val rel = NamedExpectation("  UNNAMED2", bare = true)
    val end = new NamedExpectation("  UNNAMED1", ParameterExpression("param"), Map.empty, Seq.empty, true)

    val secondQ = Query.
      unique(UniqueLink(start, end, rel, "foo", Direction.OUTGOING)).
      returns(AllIdentifiers())

    test(
      "START n=node(0) CREATE UNIQUE n-[:foo]->({param}) RETURN *",
      Query.
        start(NodeById("n", 0)).
        tail(secondQ).
        returns(AllIdentifiers()))
  }


  @Test def with_limit() {
    test(
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

  @Test def with_sort_limit() {
    test(
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

  @Test def set_to_param() {
    test(
      "start n=node(0) set n = {prop}", {
      val q2 = Query.
        start().
        updates(MapPropertySetAction(Identifier("n"), ParameterExpression("prop"))).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  @Test def single_node_match_pattern() {
    test(
      "start s = node(*) match s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def single_node_match_pattern_path() {
    test(
      "start s = node(*) match p = s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        namedPaths(NamedPath("p", ParsedEntity("s"))).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  private def test(query: String, expectedQuery: AbstractQuery) {
    val parser = new CypherParserImpl()

    val ast = parser.parse(query)
    try {
      assertThat(ast, equalTo(expectedQuery))
    } catch {
      case x: AssertionError => throw new AssertionError(x.getMessage.replace("WrappedArray", "List"))
    }
  }
}
