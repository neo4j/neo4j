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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import commands.expressions._
import commands.values.{UnresolvedLabel, TokenType, KeyToken}
import commands.values.TokenType.PropertyKey
import helpers.LabelSupport
import mutation._
import org.neo4j.cypher.SyntaxException
import org.neo4j.graphdb.Direction
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions
import org.scalatest.junit.JUnitSuite

class CypherParserTest extends JUnitSuite with Assertions {
  @Test def shouldParseEasiestPossibleQuery() {
    test(
      "start s = NODE(1) return s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def should_return_string_literal() {
    test(
      "start s = node(1) return \"apa\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("apa"), "\"apa\"")))
  }

  @Test def should_return_string_literal_with_escaped_sequence_in() {
    test(
      "start s = node(1) return \"a\\tp\\\"a\\\'b\"",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a\tp\"a\'b"), "\"a\\tp\\\"a\\\'b\"")))

    test(
      "start s = node(1) return \'a\\tp\\\'a\\\"b\'",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Literal("a\tp\'a\"b"), "\'a\\tp\\\'a\\\"b\'")))
  }

  @Test def allTheNodes() {
    test(
      "start s = NODE(*) return s",
      Query.
        start(AllNodes("s")).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def allTheRels() {
    test(
      "start r = relationship(*) return r",
      Query.
        start(AllRelationships("r")).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def shouldHandleAliasingOfColumnNames() {
    test(
      "start s = NODE(1) return s as somethingElse",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "somethingElse", true)))
  }

  @Test def sourceIsAnIndex() {
    test(
      """start a = node:index(key = "value") return a""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def sourceIsAnNonParsedIndexQuery() {
    test(
      """start a = node:index("key:value") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    test(
      "start s = relationship(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseEasiestPossibleRelationshipQueryShort() {
    test(
      "start s = rel(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def sourceIsARelationshipIndex() {
    test(
      """start a = rel:index(key = "value") return a""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def keywordsShouldBeCaseInsensitive() {
    test(
      "START s = NODE(1) RETURN s",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseMultipleNodes() {
    test(
      "start s = NODE(1,2,3) return s",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ReturnItem(Identifier("s"), "s")))
  }

  @Test def shouldParseMultipleInputs() {
    test(
      "start a = node(1), b = NODE(2) return a,b",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def shouldFilterOnProp() {
    test(
      "start a = NODE(1) where a.name = \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldReturnLiterals() {
    test(
      "start a = NODE(1) return 12",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Literal(12), "12")))
  }

  @Test def shouldReturnAdditions() {
    test(
      "start a = NODE(1) return 12+2",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Add(Literal(12), Literal(2)), "12+2")))
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
        , "12/4*3-2*4")))
  }

  @Test def shouldFilterOnPropWithDecimals() {
    test(
      "start a = node(1) where a.extractReturnItems = 3.1415 return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property(Identifier("a"), PropertyKey("extractReturnItems")), Literal(3.1415))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleNot() {
    test(
      "start a = node(1) where not(a.name = \"andres\") return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleNotEqualTo() {
    test(
      "start a = node(1) where a.name <> \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleLessThan() {
    test(
      "start a = node(1) where a.name < \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleGreaterThan() {
    test(
      "start a = node(1) where a.name > \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldHandleLessThanOrEqual() {
    test(
      "start a = node(1) where a.name <= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(Property(Identifier("a"), PropertyKey("name")), Literal("andres"))).
        returns(ReturnItem(Identifier("a"), "a")))
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
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def booleanLiterals() {
    test(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(True(), Not(True()))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldFilterOnNumericProp() {
    test(
      "start a = NODE(1) where 35 = a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), Property(Identifier("a"), PropertyKey("age")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }


  @Test def shouldHandleNegativeLiteralsAsExpected() {
    test(
      "start a = NODE(1) where -35 = a.age AND a.age > -1.2 return a",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Literal(-35), Property(Identifier("a"), PropertyKey("age"))),
        GreaterThan(Property(Identifier("a"), PropertyKey("age")), Literal(-1.2)))
      ).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldCreateNotEqualsQuery() {
    test(
      "start a = NODE(1) where 35 <> a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), Property(Identifier("a"), PropertyKey("age"))))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def multipleFilters() {
    test(
      "start a = NODE(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("mattias")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def shouldCreateXorQuery() {
    test(
      "start a = NODE(1) where a.name = 'andres' xor a.name = 'mattias' return a",
      Query.
        start(NodeById("a", 1)).
        where(Xor(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("mattias")))).
        returns(ReturnItem(Identifier("a"), "a")))
  }

  @Test def relatedTo() {
    test(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def relatedToWithoutRelType() {
    test(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    test(
      "start a = NODE(1) match a-[r]->b return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("r"), "r")))
  }

  @Test def relatedToTheOtherWay() {
    test(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("b", "a", "  UNNAMED26", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def twoDoubleOptionalWithFourHalfs() {
    test(
      "START a=node(1), b=node(2) OPTIONAL MATCH a-[r1]->X<-[r2]-b, a<-[r3]-Z-[r4]->b return r1,r2,r3,r4 order by id(r1),id(r2),id(r3),id(r4)",
      Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      matches(
        RelatedTo(SingleNode("a"), SingleNode("X"), "r1", Seq(), Direction.OUTGOING, false),
        RelatedTo(SingleNode("b"), SingleNode("X"), "r2", Seq(), Direction.OUTGOING, false),
        RelatedTo(SingleNode("Z"), SingleNode("a"), "r3", Seq(), Direction.OUTGOING, false),
        RelatedTo(SingleNode("Z"), SingleNode("b"), "r4", Seq(), Direction.OUTGOING, false)
      ).makeOptional().
      orderBy(
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
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "a.name")))
  }

  @Test def shouldReadPropertiesOnExpressions() {
    test(
      "start a = NODE(1) return (a).name",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "(a).name")))
  }

  @Test def shouldHandleAndPredicates() {
    test(
      "start a = NODE(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("andres")),
        Equals(Property(Identifier("a"), PropertyKey("lastname")), Literal("taylor")))).
        returns(ReturnItem(Property(Identifier("a"), PropertyKey("name")), "a.name")))
  }

  @Test def relatedToWithRelationOutput() {
    test(
      "start a = NODE(1) match a -[rel:KNOWS]-> (b) return rel",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Seq("KNOWS"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("rel"), "rel")))
  }


  @Test def relatedToWithoutEndName() {
    test(
      "start a = NODE(1) match a -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED42", "r", Seq("MARRIED"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def relatedInTwoSteps() {
    test(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED26", Seq("KNOWS"), Direction.OUTGOING),
        RelatedTo("b", "c", "  UNNAMED40", Seq("FRIEND"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("c"), "c"))
    )
  }

  @Test def djangoRelationshipType() {
    test(
      "start a = NODE(1) match a -[r:`<<KNOWS>>`]-> b return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq("<<KNOWS>>"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("b"), "b")))
  }

  @Test def countTheNumberOfHits() {
    test(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), Direction.OUTGOING)).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(CountStar(), "count(*)")))
  }

  @Test def countStar() {
    test(
      "start a = NODE(1) return count(*) order by count(*)",
      Query.
        start(NodeById("a", 1)).
        aggregation(CountStar()).
        columns("count(*)").
        orderBy(SortItem(CountStar(), true)).
        returns(ReturnItem(CountStar(), "count(*)")))
  }

  @Test def distinct() {
    test(
      "start a = NODE(1) match a -[r]-> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        aggregation().
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b")))
  }

  @Test def sumTheAgesOfPeople() {
    test(
      "start a = NODE(1) match a -[r]-> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        aggregation(Sum(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "sum(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Sum(Property(Identifier("a"), PropertyKey("age"))), "sum(a.age)")))
  }

  @Test def avgTheAgesOfPeople() {
    test(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), Direction.OUTGOING)).
        aggregation(Avg(Property(Identifier("a"), PropertyKey("age")))).
        columns("a", "b", "avg(a.age)").
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Avg(Property(Identifier("a"), PropertyKey("age"))), "avg(a.age)")))
  }

  @Test def minTheAgesOfPeople() {
    test(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED28", Seq(), Direction.OUTGOING)).
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
        matches(RelatedTo("a", "b", "  UNNAMED26", Seq(), Direction.OUTGOING)).
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

  @Test def nestedBooleanOperatorsAndParentesisXor() {
    test(
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
        matches(RelatedTo("a", "b", "  UNNAMED29", Seq(), Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("  UNNAMED29", "a", "b", Seq.empty, Direction.OUTGOING))).
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
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelsIterable() {
    test(
      "start a=node(0) match a -[r:knows*1..3]-> x return length(r)",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING, Some("r"))).
        returns(ReturnItem(LengthFunction(Identifier("r")), "length(r)"))
    )
  }

  @Test def fixedVarLengthPath() {
    test(
      "start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", SingleNode("a"), SingleNode("x"), Some(3), Some(3), Seq(),
        Direction.OUTGOING, None, optional = false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMinDepth() {
    test(
      "start a=node(0) match a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    test(
      "start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(2), None, "knows", Direction.OUTGOING, Some("r"))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    test(
      "start a=node(0) match a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def unboundVariableLengthPath() {
    test(
      "start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED24", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def optionalRelationship() {
    test(
      "start a = node(1) optional match a --> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED35", Seq(), Direction.OUTGOING, false)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def optionalTypedRelationship() {
    test(
      "start a = node(1) optional match a -[:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED35", Seq("KNOWS"), Direction.OUTGOING)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def optionalTypedAndNamedRelationship() {
    test(
      "start a = node(1) optional match a -[r:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq("KNOWS"), Direction.OUTGOING, optional = false)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def optionalNamedRelationship() {
    test(
      "start a = node(1) optional match a -[r]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Seq(), Direction.OUTGOING)).
        makeOptional().
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def testAllIterablePredicate() {
    test(
      """start a = node(1) match p=(a-[r]->b) where all(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), Direction.OUTGOING))).
        where(AllInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b"))
    )
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
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def testNoneIterablePredicate() {
    test(
      """start a = node(1) match p=(a-[r]->b) where none(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), Direction.OUTGOING))).
        where(NoneInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def testSingleIterablePredicate() {
    test(
      """start a = node(1) match p=(a-[r]->b) where single(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a"), SingleNode("b"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq(), Direction.OUTGOING))).
        where(SingleInCollection(NodesFunction(Identifier("p")), "x", Equals(Property(Identifier("x"), PropertyKey("name")), Literal("Andres")))).
        returns(ReturnItem(Identifier("b"), "b"))
    )
  }

  @Test def testParamAsStartNode() {
    test(
      """start pA = node({a}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamAsStartRel() {
    test(
      """start pA = relationship({a}) return pA""",
      Query.
        start(RelationshipById("pA", ParameterExpression("a"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testNumericParamNameAsStartNode() {
    test(
      """start pA = node({0}) return pA""",
      Query.
        start(NodeById("pA", ParameterExpression("0"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamForWhereLiteral() {
    test(
      """start pA = node(1) where pA.name = {name} return pA""",
      Query.
        start(NodeById("pA", 1)).
        where(Equals(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("name")))
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamForIndexValue() {
    test(
      """start pA = node:idx(key = {Value}) return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), ParameterExpression("Value"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamForIndexQuery() {
    test(
      """start pA = node:idx({query}) return pA""",
      Query.
        start(NodeByIndexQuery("pA", "idx", ParameterExpression("query"))).
        returns(ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamForSkip() {
    test(
      """start pA = node(0) return pA skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamForLimit() {
    test(
      """start pA = node(0) return pA limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamForLimitAndSkip() {
    test(
      """start pA = node(0) return pA skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ("stop")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testQuotedParams() {
    test(
      """start pA = node({`id`}) where pA.name =~ {`regex`} return pA skip {`ski``pper`} limit {`stop`}""",
      Query.
        start(NodeById("pA", ParameterExpression("id"))).
        where(RegularExpression(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("regex")))
        skip("ski`pper")
        limit ("stop")
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testParamForRegex() {
    test(
      """start pA = node(0) where pA.name =~ {regex} return pA""",
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(Property(Identifier("pA"), PropertyKey("name")), ParameterExpression("regex")))
        returns (ReturnItem(Identifier("pA"), "pA"))
    )
  }

  @Test def testShortestPathWithMaxDepth() {
    test(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def testShortestPathWithType() {
    test(
      """start a=node(0), b=node(1) match p = shortestPath( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def testAllShortestPathsWithType() {
    test(
      """start a=node(0), b=node(1) match p = allShortestPaths( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), Direction.OUTGOING, Some(6), false, false, None)).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def testShortestPathWithoutStart() {
    test(
      """match p = shortestPath( a-[*..3]->b ) WHERE a.name = 'John' AND b.name = 'Sarah' return p""",
      Query.
        matches(ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), Direction.OUTGOING, Some(3), false, true, None)).
        where(And(
        Equals(Property(Identifier("a"), PropertyKey("name")), Literal("John")),
        Equals(Property(Identifier("b"), PropertyKey("name")), Literal("Sarah"))))
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def testShortestPathExpression() {
    test(
      """start a=node(0), b=node(1) return shortestPath(a-[:KNOWS*..3]->b) AS path""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        returns(ReturnItem(ShortestPathExpression(
        ShortestPath("  UNNAMED34", SingleNode("a"), SingleNode("b"), Seq("KNOWS"), Direction.OUTGOING, Some(3), false, true, None)),
        "path", true)))
  }

  @Test def testForNull() {
    test(
      """start a=node(0) where a is null return a""",
      Query.
        start(NodeById("a", 0)).
        where(IsNull(Identifier("a")))
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def testForNotNull() {
    test(
      """start a=node(0) where a is not null return a""",
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(Identifier("a"))))
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def testCountDistinct() {
    test(
      """start a=node(0) return count(distinct a)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(Distinct(Count(Identifier("a")), Identifier("a"))).
        columns("count(distinct a)").
        returns (ReturnItem(Distinct(Count(Identifier("a")), Identifier("a")), "count(distinct a)"))
    )
  }


  @Test def supportsHasRelationshipInTheWhereClause() {
    test(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED34", Seq(), Direction.OUTGOING, optional = false))))).
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def supportsNotHasRelationshipInTheWhereClause() {
    test(
      """start a=node(0), b=node(1) where not(a-->()) return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(Not(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED42"), "  UNNAMED38", Seq(), Direction.OUTGOING, optional = false)))))).
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleLFAsWhiteSpace() {
    test(
      "start\na=node(0)\nwhere\na.prop=12\nreturn\na",
      Query.
        start(NodeById("a", 0)).
        where(Equals(Property(Identifier("a"), PropertyKey("prop")), Literal(12)))
        returns (ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def shouldHandleUpperCaseDistinct() {
    test(
      "start s = NODE(1) return DISTINCT s",
      Query.
        start(NodeById("s", 1)).
        aggregation().
        returns(ReturnItem(Identifier("s"), "s"))
    )
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
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def shouldAllowCommentAlone() {
    test(
      """start s = NODE(1) return s
      // COMMENT""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def shouldAllowCommentsInsideStrings() {
    test(
      "start s = NODE(1) where s.apa = '//NOT A COMMENT' return s",
      Query.
        start(NodeById("s", 1)).
        where(Equals(Property(Identifier("s"), PropertyKey("apa")), Literal("//NOT A COMMENT")))
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def shouldHandleCommentsFollowedByWhiteSpace() {
    test(
      """start s = NODE(1)
      //I can haz more comment?
      return s""",
      Query.
        start(NodeById("s", 1)).
        returns(ReturnItem(Identifier("s"), "s"))
    )
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
        ReturnItem(TailFunction(NodesFunction(Identifier("p"))), "tail(nodes(p))"))
    )
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

    test(
      "start x = NODE(1) match p=x-[r]->z return [x in p WHERE x.prop = 123]",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, Direction.OUTGOING))).
        returns(
        ReturnItem(FilterFunction(Identifier("p"), "x", Equals(Property(Identifier("x"), PropertyKey("prop")), Literal(123))), "[x in p WHERE x.prop = 123]"))
    )
  }

  @Test def extract() {
    test(
      "start x = NODE(1) match p=x-[r]->z return [x in p | x.prop]",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, Direction.OUTGOING))).
        returns(
        ReturnItem(ExtractFunction(Identifier("p"), "x", Property(Identifier("x"), PropertyKey("prop"))), "[x in p | x.prop]"))
    )
  }

  @Test def listComprehension() {
    test(
      "start x = NODE(1) match p=x-[r]->z return [x in p WHERE x.prop > 123 | x.prop]",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "r", Seq(), Direction.OUTGOING, false)).
        namedPaths(NamedPath("p", ParsedRelation("r", "x", "z", Seq.empty, Direction.OUTGOING))).
        returns(
        ReturnItem(ExtractFunction(
          FilterFunction(Identifier("p"), "x", GreaterThan(Property(Identifier("x"), PropertyKey("prop")), Literal(123))),
          "x",
          Property(Identifier("x"), PropertyKey("prop"))
        ), "[x in p WHERE x.prop > 123 | x.prop]"))
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
    test(
      "start x = NODE(1) where x.prop in ['a','b'] return x",
      Query.
        start(NodeById("x", 1)).
        where(AnyInCollection(Collection(Literal("a"), Literal("b")), "-_-INNER-_-", Equals(Property(Identifier("x"), PropertyKey("prop")), Identifier("-_-INNER-_-")))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def in_with_collection_prop() {
    test(
      "start x = NODE(1) where x.prop in x.props return x",
      Query.
        start(NodeById("x", 1)).
        where(AnyInCollection(Property(Identifier("x"), PropertyKey("props")), "-_-INNER-_-", Equals(Property(Identifier("x"), PropertyKey("prop")), Identifier("-_-INNER-_-")))).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_match() {
    test(
      "start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED25", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_varlength_rel() {
    test(
      "start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED25", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_shortest_path() {
    test(
      "start x = NODE(1) match x-[:REL1|:REL2|:REL3]->z return x",
      Query.
        start(NodeById("x", 1)).
        matches(RelatedTo(SingleNode("x"), SingleNode("z"), "  UNNAMED25", Seq("REL1", "REL2", "REL3"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def multiple_relationship_type_in_relationship_predicate() {
    test(
      """start a=node(0), b=node(1) where a-[:KNOWS|:BLOCKS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("b"), "  UNNAMED34", Seq("KNOWS","BLOCKS"), Direction.BOTH, optional = false))))).
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
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), PropertyKey("age")), Literal(2.0))), Seq.empty, false))).
        returns(ReturnItem(Identifier("b"), "b"))

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(ReturnItem(Identifier("a"), "a"))
    })
  }

  @Test def variable_length_path_with_collection_for_relationships() {
    test(
      "start a=node(0) optional match a -[r*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED33", SingleNode("a"), SingleNode("x"), Some(1), Some(3), Seq(), Direction.OUTGOING, Some("r"), optional = false)).
        makeOptional().
        returns(ReturnItem(Identifier("x"), "x"))
    )
  }

  @Test def binary_precedence() {
    test(
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

  @Test def create_node() {
    test(
      "create a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), Seq.empty))).
        returns()
    )
  }

  @Test def create_node_from_param() {
    test(
      "create ({param})",
      Query.
        start(CreateNodeStartItem(CreateNode("  UNNAMED8", Map("*" -> ParameterExpression("param")), Seq.empty, false))).
        returns()
    )
  }

  @Test def create_node_with_a_property() {
    test(
      "create (a {name : 'Andres'})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, false))).
        returns()
    )
  }

  @Test def create_node_with_a_property_and_return_it() {
    test(
      "create (a {name : 'Andres'}) return a",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("name" -> Literal("Andres")), Seq.empty, false))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def create_node_from_map_expression() {
    test(
      "create (a {param})",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map("*" -> ParameterExpression("param")), Seq.empty, false))).
        returns()
    )
  }

  @Test def create_node_with_a_label() {
    test(
      "create (a:FOO)",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO"), false))).
        returns()
    )
  }

  @Test def create_node_with_multiple_labels() {
    test(
      "create (a:FOO:BAR)",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO", "BAR"), false))).
        returns()
    )
  }

  @Test def create_node_with_multiple_labels_with_spaces() {
    test(
      "create (a :FOO :BAR)",
      Query.
        start(CreateNodeStartItem(CreateNode("a", Map(), LabelSupport.labelCollection("FOO", "BAR"), false))).
        returns()
    )
  }

  @Test def create_nodes_with_labels_and_a_rel() {
    test(
      "CREATE (n:Person:Husband)-[:FOO]->(x:Person)",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED25",
        RelationshipEndpoint(Identifier("n"),Map(), LabelSupport.labelCollection("Person", "Husband"), false),
        RelationshipEndpoint(Identifier("x"),Map(), LabelSupport.labelCollection("Person"), false), "FOO", Map()))).
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

  @Test def create_relationship_without_identifier() {
    test(
      "create (a {a})-[:REL]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED14",
        RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")),Seq.empty, false),
        RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")),Seq.empty, false),
        "REL", Map()))).
        returns()
    )
  }

  @Test def create_relationship_with_properties_from_map() {
    test(
      "create (a {a})-[:REL {param}]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED14",
        RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")), Seq.empty, false),
        RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")), Seq.empty, false),
        "REL", Map("*" -> ParameterExpression("param"))))).
        returns()
    )
  }

  @Test def create_relationship_without_identifier2() {
    test(
      "create (a {a})-[:REL]->(b {b})",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED14",
        RelationshipEndpoint(Identifier("a"), Map("*" -> ParameterExpression("a")), Seq.empty, false),
        RelationshipEndpoint(Identifier("b"), Map("*" -> ParameterExpression("b")), Seq.empty, false),
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

  @Test def set_property_on_node_from_expression() {
    test(
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

  @Test def remove_property() {
    test(
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

  @Test def foreach_on_path() {
    test(
      "start a=node(0) match p = a-[r:REL]->b with p foreach(n in nodes(p) | set n.touched = true ) ", {
      val secondQ = Query.
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), PropertyKey("touched")), True())))).
        returns()

      Query.
        start(NodeById("a", 0)).
        matches(RelatedTo("a", "b", "r", "REL", Direction.OUTGOING)).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "b", Seq("REL"), Direction.OUTGOING))).
        tail(secondQ).
        returns(ReturnItem(Identifier("p"), "p"))
    })
  }

  @Test def foreach_on_path_with_multiple_updates() {
    test(
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

  @Test def simple_read_first_and_update_next() {
    test(
      "start a = node(1) create (b {age : a.age * 2}) return b", {
      val secondQ = Query.
        start(CreateNodeStartItem(CreateNode("b", Map("age" -> Multiply(Property(Identifier("a"), PropertyKey("age")), Literal(2.0))), Seq.empty, false))).
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
      ))).returns()

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
        updates(ForeachAction(NodesFunction(Identifier("p")), "n", Seq(PropertySetAction(Property(Identifier("n"), PropertyKey("touched")), True())))).
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
        returns(AllIdentifiers()))
  }

  @Test def single_create_unique() {
    test(
      "start a = node(1), b=node(2) create unique a-[:reltype]->b", {
      val secondQ = Query.
        unique(UniqueLink("a", "b", "  UNNAMED44", "reltype", Direction.OUTGOING)).
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
        unique(UniqueLink("a", "  UNNAMED58", "  UNNAMED44", "reltype", Direction.OUTGOING)).
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
        UniqueLink("a", "b", "  UNNAMED33", "X", Direction.OUTGOING),
        UniqueLink("c", "b", "  UNNAMED41", "X", Direction.OUTGOING)).
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
          NamedExpectation("  UNNAMED33", bare = true), "X", Direction.OUTGOING)).
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
          NamedExpectation("  UNNAMED33", Map[String, Expression]("name" -> Literal("Andres")), bare = false), "X", Direction.OUTGOING)).
        returns()

      Query.
        start(NodeById("a", 1)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def foreach_with_literal_collection() {
    test(
      "create root foreach(x in [1,2,3] | create (a {number:x}))",
      Query.
        start(CreateNodeStartItem(CreateNode("root", Map.empty, Seq.empty))).
        updates(ForeachAction(Collection(Literal(1.0), Literal(2.0), Literal(3.0)), "x", Seq(CreateNode("a", Map("number" -> Identifier("x")), Seq.empty, false)))).
        returns()
    )
  }

  @Test def string_literals_should_not_be_mistaken_for_identifiers() {
    test(
      "create (tag1 {name:'tag2'}), (tag2 {name:'tag1'})",
      Query.
        start(
        CreateNodeStartItem(CreateNode("tag1", Map("name" -> Literal("tag2")), Seq.empty, false)),
        CreateNodeStartItem(CreateNode("tag2", Map("name" -> Literal("tag1")), Seq.empty, false))
      ).returns()
    )
  }

  @Test def relate_with_two_rels_to_same_node() {
    test(
      "start root=node(0) create unique x<-[r1:X]-root-[r2:Y]->x return x", {
      val returns = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(
        UniqueLink("root", "x", "r1", "X", Direction.OUTGOING),
        UniqueLink("root", "x", "r2", "Y", Direction.OUTGOING))))
        .returns(ReturnItem(Identifier("x"), "x"))

      Query.start(NodeById("root", 0)).tail(returns).returns(AllIdentifiers())
    })
  }

  @Test def optional_shortest_path() {
    test(
      """start a  = node(1), x = node(2,3)
         optional match p = shortestPath(a -[*]-> x)
         return *""",
      Query.
        start(NodeById("a", 1),NodeById("x", 2,3)).
        matches(ShortestPath("p", SingleNode("a"), SingleNode("x"), Seq(), Direction.OUTGOING, None, optional = false, single = true, relIterator = None)).
        makeOptional().
        returns(AllIdentifiers())
    )
  }

  @Test def return_paths() {
    test(
      "start a  = node(1) return a-->()",
      Query.
        start(NodeById("a", 1)).
        returns(ReturnItem(PathExpression(Seq(RelatedTo(SingleNode("a"), SingleNode("  UNNAMED31"), "  UNNAMED27", Seq(), Direction.OUTGOING, optional = false))), "a-->()"))
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

  @Test def precedence_of_not_without_parenthesis() {
    test(
      "start a = node(1) where not true or false return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(Not(True()), Not(True()))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
    test(
      "start a = node(1) where not 1 < 2 return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(LessThan(Literal(1), Literal(2)))).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def not_with_pattern() {
    def parsedQueryWithOffsets(offset1: Int, offset2: Int) = Query.
      matches(SingleNode("admin")).
      where(Not(NonEmpty(PathExpression(Seq(RelatedTo(SingleNode("admin"), SingleNode("  UNNAMED" + offset2), "  UNNAMED" + offset1, Seq("MEMBER_OF"), Direction.OUTGOING, optional = false)))))).
      returns(ReturnItem(Identifier("admin"), "admin"))

    test(
      "MATCH (admin) WHERE NOT (admin)-[:MEMBER_OF]->() RETURN admin",
      parsedQueryWithOffsets(31, 47))

    test(
      "MATCH (admin) WHERE NOT ((admin)-[:MEMBER_OF]->()) RETURN admin",
      parsedQueryWithOffsets(32, 48))
  }

  @Test def full_path_in_create() {
    test(
      "start a=node(1), b=node(2) create a-[r1:KNOWS]->()-[r2:LOVES]->b", {
      val secondQ = Query.
        start(
        CreateRelationshipStartItem(CreateRelationship("r1",
          RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("  UNNAMED49"), Map(), Seq.empty, true), "KNOWS", Map())),
        CreateRelationshipStartItem(CreateRelationship("r2",
          RelationshipEndpoint(Identifier("  UNNAMED49"), Map(), Seq.empty, true),
          RelationshipEndpoint(Identifier("b"), Map(), Seq.empty, true), "LOVES", Map()))).
        returns()

      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def create_and_assign_to_path_identifier() {
    test(
      "create p = a-[r:KNOWS]->() return p",
      Query.
        start(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty, true),
        RelationshipEndpoint(Identifier("  UNNAMED25"), Map(), Seq.empty, true), "KNOWS", Map()))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "  UNNAMED25", Seq("KNOWS"), Direction.OUTGOING))).
        returns(ReturnItem(Identifier("p"), "p")))
  }

  @Test def relate_and_assign_to_path_identifier() {
    test(
      "start a=node(0) create unique p = a-[r:KNOWS]->() return p", {
      val q2 = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink("a", "  UNNAMED48", "r", "KNOWS", Direction.OUTGOING)))).
        namedPaths(NamedPath("p", ParsedRelation("r", "a", "  UNNAMED48", Seq("KNOWS"), Direction.OUTGOING))).
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

  @Test def create_unique_should_support_parameter_maps() {
    test(
      "START n=node(0) CREATE UNIQUE n-[:foo]->({param}) RETURN *", {
      val start = NamedExpectation("n", true)
      val rel = NamedExpectation("  UNNAMED31", true)
      val end = NamedExpectation("  UNNAMED41", Map("*" -> ParameterExpression("param")), Seq.empty, false)
      val secondQ = Query.
        unique(UniqueLink(start, end, rel, "foo", Direction.OUTGOING)).
        returns(AllIdentifiers())

      Query.
        start(NodeById("n", 0)).
        tail(secondQ).
        returns(AllIdentifiers())
    })
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
    val q2 = Query.
      start().
      updates(MapPropertySetAction(Identifier("n"), ParameterExpression("prop"))).
      returns()

    test(
      "start n=node(0) set n = {prop}",
      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers()))
  }

  @Test def set_to_map() {
    test(
      "start n=node(0) set n = {key: 'value', foo: 1}", {
      val q2 = Query.
        start().
        updates(MapPropertySetAction(Identifier("n"), LiteralMap(Map("key" -> Literal("value"), "foo" -> Literal(1))))).
        returns()

      Query.
        start(NodeById("n", 0)).
        tail(q2).
        returns(AllIdentifiers())
    })
  }

  @Test def add_label() {
    test(
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

  @Test def add_short_label() {
    test(
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

  @Test def add_multiple_labels() {
    test(
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

  @Test def add_multiple_short_labels() {
    test(
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

  @Test def add_multiple_short_labels2() {
    test(
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

  @Test def remove_label() {
    test(
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

  @Test def remove_multiple_labels() {
    test(
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

  @Test def filter_by_label_in_where() {
    test(
      "START n=node(0) WHERE (n):Foo RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(HasLabel(Identifier("n"), KeyToken.Unresolved("Foo", TokenType.Label))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test def filter_by_label_in_where_with_expression() {
    test(
      "START n=node(0) WHERE (n):Foo RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(HasLabel(Identifier("n"), KeyToken.Unresolved("Foo", TokenType.Label))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test def filter_by_labels_in_where() {
    test(
      "START n=node(0) WHERE n:Foo:Bar RETURN n",
      Query.
        start(NodeById("n", 0)).
        where(And(HasLabel(Identifier("n"), KeyToken.Unresolved("Foo", TokenType.Label)), HasLabel(Identifier("n"), KeyToken.Unresolved("Bar", TokenType.Label)))).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test(expected = classOf[SyntaxException]) def create_no_index_without_properties() {
    test(
      "create index on :MyLabel",
      CreateIndex("MyLabel", Seq()))
  }

  @Test def create_index_on_single_property() {
    test(
      "create index on :MyLabel(prop1)",
      CreateIndex("MyLabel", Seq("prop1")))
  }

  @Test(expected = classOf[SyntaxException]) def create_index_on_multiple_properties() {
    test(
      "create index on :MyLabel(prop1, prop2)",
      CreateIndex("MyLabel", Seq("prop1", "prop2")))
  }

  @Test def match_left_with_single_label() {
    test(
      "start a = NODE(1) match (a:foo) -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a", Seq(UnresolvedLabel("foo"))), SingleNode("  UNNAMED48"), "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def match_left_with_multiple_labels() {
    test(
      "start a = NODE(1) match (a:foo:bar) -[r:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("a", Seq(UnresolvedLabel("foo"), UnresolvedLabel("bar"))), SingleNode("  UNNAMED52"), "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def match_right_with_multiple_labels() {
    test(
      "start a = NODE(1) match () -[r:MARRIED]-> (a:foo:bar) return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("  UNNAMED25"), SingleNode("a", Seq(UnresolvedLabel("foo"), UnresolvedLabel("bar"))), "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def match_both_with_labels() {
    test(
      "start a = NODE(1) match (b:foo) -[r:MARRIED]-> (a:bar) return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo(SingleNode("b", Seq(UnresolvedLabel("foo"))), SingleNode("a", Seq(UnresolvedLabel("bar"))), "r", Seq("MARRIED"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("a"), "a"))
    )
  }

  @Test def union_ftw() {
    val q1 = Query.
      start(NodeById("s", 1)).
      returns(ReturnItem(Identifier("s"), "s"))
    val q2 = Query.
      start(NodeById("t", 1)).
      returns(ReturnItem(Identifier("t"), "t"))
    val q3 = Query.
      start(NodeById("u", 1)).
      returns(ReturnItem(Identifier("u"), "u"))

    test(
      "start s = NODE(1) return s UNION all start t = NODE(1) return t UNION all start u = NODE(1) return u",
      Union(Seq(q1, q2, q3), QueryString.empty, distinct = false)
    )
  }

  @Test def union_distinct() {
    val q = Query.
      start(NodeById("s", 1)).
      returns(ReturnItem(Identifier("s"), "s"))

    test(
      "start s = NODE(1) return s UNION start s = NODE(1) return s UNION start s = NODE(1) return s",
      Union(Seq(q, q, q), QueryString.empty, distinct = true)
    )
  }

  @Test def keywords_in_reltype_and_label() {
    test(
      "START n=node(0) MATCH (n:On)-[:WHERE]->() RETURN n",
      Query.
        start(NodeById("n", 0)).
        matches(RelatedTo(SingleNode("n", Seq(UnresolvedLabel("On"))), SingleNode("  UNNAMED40"), "  UNNAMED28", Seq("WHERE"), Direction.OUTGOING, false)).
        returns(ReturnItem(Identifier("n"), "n"))
    )
  }

  @Test def remove_index_on_single_property() {
    test(
      "drop index on :MyLabel(prop1)",
      DropIndex("MyLabel", Seq("prop1"))
    )
  }

  @Test def simple_query_with_index_hint() {
    test(
      "match (n:Person)-->() using index n:Person(name) where n.name = 'Andres' return n",
      Query.matches(RelatedTo(SingleNode("n", Seq(UnresolvedLabel("Person"))), SingleNode("  UNNAMED20"), "  UNNAMED16", Seq(), Direction.OUTGOING, optional = false)).
        where(Equals(Property(Identifier("n"), PropertyKey("name")), Literal("Andres"))).
        using(SchemaIndex("n", "Person", "name", AnyIndex, None)).
        returns(ReturnItem(Identifier("n"), "n", renamed = false))
    )
  }

  @Test def single_node_match_pattern() {
    test(
      "start s = node(*) match s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def awesome_single_labeled_node_match_pattern() {
    test(
      "match (s:nostart) return s",
      Query.
        matches(SingleNode("s", Seq(UnresolvedLabel("nostart")))).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def single_node_match_pattern_path() {
    test(
      "start s = node(*) match p = s return s",
      Query.
        start(AllNodes("s")).
        matches(SingleNode("s")).
        namedPaths(NamedPath("p", ParsedEntity("s"))).
        returns(ReturnItem(Identifier("s"), "s"))
    )
  }

  @Test def label_scan_hint() {
    test(
      "match (p:Person) using scan p:Person return p",
      Query.
        matches(SingleNode("p", Seq(UnresolvedLabel("Person")))).
        using(NodeByLabel("p", "Person")).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def varlength_named_path() {
    test(
      "start n=node(1) match p=n-[:KNOWS*..2]->x return p",
      Query.
        start(NodeById("n", 1)).
        matches(VarLengthRelatedTo("  UNNAMED25", SingleNode("n"), SingleNode("x"), None, Some(2), Seq("KNOWS"), Direction.OUTGOING, None, false)).
        namedPaths(NamedPath("p", ParsedVarLengthRelation("  UNNAMED25", Map.empty, ParsedEntity("n"), ParsedEntity("x"), Seq("KNOWS"), Direction.OUTGOING, false, None, Some(2), None))).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def reduce_function() {
    val collection = Collection(Literal(1), Literal(2), Literal(3))
    val expression = Add(Identifier("acc"), Identifier("x"))
    test(
      "start n=node(1) return reduce(acc = 0, x in [1,2,3] | acc + x)",
      Query.
        start(NodeById("n", 1)).
        returns(ReturnItem(ReduceFunction(collection, "x", expression, "acc", Literal(0)), "reduce(acc = 0, x in [1,2,3] | acc + x)"))
    )
  }

  @Test def start_and_endNode() {
    test(
      "start r=rel(1) return startNode(r), endNode(r)",
      Query.
        start(RelationshipById("r", 1)).
        returns(
        ReturnItem(RelationshipEndPoints(Identifier("r"), start = true), "startNode(r)"),
        ReturnItem(RelationshipEndPoints(Identifier("r"), start = false), "endNode(r)"))
    )
  }

  @Test def mathy_aggregation_expressions() {
    val property = Property(Identifier("n"), PropertyKey("property"))
    val percentileCont = PercentileCont(property, Literal(0.4))
    val percentileDisc = PercentileDisc(property, Literal(0.5))
    val stdev = Stdev(property)
    val stdevP = StdevP(property)
    test(
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

  @Test def escaped_identifier() {
    test(
      "match `Unusual identifier` return `Unusual identifier`.propertyName",
      Query.
        matches(SingleNode("Unusual identifier")).
        returns(
        ReturnItem(Property(Identifier("Unusual identifier"), PropertyKey("propertyName")), "`Unusual identifier`.propertyName"))
    )
  }

  @Test def aliased_column_does_not_keep_escape_symbols() {
    test(
      "match a return a as `Escaped alias`",
      Query.
        matches(SingleNode("a")).
        returns(
        ReturnItem(Identifier("a"), "Escaped alias", renamed = true))
    )
  }

  @Test def create_with_labels_and_props_with_parens() {
    test(
      "CREATE (node :FOO:BAR {name: 'Stefan'})",
      Query.
        start(CreateNodeStartItem(CreateNode("node", Map("name"->Literal("Stefan")),
        LabelSupport.labelCollection("FOO", "BAR"), bare = false))).
        returns()
    )
  }

  @Test def constraint_creation() {
    test(
      "CREATE CONSTRAINT ON (id:Label) ASSERT id.property IS UNIQUE",
      CreateUniqueConstraint("id", "Label", "id", "property")
    )
  }

  @Test def named_path_with_variable_length_path_and_named_relationships_collection() {
    test(
      "match p = (a)-[r*]->(b) return p",
      Query.
        matches(VarLengthRelatedTo("  UNNAMED13", SingleNode("a"), SingleNode("b"), None, None, Seq.empty, Direction.OUTGOING, Some("r"), optional = false)).
        namedPaths(NamedPath("p", ParsedVarLengthRelation("  UNNAMED13", Map.empty, ParsedEntity("a"), ParsedEntity("b"), Seq.empty, Direction.OUTGOING, optional = false, None, None, Some("r")))).
        returns(ReturnItem(Identifier("p"), "p"))
    )
  }

  @Test def variable_length_relationship_with_rel_collection() {
    test(
      "MATCH (a)-[rels*]->(b) WHERE ALL(r in rels WHERE r.prop = 42) RETURN rels",
      Query.
        matches(VarLengthRelatedTo("  UNNAMED9", SingleNode("a"), SingleNode("b"), None, None, Seq.empty, Direction.OUTGOING, Some("rels"), optional = false)).
        where(AllInCollection(Identifier("rels"), "r", Equals(Property(Identifier("r"), PropertyKey("prop")), Literal(42)))).
        returns(ReturnItem(Identifier("rels"), "rels"))
    )
  }

  @Test def simple_case_statement() {
    test(
      "MATCH (a) RETURN CASE a.prop WHEN 1 THEN 'hello' ELSE 'goodbye' END AS result",
      Query.
        matches(SingleNode("a")).
        returns(
        ReturnItem(SimpleCase(Property(Identifier("a"), PropertyKey("prop")), Seq(
          (Literal(1), Literal("hello"))
        ), Some(Literal("goodbye"))), "result", true))
    )
  }

  @Test def generic_case_statement() {
    test(
      "MATCH (a) RETURN CASE WHEN a.prop = 1 THEN 'hello' ELSE 'goodbye' END AS result",
      Query.
        matches(SingleNode("a")).
        returns(
        ReturnItem(GenericCase(Seq(
          (Equals(Property(Identifier("a"), PropertyKey("prop")), Literal(1)), Literal("hello"))
        ), Some(Literal("goodbye"))), "result", true))
    )
  }

  @Test def shouldGroupCreateAndCreateUpdate() {
    test(
      """START me=node(0) MATCH p1 = me-[*2]-friendOfFriend CREATE p2 = me-[:MARRIED_TO]->(wife {name:"Gunhild"}) CREATE UNIQUE p3 = wife-[:KNOWS]-friendOfFriend RETURN p1,p2,p3""", {
      val thirdQ = Query.
        start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink("wife", "friendOfFriend", "  UNNAMED128", "KNOWS", Direction.BOTH)))).
        namedPaths(NamedPath("p3", ParsedRelation("  UNNAMED128", "wife", "friendOfFriend", Seq("KNOWS"), Direction.BOTH))).
        returns(ReturnItem(Identifier("p1"), "p1"), ReturnItem(Identifier("p2"), "p2"), ReturnItem(Identifier("p3"), "p3"))

      val secondQ = Query.
        start(CreateRelationshipStartItem(CreateRelationship("  UNNAMED65",
        RelationshipEndpoint(Identifier("me"),Map.empty, Seq.empty, true),
        RelationshipEndpoint(Identifier("wife"), Map("name" -> Literal("Gunhild")), Seq.empty, false),
        "MARRIED_TO", Map()))).
        namedPaths(NamedPath("p2", new ParsedRelation("  UNNAMED65", Map(),
        ParsedEntity("me"),
        ParsedEntity("wife", Identifier("wife"), Map("name" -> Literal("Gunhild")), Seq.empty, false),
        Seq("MARRIED_TO"), Direction.OUTGOING, false))).
        tail(thirdQ).
        returns(AllIdentifiers())

      Query.start(NodeById("me", 0)).
        matches(VarLengthRelatedTo("  UNNAMED30", SingleNode("me"), SingleNode("friendOfFriend"), Some(2), Some(2), Seq.empty, Direction.BOTH, None, false)).
        namedPaths(NamedPath("p1", ParsedVarLengthRelation("  UNNAMED30", Map.empty, ParsedEntity("me"), ParsedEntity("friendOfFriend"), Seq.empty, Direction.BOTH, false, Some(2), Some(2), None))).
        tail(secondQ).
        returns(AllIdentifiers())
    })
  }

  @Test def return_only_query_with_literal_map() {
    test(
      "RETURN { key: 'value' }",
      Query.
        matches().
        returns(
        ReturnItem(LiteralMap(Map("key"->Literal("value"))), "{ key: 'value' }"))
    )
  }

  @Test def long_match_chain() {
    test("match (a)<-[r1:REL1]-(b)<-[r2:REL2]-(c) return a, b, c",
      Query.
        matches(
        RelatedTo("b", "a", "r1", Seq("REL1"), Direction.OUTGOING),
        RelatedTo("c", "b", "r2", Seq("REL2"), Direction.OUTGOING)).
        returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"), ReturnItem(Identifier("c"), "c"))
    )
  }

  @Test def long_create_chain() {
    test("create (a)<-[r1:REL1]-(b)<-[r2:REL2]-(c)",
      Query.
        start(
        CreateRelationshipStartItem(CreateRelationship("r1",
          RelationshipEndpoint(Identifier("b"), Map.empty, Seq.empty, true),
          RelationshipEndpoint(Identifier("a"), Map.empty, Seq.empty, true),
          "REL1", Map())),
        CreateRelationshipStartItem(CreateRelationship("r2",
          RelationshipEndpoint(Identifier("c"), Map.empty, Seq.empty, true),
          RelationshipEndpoint(Identifier("b"), Map.empty, Seq.empty, true),
          "REL2", Map()))).
        returns()
    )
  }

  @Test def test_literal_numbers() {
    test(
      "RETURN 0.5, .5, 50",
      Query.
        matches().
        returns(ReturnItem(Literal(0.5), "0.5"), ReturnItem(Literal(0.5), ".5"), ReturnItem(Literal(50), "50"))
    )
  }

  @Test def test_unary_plus_minus() {
    test(
      "MATCH n RETURN -n.prop, +n.foo, 1 + -n.bar",
      Query.
        matches(SingleNode("n")).
        returns(ReturnItem(Subtract(Literal(0), Property(Identifier("n"), PropertyKey("prop"))), "-n.prop"),
        ReturnItem(Property(Identifier("n"), PropertyKey("foo")), "+n.foo"),
        ReturnItem(Add(Literal(1), Subtract(Literal(0), Property(Identifier("n"), PropertyKey("bar")))), "1 + -n.bar"))
    )
  }

  @Test
  def compileQueryIntegrationTest() {
    val q = parser.parseToQuery("create (a1) create (a2) create (a3) create (a4) create (a5) create (a6) create (a7)").asInstanceOf[commands.Query]
    assert(q.tail.nonEmpty, "wasn't compacted enough")
    val compacted = q.compact

    assert(compacted.tail.isEmpty, "wasn't compacted enough")
    assert(compacted.start.size === 7, "lost create commands")
  }

  @Test def should_handle_optional_match() {
    test(
      "OPTIONAL MATCH n RETURN n",
      Query.
        optionalMatches(SingleNode("n")).
        returns(ReturnItem(Identifier("n"), "n")))
  }

  @Test
  def compileQueryIntegrationTest2() {
    val q = parser.parseToQuery("create (a1) create (a2) create (a3) with a1 create (a4) return a1, a4").asInstanceOf[commands.Query]
    val compacted = q.compact
    var lastQ = compacted

    while (lastQ.tail.nonEmpty)
      lastQ = lastQ.tail.get

    assert(lastQ.returns.columns === List("a1", "a4"), "Lost the tail while compacting")
  }

  @Test def should_handle_optional_match_following_optional_match() {
    val last = Query.matches(RelatedTo("c", "n", "r2", Seq.empty, Direction.OUTGOING)).makeOptional().returns(AllIdentifiers())
    val second = Query.matches(RelatedTo("n", "b", "r1", Seq.empty, Direction.OUTGOING)).makeOptional().tail(last).returns(AllIdentifiers())
    val first = Query.matches(SingleNode("n")).tail(second).returns(AllIdentifiers())

    test(
      "MATCH (n) OPTIONAL MATCH (n)-[r1]->(b) OPTIONAL MATCH (n)<-[r2]-(c) RETURN *",
      first)
  }

  val parser = CypherParser()

  private def test(query: String, expectedQuery: AbstractQuery) {
    val ast = parser.parseToQuery(query)
    try {
      assertThat(query, ast, equalTo(expectedQuery))
    } catch {
      case x: AssertionError => throw new AssertionError(x.getMessage.replace("WrappedArray", "List"))
    }
  }
}
