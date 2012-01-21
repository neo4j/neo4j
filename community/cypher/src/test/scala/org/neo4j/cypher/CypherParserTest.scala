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

import internal.parser.v1_6.ConsoleCypherParser
import org.neo4j.cypher.commands._
import org.junit.Assert._
import org.neo4j.graphdb.Direction
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Ignore
import org.scalatest.Assertions

class CypherParserTest extends JUnitSuite with Assertions {
  @Test def shouldParseEasiestPossibleQuery() {
    testQuery("start s = NODE(1) return s",
      Query.
        start(NodeById("s", 1)).
        returns(ExpressionReturnItem(Entity("s"))))
  }

  @Test def shouldHandleAliasingOfColumnNames() {
    testQuery("start s = NODE(1) return s as somethingElse",
      Query.
        start(NodeById("s", 1)).
        returns(AliasReturnItem(ExpressionReturnItem(Entity("s")), "somethingElse")))
  }

  @Test def sourceIsAnIndex() {
    testQuery(
      """start a = node:index(key = "value") return a""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def sourceIsAnNonParsedIndexQuery() {
    testQuery(
      """start a = node:index("key:value") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Ignore
  @Test def sourceIsParsedAdvancedLuceneQuery() {
    testQuery(
      """start a = node:index(key="value" AND otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" AND otherKey:\"otherValue\""))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Ignore
  @Test def parsedOrIdxQuery() {
    testQuery(
      """start a = node:index(key="value" or otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" OR otherKey:\"otherValue\""))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    testQuery(
      "start s = relationship(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ExpressionReturnItem(Entity("s"))))
  }

  @Test def shouldParseEasiestPossibleRelationshipQueryShort() {
    testQuery(
      "start s = rel(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ExpressionReturnItem(Entity("s"))))
  }

  @Test def sourceIsARelationshipIndex() {
    testQuery(
      """start a = rel:index(key = "value") return a""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }


  @Test def keywordsShouldBeCaseInsensitive() {
    testQuery(
      "START s = NODE(1) RETURN s",
      Query.
        start(NodeById("s", 1)).
        returns(ExpressionReturnItem(Entity("s"))))
  }

  @Test def shouldParseMultipleNodes() {
    testQuery(
      "start s = NODE(1,2,3) return s",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ExpressionReturnItem(Entity("s"))))
  }

  @Test def shouldParseMultipleInputs() {
    testQuery(
      "start a = node(1), b = NODE(2) return a,b",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def shouldFilterOnProp() {
    testQuery(
      "start a = NODE(1) where a.name = \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property("a", "name"), Literal("andres"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldFilterOnPropWithDecimals() {
    testQuery(
      "start a = node(1) where a.extractReturnItems = 3.1415 return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Property("a", "extractReturnItems"), Literal(3.1415))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleNot() {
    testQuery(
      "start a = node(1) where not(a.name = \"andres\") return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property("a", "name"), Literal("andres")))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleNotEqualTo() {
    testQuery(
      "start a = node(1) where a.name <> \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Property("a", "name"), Literal("andres")))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleLessThan() {
    testQuery(
      "start a = node(1) where a.name < \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(Property("a", "name"), Literal("andres"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleGreaterThan() {
    testQuery(
      "start a = node(1) where a.name > \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(Property("a", "name"), Literal("andres"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleLessThanOrEqual() {
    testQuery(
      "start a = node(1) where a.name <= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(Property("a", "name"), Literal("andres"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleRegularComparison() {
    testQuery(
      "start a = node(1) where \"Andres\" =~ /And.*/ return a",
      Query.
        start(NodeById("a", 1)).
        where(RegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ExpressionReturnItem(Entity("a")))
    )
  }

  @Test def shouldHandleMultipleRegularComparison() {
    testQuery(
      """start a = node(1) where a.name =~ /And.*/ AND a.name =~ /And.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(RegularExpression(Property("a", "name"), Literal("And.*")), RegularExpression(Property("a", "name"), Literal("And.*")))).
        returns(ExpressionReturnItem(Entity("a")))
    )
  }

  @Test def shouldHandleEscapedRegexs() {
    testQuery(
      """start a = node(1) where a.name =~ /And\/.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(RegularExpression(Property("a", "name"), Literal("And\\/.*"))).
        returns(ExpressionReturnItem(Entity("a")))
    )
  }

  @Test def shouldHandleGreaterThanOrEqual() {
    testQuery(
      "start a = node(1) where a.name >= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThanOrEqual(Property("a", "name"), Literal("andres"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }


  @Test def booleanLiterals() {
    testQuery(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(true), Literal(false))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldFilterOnNumericProp() {
    testQuery(
      "start a = NODE(1) where 35 = a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), Property("a", "age"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }


  @Test def shouldHandleNegativeLiteralsAsExpected() {
    testQuery(
      "start a = NODE(1) where -35 = a.age AND a.age > -1.2 return a",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Literal(-35), Property("a", "age")),
        GreaterThan(Property("a", "age"), Literal(-1.2)))
      ).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldCreateNotEqualsQuery() {
    testQuery(
      "start a = NODE(1) where 35 != a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), Property("a", "age")))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def multipleFilters() {
    testQuery(
      "start a = NODE(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(Property("a", "name"), Literal("andres")),
        Equals(Property("a", "name"), Literal("mattias")))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def relatedTo() {
    testQuery(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def relatedToWithoutRelType() {
    testQuery(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testQuery(
      "start a = NODE(1) match a -[r]-> (b) return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", None, Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(Entity("r"))))
  }

  @Test def relatedToTheOtherWay() {
    testQuery(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.INCOMING, false)).
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def shouldOutputVariables() {
    testQuery(
      "start a = NODE(1) return a.name",
      Query.
        start(NodeById("a", 1)).
        returns(ExpressionReturnItem(Property("a", "name"))))
  }

  @Test def shouldHandleAndPredicates() {
    testQuery(
      "start a = NODE(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Property("a", "name"), Literal("andres")),
        Equals(Property("a", "lastname"), Literal("taylor")))).
        returns(ExpressionReturnItem(Property("a", "name"))))
  }

  @Test def relatedToWithRelationOutput() {
    testQuery(
      "start a = NODE(1) match a -[rel:KNOWS]-> (b) return rel",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Some("KNOWS"), Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(Entity("rel"))))
  }

  @Test def relatedToWithoutEndName() {
    testQuery(
      "start a = NODE(1) match a -[:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Some("MARRIED"), Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def relatedInTwoSteps() {
    testQuery(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, false),
        RelatedTo("b", "c", "  UNNAMED2", Some("FRIEND"), Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(Entity("c")))
    )
  }

  @Test def djangoRelationshipType() {
    testQuery(
      "start a = NODE(1) match a -[:`<<KNOWS>>`]-> b return c",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("<<KNOWS>>"), Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(Entity("c"))))
  }

  @Test def countTheNumberOfHits() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def distinct() {
    testQuery(
      "start a = NODE(1) match a --> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation().
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def sumTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Sum(Property("a", "age")))).
        columns("a", "b", "sum(a.age)").
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def avgTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Avg(Property("a", "age")))).
        columns("a", "b", "avg(a.age)").
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def minTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Min(Property("a", "age")))).
        columns("a", "b", "min(a.age)").
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def maxTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, max(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Max((Property("a", "age"))))).
        columns("a", "b", "max(a.age)").
        returns(ExpressionReturnItem(Entity("a")), ExpressionReturnItem(Entity("b"))))
  }

  @Test def singleColumnSorting() {
    testQuery(
      "start a = NODE(1) return a order by a.name",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ExpressionReturnItem(Property("a", "name")), true)).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def sortOnAggregatedColumn() {
    testQuery(
      "start a = NODE(1) return a order by avg(a.name)",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueAggregationItem(Avg(Property("a", "name"))), true)).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleTwoSortColumns() {
    testQuery(
      "start a = NODE(1) return a order by a.name, a.age",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(ExpressionReturnItem(Property("a", "name")), true),
        SortItem(ExpressionReturnItem(Property("a", "age")), true)).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleTwoSortColumnsAscending() {
    testQuery(
      "start a = NODE(1) return a order by a.name ASCENDING, a.age ASC",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(ExpressionReturnItem(Property("a", "name")), true),
        SortItem(ExpressionReturnItem(Property("a", "age")), true)).
        returns(ExpressionReturnItem(Entity("a"))))

  }

  @Test def orderByDescending() {
    testQuery(
      "start a = NODE(1) return a order by a.name DESCENDING",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ExpressionReturnItem(Property("a", "name")), false)).
        returns(ExpressionReturnItem(Entity("a"))))

  }

  @Test def orderByDesc() {
    testQuery(
      "start a = NODE(1) return a order by a.name desc",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ExpressionReturnItem(Property("a", "name")), false)).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def nullableProperty() {
    testQuery(
      "start a = NODE(1) return a.name?",
      Query.
        start(NodeById("a", 1)).
        returns(ExpressionReturnItem(Nullable(Property("a", "name")))))
  }

  @Test def nestedBooleanOperatorsAndParentesis() {
    testQuery(
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
        returns(ExpressionReturnItem(Entity("n"))))
  }

  @Test def limit5() {
    testQuery(
      "start n=NODE(1) return n limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        returns(ExpressionReturnItem(Entity("n"))))
  }

  @Test def skip5() {
    testQuery(
      "start n=NODE(1) return n skip 5",
      Query.
        start(NodeById("n", 1)).
        skip(5).
        returns(ExpressionReturnItem(Entity("n"))))
  }

  @Test def skip5limit5() {
    testQuery(
      "start n=NODE(1) return n skip 5 limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        skip(5).
        returns(ExpressionReturnItem(Entity("n"))))
  }

  @Test def relationshipType() {
    testQuery(
      "start n=NODE(1) match n-[r]->(x) where type(r) = \"something\" return r",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
        where(Equals(RelationshipTypeFunction(Entity("r")), Literal("something"))).
        returns(ExpressionReturnItem(Entity("r"))))
  }

  @Test def pathLength() {
    testQuery(
      "start n=NODE(1) match p=(n-->x) where LENGTH(p) = 10 return p",
      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        where(Equals(LengthFunction(Entity("p")), Literal(10.0))).
        returns(ExpressionReturnItem(Entity("p"))))
  }

  @Test def relationshipTypeOut() {
    testQuery(
      "start n=NODE(1) match n-[r]->(x) return type(r)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(RelationshipTypeFunction(Entity("r")))))
  }


  @Test def shouldBeAbleToParseCoalesce() {
    testQuery(
      "start n=NODE(1) match n-[r]->(x) return coalesce(r.name, x.name)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
        returns(ExpressionReturnItem(CoalesceFunction(Property("r", "name"), Property("x", "name")))))
  }

  @Test def relationshipsFromPathOutput() {
    testQuery(
      "start n=NODE(1) match p=n-->x return relationships(p)",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        returns(ExpressionReturnItem(RelationshipFunction(Entity("p")))))
  }

  @Test def relationshipsFromPathInWhere() {
    testQuery(
      "start n=NODE(1) match p=n-->x where length(rels(p))=1 return p",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        where(Equals(LengthFunction(RelationshipFunction(Entity("p"))), Literal(1)))
        returns (ExpressionReturnItem(Entity("p"))))
  }

  @Test def countNonNullValues() {
    testQuery(
      "start a = NODE(1) return a, count(a)",
      Query.
        start(NodeById("a", 1)).
        aggregation(ValueAggregationItem(Count(Entity("a")))).
        columns("a", "count(a)").
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleIdBothInReturnAndWhere() {
    testQuery(
      "start a = NODE(1) where id(a) = 0 return id(a)",
      Query.
        start(NodeById("a", 1)).
        where(Equals(IdFunction(Entity("a")), Literal(0)))
        returns (ExpressionReturnItem(IdFunction(Entity("a")))))
  }

  @Test def shouldBeAbleToHandleStringLiteralsWithApostrophe() {
    testQuery(
      "start a = node:index(key = 'value') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleQuotationsInsideApostrophes() {
    testQuery(
      "start a = node:index(key = 'val\"ue') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("val\"ue"))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def simplePathExample() {
    testQuery(
      "start a = node(0) match p = ( a-->b ) return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false))).
        returns(ExpressionReturnItem(Entity("a"))))
  }

  @Test def threeStepsPath() {
    testQuery(
      "start a = node(0) match p = ( a-->b-->c ) return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p",
        RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
        RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false)
      ))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def pathsShouldBePossibleWithoutParenthesis() {
    testQuery(
      "start a = node(0) match p = a-->b return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def variableLengthPath() {
    testQuery("start a=node(0) match a -[:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ExpressionReturnItem(Entity("x")))
    )
  }

  @Test def variableLengthPathWithRelsIterable() {
    testQuery("start a=node(0) match a -[r:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), Some("knows"), Direction.OUTGOING, Some("r"), false)).
        returns(ExpressionReturnItem(Entity("x")))
    )
  }

  @Test def fixedVarLengthPath() {
    testQuery("start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(3), Some(3), None, Direction.OUTGOING, None,
        false)).
        returns(ExpressionReturnItem(Entity("x")))
    )
  }

  @Test def variableLengthPathWithoutMinDepth() {
    testQuery("start a=node(0) match a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ExpressionReturnItem(Entity("x")))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    testQuery("start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, Some("knows"), Direction.OUTGOING,
        Some("r"), false)).
        returns(ExpressionReturnItem(Entity("x")))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    testQuery("start a=node(0) match a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ExpressionReturnItem(Entity("x")))
    )
  }

  @Test def unboundVariableLengthPath() {
    testQuery("start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ExpressionReturnItem(Entity("x")))
    )
  }

  @Test def optionalRelationship() {
    testQuery(
      "start a = node(1) match a -[?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, true)).
        returns(ExpressionReturnItem(Entity("b"))))
  }

  @Test def optionalTypedRelationship() {
    testQuery(
      "start a = node(1) match a -[?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, true)).
        returns(ExpressionReturnItem(Entity("b"))))
  }

  @Test def optionalTypedAndNamedRelationship() {
    testQuery(
      "start a = node(1) match a -[r?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Some("KNOWS"), Direction.OUTGOING, true)).
        returns(ExpressionReturnItem(Entity("b"))))
  }

  @Test def optionalNamedRelationship() {
    testQuery(
      "start a = node(1) match a -[r?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", None, Direction.OUTGOING, true)).
        returns(ExpressionReturnItem(Entity("b"))))
  }

  @Test def testOnAllNodesInAPath() {
    testQuery(
      """start a = node(1) match p = a --> b --> c where ALL(n in NODES(p) where n.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        namedPaths(
        NamedPath("p",
          RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
          RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false))).
        where(AllInIterable(NodesFunction(Entity("p")), "n", Equals(Property("n", "name"), Literal("Andres"))))
        returns (ExpressionReturnItem(Entity("b"))))
  }

  @Test def extractNameFromAllNodes() {
    testQuery(
      """start a = node(1) match p = a --> b --> c return extract(n in nodes(p) : n.name)""",
      Query.
        start(NodeById("a", 1)).
        namedPaths(
        NamedPath("p",
          RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
          RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false))).
        returns(ExpressionReturnItem(ExtractFunction(NodesFunction(Entity("p")), "n", Property("n", "name")))))
  }


  @Test def testAny() {
    testQuery(
      """start a = node(1) where ANY(x in NODES(p) where x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(AnyInIterable(NodesFunction(Entity("p")), "x", Equals(Property("x", "name"), Literal("Andres"))))
        returns (ExpressionReturnItem(Entity("b"))))
  }

  @Test def testNone() {
    testQuery(
      """start a = node(1) where none(x in nodes(p) where x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(NoneInIterable(NodesFunction(Entity("p")), "x", Equals(Property("x", "name"), Literal("Andres"))))
        returns (ExpressionReturnItem(Entity("b"))))
  }

  @Test def testSingle() {
    testQuery(
      """start a = node(1) where single(x in NODES(p) WHERE x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(SingleInIterable(NodesFunction(Entity("p")), "x", Equals(Property("x", "name"),
        Literal("Andres"))))
        returns (ExpressionReturnItem(Entity("b"))))
  }

  @Test def testParamAsStartNode() {
    testQuery(
      """start pA = node({a}) return pA""",
      Query.
        start(NodeById("pA", Parameter("a"))).
        returns(ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testNumericParamNameAsStartNode() {
    testQuery(
      """start pA = node({0}) return pA""",
      Query.
        start(NodeById("pA", Parameter("0"))).
        returns(ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForWhereLiteral() {
    testQuery(
      """start pA = node(1) where pA.name = {name} return pA""",
      Query.
        start(NodeById("pA", 1)).
        where(Equals(Property("pA", "name"), Parameter("name")))
        returns (ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForIndexKey() {
    testQuery(
      """start pA = node:idx({key} = "Value") return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Parameter("key"), Literal("Value"))).
        returns(ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForIndexValue() {
    testQuery(
      """start pA = node:idx(key = {Value}) return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), Parameter("Value"))).
        returns(ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForIndexQuery() {
    testQuery(
      """start pA = node:idx({query}) return pA""",
      Query.
        start(NodeByIndexQuery("pA", "idx", Parameter("query"))).
        returns(ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForSkip() {
    testQuery(
      """start pA = node(0) return pA skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns (ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForLimit() {
    testQuery(
      """start pA = node(0) return pA limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns (ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForLimitAndSkip() {
    testQuery(
      """start pA = node(0) return pA skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ("stop")
        returns (ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testParamForRegex() {
    testQuery(
      """start pA = node(0) where pA.name =~ {regex} return pA""",
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(Property("pA", "name"), Parameter("regex")))
        returns (ExpressionReturnItem(Entity("pA"))))
  }

  @Test def testShortestPath() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", None, Direction.OUTGOING, Some(1), false, true, None))
        returns (ExpressionReturnItem(Entity("p"))))
  }

  @Test def testShortestPathWithMaxDepth() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", None, Direction.OUTGOING, Some(6), false, true, None)).
        returns(ExpressionReturnItem(Entity("p"))))
  }

  @Test def testShortestPathWithType() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", Some("KNOWS"), Direction.OUTGOING, Some(6), false, true, None)).
        returns(ExpressionReturnItem(Entity("p"))))
  }

  @Test def testShortestPathBiDirectional() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]-b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", None, Direction.BOTH, Some(6), false, true, None)).
        returns(ExpressionReturnItem(Entity("p"))))
  }

  @Test def testShortestPathOptional() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[?*..6]-b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", None, Direction.BOTH, Some(6), true, true, None)).
        returns(ExpressionReturnItem(Entity("p"))))
  }

  @Test def testAllShortestPath() {
    testQuery(
      """start a=node(0), b=node(1) match p = allShortestPaths( a-[*]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        matches(ShortestPath("p", "a", "b", None, Direction.OUTGOING, None, false, false, None)).
        returns(ExpressionReturnItem(Entity("p"))))
  }

  @Test def testForNull() {
    testQuery(
      """start a=node(0) where a is null return a""",
      Query.
        start(NodeById("a", 0)).
        where(IsNull(Entity("a")))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def testForNotNull() {
    testQuery(
      """start a=node(0) where a is not null return a""",
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(Entity("a"))))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def testCountDistinct() {
    testQuery(
      """start a=node(0) return count(distinct a)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(ValueAggregationItem(Distinct(Count(Entity("a")), Entity("a")))).
        columns("count(distinct a)")
        returns())
  }

  @Test def consoleModeParserShouldOutputNullableProperties() {
    val query = "start a = node(1) return a.name"
    val parser = new ConsoleCypherParser()
    val executionTree = parser.parse(query)

    assertEquals(
      Query.
        start(NodeById("a", 1)).
        returns(ExpressionReturnItem(Nullable(Property("a", "name")))),
      executionTree)
  }

  @Test def supportsHasRelationshipInTheWhereClause() {
    testQuery(
      """start a=node(0), b=node(1) where a-->b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Entity("a"), Entity("b"), Direction.OUTGOING, None))
        returns (ExpressionReturnItem(Entity("a"))))
  }


  @Test def supportsHasRelationshipWithoutDirectionInTheWhereClause() {
    testQuery(
      """start a=node(0), b=node(1) where a-[:KNOWS]-b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Entity("a"), Entity("b"), Direction.BOTH, Some("KNOWS")))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def supportsHasRelationshipWithoutDirectionInTheWhereClause2() {
    testQuery(
      """start a=node(0), b=node(1) where a--b return a""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        where(HasRelationshipTo(Entity("a"), Entity("b"), Direction.BOTH, None))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldSupportHasRelationshipToAnyNode() {
    testQuery(
      """start a=node(0) where a-->() return a""",
      Query.
        start(NodeById("a", 0)).
        where(HasRelationship(Entity("a"), Direction.OUTGOING, None))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldHandleLFAsWhiteSpace() {
    testQuery(
      "start\na=node(0)\nwhere\na-->()\nreturn\na",
      Query.
        start(NodeById("a", 0)).
        where(HasRelationship(Entity("a"), Direction.OUTGOING, None))
        returns (ExpressionReturnItem(Entity("a"))))
  }

  @Test def shouldBeAbleToParseThingsLikeIts15AllOverAgain() {
    testQuery(
      "cypher 1.5 start a = node(1) where ANY(x in NODES(p) : x.name = 'Andres') return b",
      Query.
        start(NodeById("a", 1)).
        where(AnyInIterable(NodesFunction(Entity("p")), "x", Equals(Property("x", "name"), Literal("Andres"))))
        returns (ExpressionReturnItem(Entity("b"))))
  }

  def testQuery(query: String, expectedQuery: Query) {
    val parser = new CypherParser()

    val ast = parser.parse(query)

    assert(expectedQuery === ast)
  }
}
