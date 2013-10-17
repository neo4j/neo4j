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
package org.neo4j.cypher.internal.parser.v2_0

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers.equalTo
import org.neo4j.cypher.{CypherException, ExecutionEngineHelper}

class SemanticErrorTest extends ExecutionEngineHelper with Assertions {
  @Test def returnNodeThatsNotThere() {
    test(
      "start x=node(0) return bar",
      "bar not defined (line 1, column 24)"
    )
  }

  @Test def defineNodeAndTreatItAsARelationship() {
    test(
      "start r=node(0) match a-[r]->b return r",
      "Type mismatch: r already defined with conflicting type Node (expected Relationship) (line 1, column 26)"
    )
  }

  @Test def redefineSymbolInMatch() {
    test(
      "start a=node(0) match a-[r]->b-->r return r",
      "Type mismatch: r already defined with conflicting type Relationship (expected Node) (line 1, column 34)"
    )
  }

  @Test def cantUseTYPEOnNodes() {
    test(
      "start r=node(0) return type(r)",
      "Type mismatch: r already defined with conflicting type Node (expected Relationship) (line 1, column 29)"
    )
  }

  @Test def cantUseLENGTHOnNodes() {
    test(
      "start n=node(0) return length(n)",
      "Type mismatch: n already defined with conflicting type Node (expected Collection<Any>) (line 1, column 31)"
    )
  }

  @Test def cantReUseRelationshipIdentifier() {
    test(
      "start a=node(0) match a-[r]->b-[r]->a return r",
      "Can't re-use pattern relationship 'r' with different start/end nodes."
    )
  }

  @Test def shouldKnowNotToCompareStringsAndNumbers() {
    test(
      "start a=node(0) where a.age =~ 13 return a",
      "Type mismatch: expected String but was Long (line 1, column 32)"
    )
  }

  @Test def shouldComplainAboutUsingNotWithANonBoolean() {
    test(
      "RETURN NOT 'foo'",
      "Type mismatch: expected Boolean but was String (line 1, column 12)"
    )
  }

  @Test def shouldComplainAboutUnknownIdentifier() {
    test(
      "start s = node(1) where s.name = Name and s.age = 10 return s",
      "Name not defined (line 1, column 34)"
    )
  }

  @Test def shouldComplainIfVarLengthRelInCreate() {
    test(
      "create (a)-[:FOO*2]->(b)",
      "Variable length relationships cannot be specified in this context (line 1, column 11)"
    )
  }

  @Test def shouldComplainIfShortestPathHasNoRelationship() {
    test(
      "start n=node(0) match p=shortestPath(n) return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 25)"
    )
    test(
      "start n=node(0) match p=allShortestPaths(n) return p",
      "allShortestPaths(...) requires a pattern containing a single relationship (line 1, column 25)"
    )
  }

  @Test def shouldComplainIfShortestPathHasMultipleRelationships() {
    test(
      "start a=node(0), b=node(1) match p=shortestPath(a--()--b) return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 36)"
    )
    test(
      "start a=node(0), b=node(1) match p=allShortestPaths(a--()--b) return p",
      "allShortestPaths(...) requires a pattern containing a single relationship (line 1, column 36)"
    )
  }

  @Test def shouldComplainIfShortestPathHasAMinimalLength() {
    test(
      "start a=node(0), b=node(1) match p=shortestPath(a-[*1..2]->b) return p",
      "shortestPath(...) does not support a minimal length (line 1, column 36)"
    )
    test(
      "start a=node(0), b=node(1) match p=allShortestPaths(a-[*1..2]->b) return p",
      "allShortestPaths(...) does not support a minimal length (line 1, column 36)"
    )
  }

  @Test def shortestPathNeedsBothEndNodes() {
    test(
      "start a=node(0) match p=shortestPath(a-->b) return p",
      "Unknown identifier `b`"
    )
  }

  @Test def shouldBeSemanticallyIncorrectToReferToUnknownIdentifierInCreateConstraint() {
    test(
      "create constraint on (foo:Foo) bar.name is unique",
      "Unknown identifier `bar`, was expecting `foo`"
    )
  }

  @Test def shouldBeSemanticallyIncorrectToReferToUnknownIdentifierInDropConstraint() {
    test(
      "drop constraint on (foo:Foo) bar.name is unique",
      "Unknown identifier `bar`, was expecting `foo`"
    )
  }

  @Test def shouldFailTypeCheckWhenDeleting() {
    test(
      "start a=node(0) delete 1 + 1",
      "Type mismatch: expected Node, Relationship or Collection<Map> but was Long (line 1, column 26)"
    )
  }

  @Test def shouldNotAllowIdentifierToBeOverwrittenByCreate() {
    test(
      "start a=node(0) create (a)",
      "a already declared (line 1, column 25)"
    )
  }

  @Test def shouldNotAllowIdentifierToBeOverwrittenByMerge() {
    test(
      "start a=node(0) merge (a)",
      "a already declared (line 1, column 24)"
    )
  }

  @Test def shouldNotAllowIdentifierToBeOverwrittenByCreateRelationship() {
    test(
      "start a=node(0), r=rel(1) create (a)-[r:TYP]->()",
      "r already declared (line 1, column 39)"
    )
  }

  @Test def shouldFailWhenTryingToCreateShortestPaths() {
    test(
      "match a, b create shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to CREATE (line 1, column 19)"
    )
    test(
      "match a, b create allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 19)"
    )
  }

  @Test def shouldFailWhenTryingToUniquelyCreateShortestPaths() {
    test(
      "match a, b create unique shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to CREATE (line 1, column 26)"
    )
    test(
      "match a, b create unique allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 26)"
    )
  }

  @Test def shouldFailWhenReduceUsedWithWrongSeparator() {
    test("""
        |START s=node(1), e=node(2)
        |MATCH topRoute = (s)<-[:CONNECTED_TO*1..3]-(e)
        |RETURN reduce(weight=0, r in relationships(topRoute) : weight+r.cost) AS score
        |ORDER BY score ASC LIMIT 1
      """.stripMargin,
      "reduce(...) requires '| expression' (an accumulation expression) (line 4, column 8)"
    )
  }

  @Test def shouldFailIfOldIterableSeparator() {
    test(
      "start a=node(0) return filter(x in a.collection : x.prop = 1)",
      "filter(...) requires a WHERE predicate (line 1, column 24)"
    )

    test(
      "start a=node(0) return extract(x in a.collection : x.prop)",
      "extract(...) requires '| expression' (an extract expression) (line 1, column 24)"
    )

    test(
      "start a=node(0) return reduce(i = 0, x in a.collection : i + x.prop)",
      "reduce(...) requires '| expression' (an accumulation expression) (line 1, column 24)"
    )

    test(
      "start a=node(0) return any(x in a.collection : x.prop = 1)",
      "any(...) requires a WHERE predicate (line 1, column 24)"
    )

    test(
      "start a=node(0) return all(x in a.collection : x.prop = 1)",
      "all(...) requires a WHERE predicate (line 1, column 24)"
    )

    test(
      "start a=node(0) return single(x in a.collection : x.prop = 1)",
      "single(...) requires a WHERE predicate (line 1, column 24)"
    )

    test(
      "start a=node(0) return none(x in a.collection : x.prop = 1)",
      "none(...) requires a WHERE predicate (line 1, column 24)"
    )
  }

  @Test def shouldFailIfUsingAnHintWithAnUnknownIdentifier() {
    test(
      "match (n:Person)-->() using index m:Person(name) where n.name = \"kabam\" return n",
      "m not defined (line 1, column 35)"
    )
  }

  @Test def shouldFailIfNoParensAroundNode() {
    test(
      "match n:Person return n",
      "Parenthesis are required to identify nodes in patterns (line 1, column 7)"
    )
    test(
      "match n {foo: 'bar'} return n",
      "Parenthesis are required to identify nodes in patterns (line 1, column 7)"
    )
  }

  @Test def shouldFailIfUnknownIdentifierInMergeActions() {
    test(
      "MERGE (n:Person) ON CREATE x SET x.foo = 1",
      "x not defined (line 1, column 28)"
    )
    test(
      "MERGE (n:Person) ON MATCH x SET x.foo = 1",
      "x not defined (line 1, column 27)"
    )
  }

  @Test def shouldFailIfMergeActionIdentifierNotIntroducedInMerge() {
    test(
      "MATCH (n) MERGE (m) ON CREATE n SET n.foo = 1",
      "Invalid use of n for ON CREATE: already defined prior to MERGE (line 1, column 31)"
    )
  }

  @Test def shouldFailIfMergeActionUsesPathIdentifier() {
    test(
      "MERGE p=(n:Person) ON CREATE p SET n.foo = 1",
      "Type mismatch: p already defined with conflicting type Collection<Map> (expected Node or Relationship) (line 1, column 30)"
    )
  }

  @Test def shouldFailIfUnknownIdentifierInMergeActionSetClause() {
    test(
      "MERGE (n:Person) ON CREATE n SET x.foo = 1",
      "x not defined (line 1, column 34)"
    )
    test(
      "MERGE (n:Person) ON MATCH n SET x.foo = 1",
      "x not defined (line 1, column 33)")
  }

  @Test def shouldFailIfUsingLegacyOptionalsMatch() {
    test(
      "start n = node(0) match (n)-[?]->(m) return n",
      "Question mark is not used for optional patterns any more. Please use OPTIONAL MATCH instead. (line 1, column 28)"
    )
  }

  @Test def shouldFailIfUsingLegacyOptionalsMatch2() {
    test(
      "start n = node(0) match (n)-[?*]->(m) return n",
      "Question mark is not used for optional patterns any more. Please use OPTIONAL MATCH instead. (line 1, column 28)"
    )
  }

  @Test def shouldFailIfUsingLegacyOptionalsMatch3() {
    test(
      "start n = node(0) match shortestPath((n)-[?*]->(m)) return n",
      "Question mark is not used for optional patterns any more. Please use OPTIONAL MATCH instead. (line 1, column 41)"
    )
  }

  def test(query: String, message: String) {
    try {
      val result = execute(query)
      result.toList
      fail(s"Did not get the expected syntax error, expected: $message")
    } catch {
      case x: CypherException => {
        val actual = x.getMessage.lines.next().trim
        assertThat(actual, equalTo(message))
      }
    }
  }
}
