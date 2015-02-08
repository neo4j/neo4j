/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.prettifier

import org.junit.Test
import org.scalatest.Assertions
import org.junit.Assert.assertEquals

class PrettifierTest extends Assertions {
  @Test
  def shouldUpcaseKeywords() {
    assertIsPrettified("CREATE n", "create n")
  }

  @Test
  def shouldNotBreakIndexOn() {
    assertIsPrettified("CREATE INDEX ON :Person(name)", "create index on :Person(name)")
  }

  @Test
  def shouldNotBreakOnAsc() {
    assertIsPrettified("ORDER BY n.name ASC", "order by n.name asc")
  }

  @Test
  def shouldNotBreakCreateInForeach() {
    assertIsPrettified("MATCH p=n%nFOREACH (x IN p | CREATE x--())", "match p=n foreach(x in p | create x--())")
  }

  @Test
  def shouldNotBreakCreateInComplexForeach() {
    assertIsPrettified("MATCH p=n%nFOREACH (x IN p | CREATE x--()%nSET x.foo = 'bar')%nRETURN DISTINCT p;",
      "match p=n foreach(x in p | create x--() set x.foo = 'bar') return distinct p;")
  }

  @Test
  def shouldNotBreakConstraintOn() {
    assertIsPrettified("CREATE CONSTRAINT ON (person:Person) ASSERT person.age IS UNIQUE",
      "create constraint on (person:Person) assert person.age is unique")
  }

  @Test
  def shouldBreakCertainKeywords() {
    assertIsPrettified("MERGE n%nON CREATE SET n.age=32", "merge n on create set n.age=32")
  }

  @Test
  def shouldHandleParenthesisCorrectlyInMatch() {
    assertIsPrettified("MATCH (a)-->(b)%nRETURN b", "match (a)-->(b) return b")
  }

  @Test
  def shouldUpcaseMultipleKeywords() {
    assertIsPrettified("MATCH n%nWHERE n.name='B'%nRETURN n", "match n where n.name='B' return n")
  }

  @Test
  def shouldUpcaseMultipleKeywords2() {
    assertIsPrettified("MATCH a%nWHERE a.name='A'%nRETURN a.age AS SomethingTotallyDifferent",
      "match a where a.name='A' return a.age as SomethingTotallyDifferent")
  }

  @Test
  def shouldNotBreakCreateUnique() {
    assertIsPrettified(
      "START me=node(3)%n" +
        "MATCH p1 = me-[*2]-friendOfFriend%n" +
        "CREATE p2 = me-[:MARRIED_TO]-(wife { name: \"Gunhild\" })%n" +
        "CREATE UNIQUE p3 = wife-[:KNOWS]-friendOfFriend%n" +
        "RETURN p1,p2,p3",
      "start me=node(3) match p1 = me-[*2]-friendOfFriend create p2 = me-[:MARRIED_TO]-(wife { name: \"Gunhild\" }) create unique p3 = wife-[:KNOWS]-friendOfFriend return p1,p2,p3")
  }

  @Test
  def shouldNotBreakWhereInComprehensions() {
    assertIsPrettified(
      "RETURN [x IN range(0,10) WHERE x + 2 = 0 | x^3] AS result",
      "return [x in range(0,10) where x + 2 = 0 | x^3] as result")
  }

  @Test
  def shouldUpcaseExtraKeywords() {
    assertIsPrettified("MATCH david--otherPerson-->()%nWHERE david.name='David'%nWITH otherPerson, count(*) AS foaf%nWHERE foaf > 1%nRETURN otherPerson",
      "match david--otherPerson-->() where david.name='David' with otherPerson, count(*) as foaf where foaf > 1 return otherPerson")
  }

  @Test
  def shouldNotBreakAfterOptional() {
    assertIsPrettified(
      "OPTIONAL MATCH (n)-->(x)%nRETURN n, x",
      "optional MATCH (n)-->(x) return n, x")
  }

  @Test
  def shouldPrettifyLoadCsv() {
    assertIsPrettified("LOAD CSV FROM \"f\" AS line", "LOAD CSV FROM \"f\" AS line")
  }

  @Test
  def shouldPrettifyLoadCsvWithHeaders() {
    assertIsPrettified("LOAD CSV WITH HEADERS FROM \"f\" AS line", "LOAD CSV wiTh HEADERS FROM \"f\" AS line")
  }

  @Test
  def shouldPrettifyAndBreakLoadCsv() {
    assertIsPrettified("MATCH (n)%nLOAD CSV FROM \"f\" AS line%nRETURN (n)", "MATCH (n) LOAD CSV FROM \"f\" AS line return (n)")
  }

  @Test
  def shouldPrettifyWithCorrectStringQuotes() {
    assertIsPrettified(
      "MATCH a%nWHERE a.name='A'%nRETURN a.age > 30, \"I'm a literal\", a-->()",
      "mATCH a WhERE a.name='A' RETURN a.age > 30, \"I'm a literal\", a-->()")
  }
  private def assertIsPrettified(expected: String, query: String) {
    assertEquals(String.format(expected), Prettifier(query))
  }
}

