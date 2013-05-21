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
package org.neo4j.cypher.internal.parser.prettifier

import org.junit.Test
import org.scalatest.Assertions
import org.junit.Assert.assertEquals

class PrettifierTest extends Assertions {
  val prettifier = Prettifier

  @Test
  def shouldUpcaseKeywords() {
    assertEquals( "CREATE n", prettifier("create n") )
  }

  @Test
  def shouldNotBreakIndexOn() {
    assertEquals( "CREATE INDEX ON :Person(name)", prettifier("create index on :Person(name)") )
  }

  @Test
  def shouldNotBreakOnAsc() {
    assertEquals( "ORDER BY n.name ASC", prettifier("order by n.name asc") )
  }

  @Test
  def shouldNotBreakCreateInForeach() {
    assertEquals( "MATCH p=n\nFOREACH (x IN p : CREATE x--())", prettifier("match p=n foreach(x in p : create x--())") )
  }

  @Test
  def shouldNotBreakCreateInComplexForeach() {
    assertEquals( "MATCH p=n\nFOREACH (x IN p : CREATE x--()\nSET x.foo = 'bar')\nRETURN p;", prettifier("match p=n foreach(x in p : create x--() set x.foo = 'bar') return p;") )
  }

  @Test
  def shouldNotBreakConstraintOn() {
    assertEquals( "CREATE CONSTRAINT ON (person:Person)\nASSERT person.age IS UNIQUE", prettifier("create constraint on (person:Person) assert person.age is unique") )
  }

  @Test
  def shouldBreakCertainKeywords() {
    assertEquals( "MERGE n\nON CREATE SET n.age=32", prettifier("merge n on create set n.age=32") )
  }

  @Test
  def shouldHandleParenthesisCorrectlyInMatch() {
    assertEquals( "MATCH (a)-->(b)\nRETURN b", prettifier("match (a)-->(b) return b") )
  }

  @Test
  def shouldUpcaseMultipleKeywords() {
    assertEquals( "MATCH n\nWHERE n.name='B'\nRETURN n", prettifier("match n where n.name='B' return n") )
  }

  @Test
  def shouldUpcaseMultipleKeywords2() {
    assertEquals( "MATCH a\nWHERE a.name='A'\nRETURN a.age AS SomethingTotallyDifferent", prettifier("match a where a.name='A' return a.age AS SomethingTotallyDifferent") )
  }

  @Test
  def shouldNotBreakCreateUnique() {
    assertEquals( "START me=node(3)\nMATCH p1 = me-[*2]-friendOfFriend\nCREATE p2 = me-[:MARRIED_TO]-(wife { name: 'Gunhild' })\nCREATE UNIQUE p3 = wife-[:KNOWS]-friendOfFriend\nRETURN p1,p2,p3", prettifier("start me=node(3) match p1 = me-[*2]-friendOfFriend create p2 = me-[:MARRIED_TO]-(wife { name: \"Gunhild\" }) create unique p3 = wife-[:KNOWS]-friendOfFriend return p1,p2,p3") )
  }

  @Test
  def shouldUpcaseExtraKeywords() {
    assertEquals("MATCH david--otherPerson-->()\nWHERE david.name='David'\nWITH otherPerson, count(*) AS foaf\nWHERE foaf > 1\nRETURN otherPerson", prettifier("match david--otherPerson-->() where david.name='David' with otherPerson, count(*) as foaf where foaf > 1 return otherPerson"))
  }
}

