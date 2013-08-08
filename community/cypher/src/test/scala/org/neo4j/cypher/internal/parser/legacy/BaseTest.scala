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
package org.neo4j.cypher.internal.parser.legacy

import org.neo4j.cypher.internal.parser.ParserTest
import org.junit.Test

class BaseTest extends Base with ParserTest {

  @Test
  def shouldParseLongest() {
    abstract class ABC
    case object TheA extends ABC
    case object TheB extends ABC
    case object TheC extends ABC


    {
      def parseA = "A" ^^^ TheA
      def parseBB = "BB" ^^^ TheB
      def parseC = "C" ^^^ TheC
      implicit val parseABC = longestOf("ABC", parseA, parseBB, parseC)

      // regular
      parsing("A") shouldGive TheA
      parsing("BB") shouldGive TheB
      parsing("C") shouldGive TheC

      // failures
      assertFails[ABC]("")
      assertFails[ABC]("D")
    }

    {
      def parseA1 = "AAA" ^^^ TheA
      def parseA2 = "A" ^^^ TheB

      implicit val parseAorB = longestOf("A+", parseA2, parseA1)

      // prefer first if multiple are matching
      partiallyParsing("A") shouldGive TheB
      partiallyParsing("AAA") shouldGive TheA
    }

    {
      def parseA = "A" ^^^ TheA
      def parseAasB = "A" ^^^ TheB

      implicit val parseAorB = longestOf("AB", parseAasB, parseA)

      // prefer first if multiple are matching
      parsing("A") shouldGive TheB
    }
  }
}