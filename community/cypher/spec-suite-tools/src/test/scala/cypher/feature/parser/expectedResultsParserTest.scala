/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher.feature.parser

import java.lang.Boolean.{FALSE, TRUE}
import java.lang.Long
import java.util.Collections.emptyList
import java.{lang, util}

import org.neo4j.graphdb.Relationship

class expectedResultsParserTest extends ParsingTestSupport {

  test("should parse null") {
    val matcher: matchers.Matcher[AnyRef] = parse("null")
    matcher should accept(null)
  }

  test("should parse integer") {
    parse("1") should accept(1L)
    parse("112312") should accept(112312L)
    parse("0") should accept(0L)
    parse("-0") should accept(0L)
    parse("-4") should accept(-4L)
  }

  test("should parse float") {
    parse("1.0") should accept(1.0)
    parse(".01") should accept(0.01)
    parse("-.000000001") should accept(-1.0E-9)
  }

  test("should parse float in scientific format") {
    parse("1.0e10") should accept(1e10)
    parse("1.0e-10") should accept(1e-10)
    parse(".0005e250") should accept(5e246)
    parse("123456.7e15") should accept(1.234567E20)
  }

  test("should parse float special values") {
    parse("Inf") should accept(Double.PositiveInfinity)
    parse("-Inf") should accept(Double.NegativeInfinity)
    // TODO NaN -- pending implementing the final form in Neo4j
  }

  test("should parse boolean") {
    parse("true") should accept(TRUE)
    parse("false") should accept(FALSE)
  }

  test("should parse string") {
    Seq("", "string", " ", "s p a c e d ", "\n\r\f\t").foreach { s =>
      parse(s"'$s'") should accept(s"$s")
    }
  }

  test("should parse escaped string delimiter") {
    parse("''") should accept("")
    parse("'\\''") should accept("'")
    parse("'\\'\\''") should accept("''")
    parse("'\\'hey\\''") should accept("'hey'")
    parse("'\\'") should accept("\\")
  }

  test("should parse list") {
    parse("[]") should accept(List.empty.asJava)
    parse("['\"\n\r\f\t']") should accept(List("\"\n\r\f\t").asJava)
    parse("[0, 1.0e-10, '$', true]") should accept(List(0L, 1e-10, "$", TRUE).asJava)
    parse("['', ',', ' ', ', ', 'end']") should accept(List("", ",", " ", ", ", "end").asJava)
  }

  test("should parse nested list") {
    parse("[[]]") should accept(List(List.empty.asJava).asJava)
    parse("[[[0]], [0], 0]") should accept(List(List(List(0L).asJava).asJava, List(0L).asJava, 0L).asJava)
  }

  test("should parse and match unordered lists") {
    val matcher = parse("[null, 0, true]", unorderedLists = true)
    matcher should accept(List(null, 0L, TRUE).asJava)
    matcher should accept(List(null, TRUE, 0L).asJava)
    matcher should accept(List(0L, null, TRUE).asJava)
    matcher should accept(List(0L, TRUE, null).asJava)
    matcher should accept(List(TRUE, null, 0L).asJava)
    matcher should accept(List(TRUE, 0L, null).asJava)
  }

  test("should parse maps") {
    parse("{}") should accept(Map.empty.asJava)
    parse("{k0:'\n\r\f\t'}") should accept(Map("k0" -> "\n\r\f\t").asJava)

    parse("{k0:0, k1:1.0e-10, k2:null, k3:true}") should accept(
      Map("k0" -> Long.valueOf(0), "k1" -> lang.Double.valueOf(1e-10), "k2" -> null, "k3" -> TRUE).asJava)
  }

  test("should parse nested maps") {
    parse("{key: {key: 'value', key2: {}}, key2: []}") should accept(
      Map("key" -> Map("key" -> "value", "key2" -> Map.empty.asJava).asJava, "key2" -> List.empty.asJava).asJava)
  }

  test("should allow whitespace between key and value") {
    parse("{key:'value'}") should accept(Map("key" -> "value").asJava)
    parse("{key: 'value'}") should accept(Map("key" -> "value").asJava)
  }

  test("should parse nodes with labels") {
    parse("()") should accept(node())
    parse("(:T)") should accept(node(Seq("T")))
    parse("(:T:T2:longlabel)") should accept(node(Seq("T", "T2", "longlabel")))
  }

  test("should parse nodes with properties") {
    parse("({key:'value'})") should accept(node(properties = Map("key" -> "value")))
    parse("({key:0})") should accept(node(properties = Map("key" -> Long.valueOf(0L))))
    parse("({key:null, key2:[]})") should accept(node(properties = Map("key" -> null, "key2" -> emptyList())))
  }

  test("should parse nodes with labels and properties") {
    parse("(:T {k:[]})") should accept(node(Seq("T"), Map("k" -> emptyList())))
    val expected = node(Seq("T", "longlabel"),
                        Map("k" -> emptyList(), "verylongkeywithonlyletters" -> lang.Double.valueOf("Infinity")))
    parse("(:T:longlabel {k:[], verylongkeywithonlyletters:Inf})") should accept(expected)
  }

  test("should parse relationships") {
    parse("[:T]") should accept(relationship("T"))
    parse("[:T {k:0}]") should accept(relationship("T", Map("k" -> Long.valueOf(0L))))
  }

  test("should parse the zero-length path") {
    parse("<()>") should accept(singleNodePath(node()))

    val path = singleNodePath(node(Seq("Person", "Director"), Map("key" -> "value")))
    parse("<(:Person:Director {key: 'value'})>") should accept(path)
  }

  test("should parse simple outgoing path") {
    val startNode = node(Seq("Start"))
    val endNode = node(Seq("End"))

    parse("<(:Start)-[:T]->(:End)>") should accept(path(pathLink(startNode, relationship("T"), endNode)))
  }

  test("should parse simple incoming path") {
    val startNode = node(Seq("Start"))
    val endNode = node(Seq("End"))

    parse("<(:End)<-[:T]-(:Start)>") should accept(path(pathLink(startNode, relationship("T"), endNode)))
  }

  test("should parse path with mixed directions") {
    val middle = node()
    val link1: Relationship = pathLink(node(Seq("S")), relationship("R1"), middle)
    val link2: Relationship = pathLink(node(Seq("E")), relationship("R2"), middle)

    parse("<(:S)-[:R1]->()<-[:R2]-(:E)>") should accept(path(link1, link2))
  }

  test("should parse path with more complex elements") {
    val value = "<(:T {k:0})-[:T {k:'s'}]->({k:true})-[:type]->()>"

    val middle = node(properties = Map("k" -> TRUE))
    val link1 = pathLink(node(Seq("T"), Map("k" -> java.lang.Long.valueOf(0))), relationship("T", Map("k" -> "s")),
                         middle)
    val link2 = pathLink(middle, relationship("type"), node())

    parse(value) should accept(path(link1, link2))
  }

  private def parse(value: String, unorderedLists: Boolean = false): cypher.feature.parser.matchers.Matcher[AnyRef] = {
    matcherParser(value, unorderedLists)
  }

  private def asMap(scalaMap: Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val map = new util.HashMap[String, AnyRef]()
    scalaMap.foreach {
      case (k, v) => map.put(k, v)
    }
    map
  }

}
