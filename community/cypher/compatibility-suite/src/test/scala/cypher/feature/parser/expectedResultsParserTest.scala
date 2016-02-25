/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Arrays.asList
import java.util.Collections.{emptyList, emptyMap}
import java.{lang, util}

import cypher.feature.parser.matchers.ValueMatcher
import org.neo4j.graphdb._
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FunSuite, Matchers}

class expectedResultsParserTest extends FunSuite with Matchers {

  test("should parse null") {
    valueParse("null") should equal(null)
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
    valueParse("[]") should equal(emptyList())
    valueParse("['\"\n\r\f\t']") should equal(asList("\"\n\r\f\t"))
    valueParse("[0, 1.0e-10, '$', true]") should equal(asList(0L, 1e-10, "$", TRUE))
    valueParse("['', ',', ' ', ', ', 'end']") should equal(asList("", ",", " ", ", ", "end"))
  }

  test("should parse nested list") {
    valueParse("[[]]") should equal(asList(emptyList()))
    valueParse("[[[0]], [0], 0]") should equal(asList(asList(asList(0L)), asList(0L), 0L))
  }

  test("should parse maps") {
    valueParse("{}") should equal(emptyMap())
    valueParse("{k0:'\n\r\f\t'}") should equal(asMap(Map("k0" -> "\n\r\f\t")))
    valueParse("{k0:0, k1:1.0e-10, k2:null, k3:true}") should equal(
      asMap(Map("k0" -> Long.valueOf(0), "k1" -> lang.Double.valueOf(1e-10), "k2" -> null, "k3" -> TRUE)))
  }

  test("should allow whitespace between key and value") {
    valueParse("{key:'value'}") should equal(valueParse("{key: 'value'}"))
  }

  test("should parse nodes with labels") {
    valueParse("()") should equal(parsedNode())
    valueParse("(:T)") should equal(parsedNode(Seq("T")))
    valueParse("(:T:T2:longlabel)") should equal(parsedNode(Seq("T", "T2", "longlabel")))
  }

  test("should parse nodes with properties") {
    valueParse("({key:'value'})") should equal(parsedNode(properties = Map("key" -> "value")))
    valueParse("({key:0})") should equal(parsedNode(properties = Map("key" -> Long.valueOf(0L))))
    valueParse("({key:null, key2:[]})") should equal(parsedNode(properties = Map("key" -> null, "key2" -> emptyList())))
  }

  test("should parse nodes with labels and properties") {
    valueParse("(:T {k:[]})") should equal(parsedNode(Seq("T"), Map("k" -> emptyList())))
    val expected = parsedNode(Seq("T", "longlabel"),
                              Map("k" -> emptyList(), "verylongkeywithonlyletters" -> lang.Double.valueOf("Infinity")))
    valueParse("(:T:longlabel {k:[], verylongkeywithonlyletters:Inf})") should equal(expected)
  }

  test("should parse relationships") {
    valueParse("[:T]") should equal(parsedRelationship("T"))
    valueParse("[:T {k:0}]") should equal(parsedRelationship("T", Map("k" -> Long.valueOf(0L))))
  }

  //  test("should parse the zero-length path") {
  //    parse("<()>") should equal(parsedPath(Seq(parsedNode())))
  //  }
  //
  //  test("should parse simple paths") {
  //    val startNode = parsedNode(Seq("Start"))
  //    val endNode = parsedNode(Seq("End"))
  //
  //    parse("<(:Start)-[:T]->(:End)>") should equal(parsedPath(Seq(startNode, endNode), Seq(parsedRelationship("T", startNode = Some(startNode), endNode = Some(endNode)))))
  //    parse("<(:End)<-[:T]-(:Start)>") should equal(parsedPath(Seq(endNode, startNode), Seq(parsedRelationship("T", startNode = Some(startNode), endNode = Some(endNode)))))
  //  }
  //
  //  test("should parse paths with mixed directions") {
  //    val s = parsedNode(Seq("S"))
  //    val middle = parsedNode()
  //    val e = parsedNode(Seq("E"))
  //    parse("<(:S)-[:R1]->()<-[:R2]-(:E)>") should equal(parsedPath(Seq(s, middle, e), Seq(parsedRelationship("R1", startNode = Some(s), endNode = Some(middle)), parsedRelationship("R2", startNode = Some(middle), endNode = Some(e)))))
  //  }
  //
  //  test("should parse path with more complex elements") {
  //    val value = "<(:T {k:0})-[:T {k:'s'}]->({k:true})-[:type]->()>"
  //
  //    val nodes = Seq(parsedNode(Seq("T"), Map("k" -> Long.valueOf(0))), parsedNode(properties = Map("k" -> TRUE)), parsedNode())
  //    val relationships = Seq(parsedRelationship("T", Map("k" -> "s")), parsedRelationship("type"))
  //
  //    parse(value) should equal(parsedPath(nodes, relationships))
  //  }

  private def valueParse(value: String) = {
    scalaResultsParser(value)
  }

  private def parse(value: String): ValueMatcher = {
    matcherParser(value)
  }

  private def parsedRelationship(typ: String, properties: Map[String, AnyRef] = Map.empty): Relationship = {
    ParsedRelationship.parsedRelationship(RelationshipType.withName(typ), asMap(properties))
  }

  private def parsedNode(labels: Seq[String] = Seq.empty, properties: Map[String, AnyRef] = Map.empty): Node = {
    val list = new util.ArrayList[Label]()
    labels.foreach(name => list.add(Label.label(name)))

    ParsedNode.parsedNode(list, asMap(properties))
  }

  private def parsedPath(nodes: Seq[Node] = Seq.empty, rels: Seq[Relationship] = Seq.empty): Path = {
    //    ParsedPath.parsedPath(nodes.toIterable.asJava, rels.toIterable.asJava)
    ???
  }

  private def asMap(scalaMap: Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val map = new util.HashMap[String, AnyRef]()
    scalaMap.foreach {
      case (k, v) => map.put(k, v)
    }
    map
  }


  case class accept(expected: Any)
    extends Matcher[ValueMatcher] {

    override def apply(actual: ValueMatcher): MatchResult = {
      MatchResult(matches = actual.matches(expected),
                  s"Mismatch! Expected value $actual did not match $expected",
                  s"No mismatch found; $actual unexpectedly matched $expected")
    }
  }

}
