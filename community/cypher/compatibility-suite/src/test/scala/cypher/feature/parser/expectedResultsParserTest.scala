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
import org.scalatest.{FunSuite, Matchers}

class expectedResultsParserTest extends FunSuite with Matchers with ParsedEntities {

  test("should parse null") {
    parse("null") should equal(null)
  }

  test("should parse integer") {
    parse("1") should equal(1L)
    parse("112312") should equal(112312L)
    parse("0") should equal(0L)
    parse("-0") should equal(0L)
    parse("-4") should equal(-4L)
  }

  test("should parse float") {
    parse("1.0") should equal(1.0)
    parse(".01") should equal(0.01)
    parse("-.000000001") should equal(-1.0E-9)
  }

  test("should parse float in scientific format") {
    parse("1.0e10") should equal(1e10)
    parse("1.0e-10") should equal(1e-10)
    parse(".0005e250") should equal(5e246)
    parse("123456.7e15") should equal(1.234567E20)
  }

  test("should parse float special values") {
    parse("Inf") should equal(Double.PositiveInfinity)
    parse("-Inf") should equal(Double.NegativeInfinity)
    // TODO NaN -- pending implementing the final form in Neo4j
  }

  test("should parse boolean") {
    parse("true") should equal(TRUE)
    parse("false") should equal(FALSE)
  }

  test("should parse string") {
    Seq("", "string", " ", "s p a c e d ", "\n\r\f\t").foreach { s =>
      parse(s"'$s'") should equal(s"$s")
    }
  }

  test("should parse escaped string delimiter") {
    parse("''") should equal("")
    parse("'\\''") should equal("'")
    parse("'\\'\\''") should equal("''")
    parse("'\\'hey\\''") should equal("'hey'")
    parse("'\\'") should equal("\\")
  }

  test("should parse list") {
    parse("[]") should equal(emptyList())
    parse("['\"\n\r\f\t']") should equal(asList("\"\n\r\f\t"))
    parse("[0, 1.0e-10, '$', true]") should equal(asList(0L, 1e-10, "$", TRUE))
    parse("['', ',', ' ', ', ', 'end']") should equal(asList("", ",", " ", ", ", "end"))
  }

  test("should parse nested list") {
    parse("[[]]") should equal(asList(emptyList()))
    parse("[[[0]], [0], 0]") should equal(asList(asList(asList(0L)), asList(0L), 0L))
  }

  test("should parse maps") {
    parse("{}") should equal(emptyMap())
    parse("{k0:'\n\r\f\t'}") should equal(asMap("k0" -> "\n\r\f\t"))
    parse("{k0:0, k1:1.0e-10, k2:null, k3:true}") should equal(
      asMap("k0" -> Long.valueOf(0), "k1" -> lang.Double.valueOf(1e-10), "k2" -> null, "k3" -> TRUE))
  }

  test("should parse nodes") {
    parse("()") should equal(parsedNode())
    parse("(:T)") should equal(parsedNode(asList("T")))
    parse("(:T:T2:longlabel)") should equal(parsedNode(asList("T", "T2", "longlabel")))
  }

  test("should parse nodes with properties") {
    parse("({key:'value'})") should equal(parsedNode(properties = asMap("key" -> "value")))
    parse("({key:0})") should equal(parsedNode(properties = asMap("key" -> Long.valueOf(0L))))
    parse("({key:null, key2:[]})") should equal(parsedNode(properties = asMap("key" -> null, "key2" -> emptyList())))
  }

  def asMap(tuples: (String, AnyRef)*): util.Map[String, AnyRef] = {
    val map = new util.HashMap[String, AnyRef]()
    tuples.foreach {
      case (k, v) => map.put(k, v)
    }
    map
  }

  private def parse(value: String) = {
    scalaResultsParser(value)
  }

}
