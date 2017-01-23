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

import java.lang
import java.lang.Boolean._

class paramsParserTest extends ParsingTestSupport {

  test("should parse null") {
    paramsParser("null") should equal(null)
  }

  test("should parse integer") {
    paramsParser("1") should equal(1L)
    paramsParser("112312") should equal(112312L)
    paramsParser("0") should equal(0L)
    paramsParser("-0") should equal(0L)
    paramsParser("-4") should equal(-4L)
  }

  test("should parse float") {
    paramsParser("1.0") should equal(1.0)
    paramsParser(".01") should equal(0.01)
    paramsParser("-.000000001") should equal(-1.0E-9)
  }

  test("should parse float in scientific format") {
    paramsParser("1.0e10") should equal(1e10)
    paramsParser("1.0e-10") should equal(1e-10)
    paramsParser(".0005e250") should equal(5e246)
    paramsParser("123456.7e15") should equal(1.234567E20)
  }

  test("should parse float special values") {
    paramsParser("Inf") should equal(Double.PositiveInfinity)
    paramsParser("-Inf") should equal(Double.NegativeInfinity)
    // TODO NaN -- pending implementing the final form in Neo4j
  }

  test("should parse boolean") {
    paramsParser("true") should equal(TRUE)
    paramsParser("false") should equal(FALSE)
  }

  test("should parse string") {
    Seq("", "string", " ", "s p a c e d ", "\n\r\f\t").foreach { s =>
      paramsParser(s"'$s'") should equal(s"$s")
    }
  }

  test("should parse escaped string delimiter") {
    paramsParser("''") should equal("")
    paramsParser("'\\''") should equal("'")
    paramsParser("'\\'\\''") should equal("''")
    paramsParser("'\\'hey\\''") should equal("'hey'")
    paramsParser("'\\'") should equal("\\")
  }

  test("should parse list") {
    paramsParser("[]") should equal(List.empty.asJava)
    paramsParser("['\"\n\r\f\t']") should equal(List("\"\n\r\f\t").asJava)
    paramsParser("[0, 1.0e-10, '$', true]") should equal(List(0L, 1e-10, "$", TRUE).asJava)
    paramsParser("['', ',', ' ', ', ', 'end']") should equal(List("", ",", " ", ", ", "end").asJava)
  }

  test("should parse nested list") {
    paramsParser("[[]]") should equal(List(List.empty.asJava).asJava)
    paramsParser("[[[0]], [0], 0]") should equal(List(List(List(0L).asJava).asJava, List(0L).asJava, 0L).asJava)
  }

  test("should parse maps") {
    paramsParser("{}") should equal(Map.empty.asJava)
    paramsParser("{k0:'\n\r\f\t'}") should equal(Map("k0" -> "\n\r\f\t").asJava)

    paramsParser("{k0:0, k1:1.0e-10, k2:null, k3:true}") should equal(
      Map("k0" -> java.lang.Long.valueOf(0), "k1" -> lang.Double.valueOf(1e-10), "k2" -> null, "k3" -> TRUE).asJava)
  }

  test("should parse nested maps") {
    paramsParser("{key: {key: 'value', key2: {}}, key2: []}") should equal(
      Map("key" -> Map("key" -> "value", "key2" -> Map.empty.asJava).asJava, "key2" -> List.empty.asJava).asJava)
  }

}
