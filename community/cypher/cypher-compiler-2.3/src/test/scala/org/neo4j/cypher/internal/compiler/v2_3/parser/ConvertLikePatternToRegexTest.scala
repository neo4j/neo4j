/*
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
package org.neo4j.cypher.internal.compiler.v2_3.parser

import java.util.regex.Pattern.quote

import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite


class ConvertLikePatternToRegexTest extends CypherFunSuite {

  import org.scalatest.prop.TableDrivenPropertyChecks._

  val tests = Table(
   "LIKE Pattern" -> "Regular Expression",

    "%" -> ".*",

    "_" -> ".",

    "A%" -> s"${quote("A")}.*",

    "A_B" -> s"${quote("A")}.${quote("B")}",

    ".*" -> quote(".*"),

    "[Ab]" -> quote("[Ab]"),

    "_._" -> s".${quote(".")}.",

    "\n" -> s"${quote("\n")}",

    "\\n" -> s"${quote("n")}",

    "\\\n" -> s"${quote("\n")}",

    /* middle part is: \\Q.\\E */
    s"_${quote(".").replace("\\", "\\\\")}_" -> s".${quote(quote("."))}."
  )

  test("converts like pattern to regex") {
    forAll(tests) { (like, regexp) =>
      val parsedLikePattern = LikePatternParser(like)
      val likeRegex = convertLikePatternToRegex(parsedLikePattern)
      likeRegex should equal(regexp)
    }
  }
}
