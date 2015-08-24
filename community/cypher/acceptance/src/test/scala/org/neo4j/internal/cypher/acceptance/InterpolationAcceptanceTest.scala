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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}

class InterpolationAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  ignore("this doesn't seem to work") {
    val query = "RETURN $'${''}' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "")))

  }

  test("should interpolate simple strings") {
    val query = "RETURN $'string' AS s"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("s" -> "string")))
  }

  test("should interpolate simple expression") {
    val query = "RETURN $'${1 + 3}' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "4")))
  }

  test("should interpolate simple expression with weird whitespaces") {
    val query = "RETURN $'${   1+ 3 }' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "4")))
  }

  test("should interpolate to null for nulls") {
    val query = "RETURN $'${1 + null}' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> null)))
  }

  test("should interpolate an identifier") {
    val query = "WITH 1 AS n RETURN $'${n}' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "1")))
  }

  test("should interpolate from inside toString") {
    val query = "RETURN toString($'') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "")))
  }

  test("should interpolate from inside str") {
    val query = "RETURN str($'val') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "\"val\"")))
  }

  test("should interpolate from inside replace first") {
    val query = "RETURN replace($'original', 'gi', '') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "orinal")))
  }

  test("should interpolate from inside replace second") {
    val query = "RETURN replace('original', $'i', 'k') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "orkgknal")))
  }

  test("should interpolate from inside replace third") {
    val query = "RETURN replace('original', 'or', $'$$') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "$iginal")))
  }

  test("should interpolate from inside substring") {
    val query = "RETURN substring($'o${123}', 3) AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "3")))
  }

  test("should interpolate from inside left") {
    val query = "RETURN left($'o${123}', 2) AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "o1")))
  }

  test("should interpolate from inside right") {
    val query = "RETURN right($'o${123}', 2) AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "23")))
  }

  test("should interpolate from inside ltrim") {
    val query = "WITH '' as a RETURN ltrim($' ${a}  ${   123}') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "123")))
  }

  test("should interpolate from inside rtrim") {
    val query = "WITH '' as a RETURN rtrim($'${123   } ${a} ') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "123")))
  }

  test("should interpolate from inside lower") {
    val query = "WITH 'aB' as aB RETURN lower($'${aB}cD') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "abcd")))
  }

  test("should interpolate from inside upper") {
    val query = "WITH 'aB' as aB RETURN upper($'${aB}cD') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "ABCD")))
  }

  test("should interpolate from inside reverse") {
    val query = "WITH 'aB' as aB RETURN reverse($'${aB}cD') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "DcBa")))
  }

  test("should interpolate from inside split first") {
    val query = "WITH 'aB' as aB RETURN split($'${aB}cD', 'c') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> Seq("aB", "D"))))
  }

  test("should interpolate from inside split second") {
    val query = "RETURN split('split, me', $',') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> Seq("split", " me"))))
  }

  test("should interpolate from inside split both") {
    val query = "WITH 'aB' as aB RETURN split($'${aB}$$cD', $'$$') AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> Seq("aB", "cD"))))
  }

  test("should interpolate dynamic property lookups") {
    val node = createNode(Map("initial" -> "X"))
    val query = "MATCH (n) WHERE n[$'${{prop}}'] = 'X' RETURN n"

    val result = executeWithAllPlanners(query, "prop" -> "initial")

    result.toList should equal(List(Map("n" -> node)))
  }

  test("should interpolate in rhs of regular expressions") {
    val node = createNode(Map("name" -> "Henry"))
    val query = "MATCH (n) WHERE n.name =~ $'H.+' RETURN n"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("n" -> node)))
  }

  test("should interpolate in lhs of regular expressions") {
    val node = createNode(Map("name" -> "Henry"))
    val query = "MATCH (n) WHERE $'${n.name}' =~ 'H.+' RETURN n"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("n" -> node)))
  }

  test("should interpolate in regular expressions") {
    val node = createNode(Map("name" -> "Henry"))
    val query = "MATCH (n) WHERE $'${n.name}' =~ $'H.+' RETURN n"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("n" -> node)))
  }
}
