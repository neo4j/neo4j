/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class ListExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should reduce on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN" +
        " reduce(acc=0, s IN ['1','22','1','333'] | acc + size(s)) AS result," +
        " reduce(acc=0, s IN ['1','22','1','333'] | acc + null) AS nullValue")

    result.toList.head should equal(Map("result" -> 7, "nullValue" -> null))
  }

  test("should reduce on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN reduce(acc=0, n IN nodes(p) | acc + n.x) AS result")

    result.toList.head should equal(Map("result" -> 6))
  }

  test("should extract on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN extract(s IN ['1','22','1','333'] | size(s)) AS result")

    result.toList.head should equal(Map("result" -> List(1, 2, 1, 3)))
  }

  test("should extract on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN extract(n IN nodes(p) | n.x) AS result")

    result.toList.head should equal(Map("result" -> List(1, 2, 3)))
  }

  test("should list comprehension on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN [s IN ['1','22','1','333']] AS result")

    result.toList.head should equal(Map("result" -> List("1", "22", "1", "333")))
  }

  test("should list comprehension on values, with predicate") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN [s IN ['1','22','1','333'] WHERE s STARTS WITH '1'] AS result")

    result.toList.head should equal(Map("result" -> List("1", "1")))
  }

  test("should list comprehension on values, with predicate and extract") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN [s IN ['1','22','1','333'] WHERE s STARTS WITH '1' | size(s)] AS result")

    result.toList.head should equal(Map("result" -> List(1, 1)))
  }

  test("should list comprehension on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN [n IN nodes(p)] AS result")

    result.toList.head should equal(Map("result" -> List(n1, n2, n3)))
  }

  test("should list comprehension on nodes, with predicate") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN [n IN nodes(p) WHERE n.x <= 2] AS result")

    result.toList.head should equal(Map("result" -> List(n1, n2)))
  }

  test("should list comprehension on nodes, with predicate and extract") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN [n IN nodes(p) WHERE n.x <= 2 | n.x] AS result")

    result.toList.head should equal(Map("result" -> List(1, 2)))
  }

  test("should filter on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN" +
        " filter(s IN ['1','22','1','333'] WHERE s STARTS WITH '1') AS result," +
        " filter(s IN ['1','22','1','333'] WHERE null) AS nullValue")

    result.toList.head should equal(Map("result" -> List("1", "1"), "nullValue" -> List()))
  }

  test("should filter on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN filter(n IN nodes(p) WHERE n.x <= 2) AS result")

    result.toList.head should equal(Map("result" -> List(n1, n2)))
  }

  test("should all predicate on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN " +
        " all(s IN ['1','22','1','333'] WHERE size(s) > 0) AS true, " +
        " all(s IN ['1','22','1','333'] WHERE size(s) > 1) AS false, " +
        " all(s IN ['1','22','1','333'] WHERE null) AS nullValue")

    result.toList.head should equal(Map("false" -> false, "true" -> true, "nullValue" -> null))
  }

  test("should all predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE all(n IN nodes(p) WHERE n:Label)" +
          "RETURN " +
          " all(n IN nodes(p) WHERE n.x > 0) AS true, " +
          " all(n IN nodes(p) WHERE n.x > 1) AS false," +
          " all(n IN nodes(p) WHERE null) AS nullValue")

    result.toList.head should equal(Map("true" -> true, "false" -> false, "nullValue" -> null))
  }

  test("should any predicate on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN " +
        " any(s IN ['1','22','1','333'] WHERE size(s) = 1) AS true, " +
        " any(s IN ['1','22','1','333'] WHERE size(s) = 0) AS false, " +
        " any(s IN ['1','22','1','333'] WHERE null) AS nullValue")

    result.toList.head should equal(Map("false" -> false, "true" -> true, "nullValue" -> null))
  }

  test("should any predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE any(n IN nodes(p) WHERE n:Label) " +
          "RETURN " +
          " any(n IN nodes(p) WHERE n.x = 1) AS true, " +
          " any(n IN nodes(p) WHERE n.x = 0) AS false," +
          " any(n IN nodes(p) WHERE null) AS nullValue")

    result.toList.head should equal(Map("true" -> true, "false" -> false, "nullValue" -> null))
  }

  test("should none predicate on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN " +
        " none(s IN ['1','22','1','333'] WHERE size(s) = 0) AS true, " +
        " none(s IN ['1','22','1','333'] WHERE size(s) = 1) AS false, " +
        " none(s IN ['1','22','1','333'] WHERE null) AS nullValue")

    result.toList.head should equal(Map("false" -> false, "true" -> true, "nullValue" -> null))
  }

  test("should none predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE none(n IN nodes(p) WHERE n:Fake) " +
          "RETURN " +
          " none(n IN nodes(p) WHERE n.x = 0) AS true, " +
          " none(n IN nodes(p) WHERE n.x = 1) AS false," +
          " none(n IN nodes(p) WHERE null) AS nullValue")

    result.toList.head should equal(Map("true" -> true, "false" -> false, "nullValue" -> null))
  }

  test("should single predicate on values") {
    val result = executeWith(Configs.Interpreted,
      query = "RETURN " +
        " single(s IN ['1','22','1','333'] WHERE s = '333') AS true, " +
        " single(s IN ['1','22','1','333'] WHERE s = '1') AS false, " +
        " single(s IN ['1','22','1','333'] WHERE null) AS nullValue")

    result.toList.head should equal(Map("false" -> false, "true" -> true, "nullValue" -> null))
  }

  test("should single predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.Interpreted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE single(n IN nodes(p) WHERE n.x = 1) " +
          "RETURN " +
          " single(n IN nodes(p) WHERE n.x = 1) AS true, " +
          " single(n IN nodes(p) WHERE n.x > 1) AS false," +
          " single(n IN nodes(p) WHERE null) AS nullValue")

    result.toList.head should equal(Map("true" -> true, "false" -> false, "nullValue" -> null))
  }
}
