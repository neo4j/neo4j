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
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Planners
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes
import org.neo4j.internal.cypher.acceptance.comparisonsupport.TestConfiguration
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions

class ListExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should reduce on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN" +
        " reduce(acc=0, s IN ['1','22','1','333'] | acc + size(s)) AS result," +
        " reduce(acc=0, s IN ['1','22','1','333'] | acc + null) AS nullExpression," +
        " reduce(acc=0, s IN ['1',null,'1','333'] | acc + size(s)) AS nullElement," +
        " reduce(acc=7, s IN [] | 7 + s) AS emptyList," +
        " reduce(acc=null, s IN [] | 7 + s) AS emptyListOnNull")

    result.toList.head should equal(Map(
      "result" -> 7,
      "nullExpression" -> null,
      "nullElement" -> null,
      "emptyList" -> 7,
      "emptyListOnNull" -> null))
  }

  test("should reduce on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN" +
          " reduce(acc=0, n IN nodes(p) | acc + n.x) AS result," +
          " reduce(acc=0, n IN nodes(p) | acc + null) AS nullExpression," +
          " reduce(acc=0, n IN nodes(p) + [null] | acc + n.x) AS nullElement")

    result.toList.head should equal(Map(
      "result" -> 6,
      "nullExpression" -> null,
      "nullElement" -> null
    ))
  }

  test("should extract on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN" +
        " extract(s IN ['1','22','1','333'] | size(s)) AS result," +
        " extract(s IN ['1','22','1','333'] | null) AS nullExpression," +
        " extract(s IN ['1',null,'1','333'] | size(s)) AS nullElement," +
        " extract(s IN [] | 7 + s) AS emptyList")

    result.toList.head should equal(Map(
      "result" -> List(1, 2, 1, 3),
      "nullExpression" -> List(null, null, null, null),
      "nullElement" -> List(1, null, 1, 3),
      "emptyList" -> List()))
  }

  test("should extract on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN" +
          " extract(n IN nodes(p) | n.x) AS result," +
          " extract(n IN nodes(p) | null) AS nullExpression," +
          " extract(n IN nodes(p) + [null] | n.x) AS nullElement")

    result.toList.head should equal(Map(
      "result" -> List(1, 2, 3),
      "nullExpression" -> List(null, null, null),
      "nullElement" -> List(1, 2, 3, null)
    ))
  }

  test("should list comprehension on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN [s IN ['1','22','1','333']] AS result")

    result.toList.head should equal(Map("result" -> List("1", "22", "1", "333")))
  }

  test("should list comprehension on values, with predicate") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN [s IN ['1','22','1','333'] WHERE s STARTS WITH '1'] AS result")

    result.toList.head should equal(Map("result" -> List("1", "1")))
  }

  test("should list comprehension on values, with predicate and extract") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN" +
        " [s IN ['1','22','1','333'] WHERE s STARTS WITH '1' | size(s)] AS result," +
        " [s IN ['1','22','1','333'] WHERE null | size(s)] AS nullPredicate," +
        " [s IN ['1',null,'1','333'] WHERE size(s)>0 | size(s)] AS nullElement," +
        " [s IN ['1','22','1','333'] WHERE size(s)>1 | null] AS nullExtract")

    result.toList.head should equal(Map(
      "result" -> List(1, 1), // ['1', '1']
      "nullPredicate" -> List(), // []
      "nullElement" -> List(1, 1, 3), // ['1', '1', '333']
      "nullExtract" -> List(null, null) // ['22', '333']
    ))
  }

  test("should list comprehension on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
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
    val result = executeWith(Configs.InterpretedAndSlotted,
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
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN [n IN nodes(p) WHERE n.x <= 2 | n.x] AS result")

    result.toList.head should equal(Map("result" -> List(1, 2)))
  }

  test("should filter on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN" +
        " filter(s IN ['1','22','1','333'] WHERE s STARTS WITH '1') AS result," +
        " filter(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " filter(s IN ['1',null,'1','333'] WHERE size(s)>1) AS nullElement," +
        " filter(s IN [] WHERE s > 7) AS emptyList")

    result.toList.head should equal(Map(
      "result" -> List("1", "1"),
      "nullPredicate" -> List(),
      "nullElement" -> List("333"),
      "emptyList" -> List()
    ))
  }

  test("should filter on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN" +
          " filter(n IN nodes(p) WHERE n.x <= 2) AS result," +
          " filter(n IN nodes(p) WHERE null) AS nullPredicate," +
          " filter(n IN nodes(p) + [null] WHERE n.x < 2) AS nullElement")

    result.toList.head should equal(Map(
      "result" -> List(n1, n2),
      "nullPredicate" -> List(),
      "nullElement" -> List(n1)
    ))
  }

  test("should all predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN " +
        " all(s IN ['1','22','1','333'] WHERE size(s) > 0) AS allTrue, " +
        " all(s IN ['1','22','1','333'] WHERE size(s) > 1) AS someFalse, " +
        " all(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " all(s IN ['1',null,'1','333'] WHERE size(s) > 0) AS allTrueWithNull," +
        " all(s IN ['1',null,'1','333'] WHERE size(s) > 1) AS someFalseWithNull," +
        " all(s IN [] WHERE true) AS emptyList")

    result.toList.head should equal(Map(
      "allTrue" -> true,
      "someFalse" -> false,
      "nullPredicate" -> null,
      "allTrueWithNull" -> null,
      "someFalseWithNull" -> false,
      "emptyList" -> true))
  }

  test("should all predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE all(n IN nodes(p) WHERE n:Label)" +
          "RETURN " +
          " all(n IN nodes(p) WHERE n.x > 0) AS allTrue, " +
          " all(n IN nodes(p) WHERE n.x > 1) AS someFalse," +
          " all(n IN nodes(p) WHERE null) AS nullPredicate," +
          " all(n IN nodes(p) + [null] WHERE n.x > 0) AS allTrueWithNull," +
          " all(n IN nodes(p) + [null] WHERE n.x > 1) AS someFalseWithNull")

    result.toList.head should equal(Map(
      "allTrue" -> true,
      "someFalse" -> false,
      "nullPredicate" -> null,
      "allTrueWithNull" -> null,
      "someFalseWithNull" -> false))
  }

  test("should any predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN " +
        " any(s IN ['1','22','1','333'] WHERE size(s) = 1) AS someTrue, " +
        " any(s IN ['1','22','1','333'] WHERE size(s) = 0) AS allFalse, " +
        " any(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " any(s IN ['1',null,'1','333'] WHERE size(s) = 1) AS someTrueWithNull," +
        " any(s IN ['1',null,'1','333'] WHERE size(s) = 0) AS allFalseWithNull," +
        " any(s IN [] WHERE s > 7) AS emptyList")

    result.toList.head should equal(Map(
      "someTrue" -> true,
      "allFalse" -> false,
      "nullPredicate" -> null,
      "someTrueWithNull" -> true,
      "allFalseWithNull" -> null,
      "emptyList" -> false))
  }

  test("should any predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE any(n IN nodes(p) WHERE n:Label) " +
          "RETURN " +
          " any(n IN nodes(p) WHERE n.x = 1) AS someTrue," +
          " any(n IN nodes(p) WHERE n.x = 0) AS allFalse," +
          " any(n IN nodes(p) WHERE null) AS nullPredicate," +
          " any(n IN nodes(p)+[null] WHERE n.x = 1) AS someTrueWithNull," +
          " any(n IN nodes(p)+[null] WHERE n.x = 0) AS allFalseWithNull")

    result.toList.head should equal(Map(
      "someTrue" -> true,
      "allFalse" -> false,
      "nullPredicate" -> null,
      "someTrueWithNull" -> true,
      "allFalseWithNull" -> null))
  }

  test("should none predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN" +
        " none(s IN ['1','22','1','333'] WHERE size(s) = 0) AS allFalse," +
        " none(s IN ['1','22','1','333'] WHERE size(s) = 1) AS someTrue," +
        " none(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " none(s IN ['1',null,'1','333'] WHERE size(s) = 0) AS allFalseWithNull," +
        " none(s IN ['1',null,'1','333'] WHERE size(s) = 1) AS someTrueWithNull," +
        " none(s IN [] WHERE s > 7) AS emptyList")

    result.toList.head should equal(Map(
      "allFalse" -> true,
      "someTrue" -> false,
      "nullPredicate" -> null,
      "allFalseWithNull" -> null,
      "someTrueWithNull" -> false,
      "emptyList" -> true
    ))
  }

  test("should none predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE none(n IN nodes(p) WHERE n:Fake) " +
          "RETURN " +
          " none(n IN nodes(p) WHERE n.x = 0) AS allFalse," +
          " none(n IN nodes(p) WHERE n.x = 1) AS someTrue, " +
          " none(n IN nodes(p) WHERE null) AS nullValue," +
          " none(n IN nodes(p) + [null] WHERE n.x = 0) AS allFalseWithNull," +
          " none(n IN nodes(p) + [null] WHERE n.x = 1) AS someTrueWithNull")

    result.toList.head should equal(Map(
      "allFalse" -> true,
      "someTrue" -> false,
      "nullValue" -> null,
      "allFalseWithNull" -> null,
      "someTrueWithNull" -> false
    ))
  }

  test("should single predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN " +
        " single(s IN ['1','22','1','333'] WHERE s = '0') AS noneTrue," +
        " single(s IN ['1','22','1','333'] WHERE s = '333') AS oneTrue," +
        " single(s IN ['1','22','1','333'] WHERE s = '1') AS twoTrue," +
        " single(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " single(s IN ['1',null,'1','333'] WHERE s = '0') AS noneTrueWithNull," +
        " single(s IN ['1',null,'1','333'] WHERE s = '333') AS oneTrueWithNull," +
        " single(s IN [] WHERE true) AS emptyList")

    result.toList.head should equal(Map(
      "noneTrue" -> false,
      "oneTrue" -> true,
      "twoTrue" -> false,
      "nullPredicate" -> null,
      "noneTrueWithNull" -> null,
      "oneTrueWithNull" -> null,
      "emptyList" -> false))
  }

  // NOTE: should be merged with above test, but older Cypher versions fail on ONLY this case. it would be a shame to remove asserts on all other cases.
  test("should single predicate on values -- multiple true with null case") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      query = "RETURN " +
        " single(s IN ['1',null,'1','333'] WHERE s = '1') AS twoTrueWithNull",
      expectedDifferentResults =
        Configs.Version2_3 + Configs.Version3_1)

    result.toList.head should equal(Map(
      "twoTrueWithNull" -> false))
  }

  test("should single predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlotted,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE single(n IN nodes(p) WHERE n.x = 1) " +
          "RETURN " +
          " single(n IN nodes(p) WHERE n.x = 0) AS noneTrue," +
          " single(n IN nodes(p) WHERE n.x = 1) AS oneTrue, " +
          " single(n IN nodes(p) WHERE n.x <= 2) AS twoTrue, " +
          " single(n IN nodes(p) WHERE null) AS nullPredicate," +
          " single(n IN nodes(p) + [null] WHERE n.x = 0) AS noneTrueWithNull," +
          " single(n IN nodes(p) + [null] WHERE n.x = 1) AS oneTrueWithNull," +
          " single(n IN nodes(p) + [null] WHERE n.x <= 2) AS twoTrueWithNull")

    result.toList.head should equal(Map(
      "noneTrue" -> false,
      "oneTrue" -> true,
      "twoTrue" -> false,
      "nullPredicate" -> null,
      "noneTrueWithNull" -> null,
      "oneTrueWithNull" -> null,
      "twoTrueWithNull" -> false))
  }
}
