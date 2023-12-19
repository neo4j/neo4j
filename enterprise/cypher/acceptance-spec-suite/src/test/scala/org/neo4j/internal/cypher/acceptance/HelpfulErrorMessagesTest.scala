/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, SyntaxException}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class HelpfulErrorMessagesTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should provide sensible error message when omitting colon before relationship type on create") {

    failWithError(Configs.AbsolutelyAll - Configs.Version2_3,

      "CREATE (a)-[ASSOCIATED_WITH]->(b)",
      Seq("Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?"))
  }

  test("should provide sensible error message when trying to add multiple relationship types on create") {
    failWithError(Configs.AbsolutelyAll,
      "CREATE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)",
      Seq("A single relationship type must be specified for CREATE",
          "The given query is not currently supported in the selected cost-based planner" ))
  }

  test("should provide sensible error message when omitting colon before relationship type on merge") {
    failWithError(Configs.AbsolutelyAll - Configs.Version2_3,
      "MERGE (a)-[ASSOCIATED_WITH]->(b)",
      Seq("Exactly one relationship type must be specified for MERGE. Did you forget to prefix your relationship type with a ':'?"))
  }

  test("should provide sensible error message when trying to add multiple relationship types on merge") {
    failWithError(Configs.AbsolutelyAll - Configs.Rule2_3,
      "MERGE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)",
      Seq("A single relationship type must be specified for MERGE",
      "The given query is not currently supported in the selected cost-based planner"))
  }

  test("should provide sensible error message for 3.4 rule planner") {
    intercept[Exception](graph.execute("CYPHER 3.4 planner=rule RETURN 1")).getMessage should be("Unsupported PLANNER - VERSION combination: rule - 3.4")
  }

  test("should not fail for specifying rule planner if no version specified") {
    graph.execute("CYPHER planner=rule RETURN 1") // should not fail
  }

  test("should provide sensible error message for rule planner and slotted") {
    intercept[Exception](graph.execute("CYPHER planner=rule runtime=slotted RETURN 1")).getMessage should be("Unsupported PLANNER - RUNTIME combination: rule - slotted")
  }

  test("should provide sensible error message for invalid regex syntax together with index") {

    // Fixed in 3.2.8
    graph.execute("CREATE (n:Person {text:'abcxxxdefyyyfff'})")
    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Compiled - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (x:Person) WHERE x.text =~ '*xxx*yyy*' RETURN x.text", List("Invalid Regex:"))
  }

  test("should provide sensible error message for START in newer runtimes") {
    val query = "START n=node(0) RETURN n"
    failWithError(Configs.SlottedInterpreted + Configs.Compiled, query, Seq("The given query is not currently supported in the selected runtime"))
  }

  test("should not fail when using compatible runtime with START") {
    createNode()
    val query = "START n=node(0) RETURN n"
    val conf = TestConfiguration(
      Versions(Versions.V2_3, Versions.V3_1, Versions.Default),
      Planners(Planners.Rule, Planners.Default),
      Runtimes(Runtimes.Interpreted, Runtimes.Default))
    executeWith(conf, query) // should not fail
  }

  test("should provide sensible error message for CREATE UNIQUE in newer runtimes") {
    val query = "MATCH (root { name: 'root' }) CREATE UNIQUE (root)-[:LOVES]-(someone) RETURN someone"
    failWithError(Configs.SlottedInterpreted + Configs.Compiled, query, Seq("The given query is not currently supported in the selected runtime"))
  }

  test("should give correct error message with invalid number literal in a subtract") {
    a[SyntaxException] shouldBe thrownBy {
      innerExecuteDeprecated("with [1a-1] as list return list", Map())
    }
  }

  // Operations on incompatible types
  test("should provide sensible error message when trying to add incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    graph.execute("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9 })")

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num + n.loc", List("Cannot add `Long` and `Point`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num + n.dur", List("Cannot add `Long` and `Duration`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num + n.dat", List("Cannot add `Long` and `DateTime`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.flo + n.bool", List("Cannot add `Double` and `Boolean`"))
  }

  test("should provide sensible error message when trying to multiply incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    graph.execute("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num * n.loc", List("Cannot multiply `Long` and `Point`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num * n.dat", List("Cannot multiply `Long` and `DateTime`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.flo * n.bool", List("Cannot multiply `Double` and `Boolean`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.lst * n.str", List("Cannot multiply `LongArray` and `String`"))
  }

  test("should provide sensible error message when trying to subtract incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    graph.execute("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num - n.loc", List("Cannot subtract `Point` from `Long`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num - n.dat", List("Cannot subtract `DateTime` from `Long`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.flo - n.bool", List("Cannot subtract `Boolean` from `Double`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.lst - n.str", List("Cannot subtract `String` from `LongArray`"))
  }

  test("should provide sensible error message when trying to calculate modulus of incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    graph.execute("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num % n.loc", List("Cannot calculate modulus of `Long` and `Point`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num % n.dat", List("Cannot calculate modulus of `Long` and `DateTime`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.flo % n.bool", List("Cannot calculate modulus of `Double` and `Boolean`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.lst % n.str", List("Cannot calculate modulus of `LongArray` and `String`"))
  }

  test("should provide sensible error message when trying to divide incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    graph.execute("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num / n.loc", List("Cannot divide `Long` by `Point`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num / n.dat", List("Cannot divide `Long` by `DateTime`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.flo / n.bool", List("Cannot divide `Double` by `Boolean`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.lst / n.str", List("Cannot divide `LongArray` by `String`"))
  }

  test("should provide sensible error message when trying to raise to the power of incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    graph.execute("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Compiled - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num ^ n.loc", List("Cannot raise `Long` to the power of `Point`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Compiled - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.num ^ n.dat", List("Cannot raise `Long` to the power of `DateTime`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Compiled - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.flo ^ n.bool", List("Cannot raise `Double` to the power of `Boolean`"))

    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Compiled - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (n:Test) RETURN n.lst ^ n.str", List("Cannot raise `LongArray` to the power of `String`"))
  }
}
