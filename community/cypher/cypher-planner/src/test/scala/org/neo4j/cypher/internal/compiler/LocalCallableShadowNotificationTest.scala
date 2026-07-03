/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.frontend.notification.NotificationWrapping
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InstrumentedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.StrictResolveCallables
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.notification.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.notifications.NotificationImplementation

/**
 * Unit tests for the local-callable-shadows-non-local notification emitted by
 * [[org.neo4j.cypher.internal.frontend.phases.ResolveCallables.localCallableShadowNotification]].
 *
 * A notification is expected for every local procedure/function definition whose name also resolves
 * non-locally (i.e. shadows a built-in or external callable), regardless of nesting and regardless of
 * whether the local callable is actually invoked.
 */
class LocalCallableShadowNotificationTest extends ResolveCallablesTestSuite {

  test("emits no notification when the statement contains no local callable definition") {
    val statement = singleQuery(return_(aliasedReturnItem(literalInt(1), "x")))

    // even a resolver that would resolve everything must not produce a notification without definitions
    assertShadowNotifications(statement, resolverResolvingEverything)
  }

  test("emits no notification when no local callable definition resolves non-locally") {
    val statement = queryWithLocalDefinitions(
      localProcedureDefinition("p.proc").body(finish()),
      localFunctionDefinition("f.func").body(literalString("a"))
    )(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(statement, resolverShadowing())
  }

  test("emits a notification for a single top-level local procedure definition that shadows") {
    val statement = queryWithLocalDefinitions(
      localProcedureDefinition("p.shadow").body(finish())
    )(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedProcedures = Set("p.shadow")),
      expectedShadowedProcedures = Seq("p.shadow")
    )
  }

  test("emits a notification for a single top-level local function definition that shadows") {
    val statement = queryWithLocalDefinitions(
      localFunctionDefinition("f.shadow").body(literalString("a"))
    )(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedFunctions = Set("f.shadow")),
      expectedShadowedFunctions = Seq("f.shadow")
    )
  }

  test("emits a notification for a shadowing local procedure nested deep in EXISTS in CALL in a UNION arm") {
    val statement = unionWithDeeplyNestedDefinition(
      localProcedureDefinition("p.deepShadow").body(finish())
    )

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedProcedures = Set("p.deepShadow")),
      expectedShadowedProcedures = Seq("p.deepShadow")
    )
  }

  test("emits a notification for a shadowing local function nested deep in EXISTS in CALL in a UNION arm") {
    val statement = unionWithDeeplyNestedDefinition(
      localFunctionDefinition("f.deepShadow").body(literalString("a"))
    )

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedFunctions = Set("f.deepShadow")),
      expectedShadowedFunctions = Seq("f.deepShadow")
    )
  }

  test("emits a notification for a shadowing local procedure nested in a non-shadowing local procedure") {
    val inner = localProcedureDefinition("p.innerShadow").body(finish())
    val outer = localProcedureDefinition("p.outer").body(
      queryWithLocalDefinitions(inner)(singleQuery(finish()))
    )
    val statement = queryWithLocalDefinitions(outer)(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedProcedures = Set("p.innerShadow")),
      expectedShadowedProcedures = Seq("p.innerShadow")
    )
  }

  test("emits a notification for a shadowing local function nested in a non-shadowing local function") {
    val inner = localFunctionDefinition("f.innerShadow").body(literalString("a"))
    val outer = localFunctionDefinition("f.outer").body(
      queryWithLocalDefinitions(inner)(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))
    )
    val statement = queryWithLocalDefinitions(outer)(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedFunctions = Set("f.innerShadow")),
      expectedShadowedFunctions = Seq("f.innerShadow")
    )
  }

  test("emits a notification for a shadowing local procedure nested in a non-shadowing local function") {
    val inner = localProcedureDefinition("p.innerShadow").body(finish())
    val outer = localFunctionDefinition("f.outer").body(
      queryWithLocalDefinitions(inner)(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))
    )
    val statement = queryWithLocalDefinitions(outer)(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedProcedures = Set("p.innerShadow")),
      expectedShadowedProcedures = Seq("p.innerShadow")
    )
  }

  test("emits a notification for a shadowing local function nested in a non-shadowing local procedure") {
    val inner = localFunctionDefinition("f.innerShadow").body(literalString("a"))
    val outer = localProcedureDefinition("p.outer").body(
      queryWithLocalDefinitions(inner)(singleQuery(finish()))
    )
    val statement = queryWithLocalDefinitions(outer)(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedFunctions = Set("f.innerShadow")),
      expectedShadowedFunctions = Seq("f.innerShadow")
    )
  }

  test("emits a notification for every shadowing one among multiple non-nested local procedures") {
    val statement = queryWithLocalDefinitions(
      localProcedureDefinition("p.a").body(finish()),
      localProcedureDefinition("p.b").body(finish()),
      localProcedureDefinition("p.c").body(finish())
    )(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedProcedures = Set("p.a", "p.c")),
      expectedShadowedProcedures = Seq("p.a", "p.c")
    )
  }

  test("emits a notification for every shadowing one among multiple non-nested local functions") {
    val statement = queryWithLocalDefinitions(
      localFunctionDefinition("f.a").body(literalString("a")),
      localFunctionDefinition("f.b").body(literalString("b")),
      localFunctionDefinition("f.c").body(literalString("c"))
    )(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedFunctions = Set("f.a", "f.c")),
      expectedShadowedFunctions = Seq("f.a", "f.c")
    )
  }

  test("emits a notification for every shadowing one among mixed non-nested local procedures and functions") {
    val statement = queryWithLocalDefinitions(
      localProcedureDefinition("p.x").body(finish()),
      localProcedureDefinition("p.y").body(finish()),
      localFunctionDefinition("f.x").body(literalString("a")),
      localFunctionDefinition("f.y").body(literalString("b"))
    )(singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))))

    assertShadowNotifications(
      statement,
      resolverShadowing(shadowedProcedures = Set("p.x"), shadowedFunctions = Set("f.x")),
      expectedShadowedProcedures = Seq("p.x"),
      expectedShadowedFunctions = Seq("f.x")
    )
  }

  test("emits a notification for every shadowing one in a mixed scenario involving nested definitions") {
    // p.top (not shadowing) nests p.nestedShadow (shadowing) and f.nestedNo (not shadowing)
    val pNestedShadow = localProcedureDefinition("p.nestedShadow").body(finish())
    val fNestedNo = localFunctionDefinition("f.nestedNo").body(literalString("a"))
    val pTop = localProcedureDefinition("p.top").body(
      queryWithLocalDefinitions(pNestedShadow, fNestedNo)(singleQuery(finish()))
    )
    // f.top (shadowing) at top level
    val fTop = localFunctionDefinition("f.top").body(literalString("b"))
    // p.other (shadowing) nests f.deepNo (not shadowing)
    val fDeepNo = localFunctionDefinition("f.deepNo").body(literalInt(1))
    val pOther = localProcedureDefinition("p.other").body(
      queryWithLocalDefinitions(fDeepNo)(singleQuery(finish()))
    )
    val statement = queryWithLocalDefinitions(pTop, fTop, pOther)(
      singleQuery(return_(aliasedReturnItem(literalInt(1), "x")))
    )

    assertShadowNotifications(
      statement,
      resolverShadowing(
        shadowedProcedures = Set("p.nestedShadow", "p.other"),
        shadowedFunctions = Set("f.top")
      ),
      expectedShadowedProcedures = Seq("p.nestedShadow", "p.other"),
      expectedShadowedFunctions = Seq("f.top")
    )
  }

  /*
   * test infrastructure
   */

  /**
   * Builds `RETURN 1 AS x UNION CALL { RETURN EXISTS { <definition> ... } AS z } RETURN z AS x`,
   * i.e. nests the given local callable definition in an EXISTS scalar subquery inside a CALL subquery
   * inside one arm of a UNION.
   */
  private def unionWithDeeplyNestedDefinition(definition: LocalCallableDefinition): Statement = {
    val existsExpr = ExistsExpression(
      queryWithLocalDefinitions(definition)(singleQuery(finish()))
    )(pos, None, None)
    val nestingArm = singleQuery(
      importingWithSubqueryCall(
        return_(aliasedReturnItem(existsExpr, "z"))
      ),
      return_(aliasedReturnItem(varFor("z"), "x"))
    )
    union(
      singleQuery(return_(aliasedReturnItem(literalInt(1), "x"))),
      nestingArm
    )
  }

  private val resolverResolvingEverything: InstrumentedProcedureSignatureResolver =
    makeResolver(
      procSignatureLookup = _ => signature,
      funcSignatureLookup = fn => Some(userFunctionSignature(fn.fullName))
    )

  private def resolverShadowing(
    shadowedProcedures: Set[String] = Set.empty,
    shadowedFunctions: Set[String] = Set.empty
  ): InstrumentedProcedureSignatureResolver =
    makeResolver(
      procSignatureLookup = pn =>
        if (shadowedProcedures.contains(pn.fullName)) signature
        else throw new RuntimeException(s"no such procedure: ${pn.fullName}"),
      funcSignatureLookup = fn =>
        if (shadowedFunctions.contains(fn.fullName)) Some(userFunctionSignature(fn.fullName))
        else None
    )

  private def userFunctionSignature(name: String): UserFunctionSignature =
    UserFunctionSignature(
      functionName(name),
      inputSignature = IndexedSeq.empty,
      outputType = CTString,
      deprecationInfo = None,
      description = None,
      isAggregate = false,
      id = 1,
      builtIn = false
    )

  private def shadowMessage(callableKind: String, callableName: String): String =
    s"Local $callableKind `$callableName` shadows a built-in or external $callableKind with the same name."

  private def shadowNotifications(
    statement: Statement,
    resolver: InstrumentedProcedureSignatureResolver
  ): Set[NotificationImplementation] = {
    val logger = new RecordingNotificationLogger()
    val context = TestContext(notificationLogger = logger)
    val from = InitialState("<test query>", NoPlannerName, new AnonymousVariableNameGenerator).withStatement(statement)
    StrictResolveCallables(resolver).process(from, context)
    logger.notifications.map(NotificationWrapping.asKernelNotification(None))
  }

  private def assertShadowNotifications(
    statement: Statement,
    resolver: InstrumentedProcedureSignatureResolver,
    expectedShadowedProcedures: Seq[String] = Seq.empty,
    expectedShadowedFunctions: Seq[String] = Seq.empty
  ): Unit = {
    val notifications = shadowNotifications(statement, resolver)
    notifications.foreach(n => withClue(s"gql status code of $n: ")(n.gqlStatus() shouldBe "03N64"))
    val actualMessages = notifications.map(_.getDescription())
    val expectedMessages =
      expectedShadowedProcedures.map(shadowMessage("procedure", _)).toSet ++
        expectedShadowedFunctions.map(shadowMessage("function", _)).toSet
    actualMessages shouldBe expectedMessages
  }
}
