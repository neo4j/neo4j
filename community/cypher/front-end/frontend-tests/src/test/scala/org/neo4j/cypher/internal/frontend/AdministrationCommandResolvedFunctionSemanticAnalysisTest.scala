/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AlterAuthRule
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AuthRuleCondition
import org.neo4j.cypher.internal.ast.CreateAuthRule
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.Node
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.WindowsSemanticErrorDefSeqStringSafe
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

/**
 * Semantic-analysis tests for the PBAC/ABAC function checks where the function has been resolved
 * to a [[ResolvedFunctionInvocation]] before the semantic check runs. The registered built-in
 * functions (the temporal functions and the abac functions) are not compiler built-ins, so they
 * only become built-ins once resolved — hence they are covered here rather than in
 * AdministrationCommandTest, which can only build raw
 * [[org.neo4j.cypher.internal.expressions.FunctionInvocation]] nodes.
 */
class AdministrationCommandResolvedFunctionSemanticAnalysisTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val windowsSafe: WindowsSemanticErrorDefSeqStringSafe.type = WindowsSemanticErrorDefSeqStringSafe

  private val p = InputPosition.withLength(13, 12, 11, 10)
  private val pos1 = InputPosition(2, 1, 3).withInputLength(2)

  private val state =
    SemanticState.clean
      .withFeature(SemanticFeature.MultipleDatabases)
      .withFeature(SemanticFeature.RelationshipPropertyValueAccessRules)
      .withFeature(SemanticFeature.AttributeBasedAccessControl)

  private val context = SemanticCheckContext(CypherVersion.Cypher25, NotImplementedErrorMessageProvider)

  private def signature(
    name: String,
    builtIn: Boolean,
    inputs: IndexedSeq[FieldSignature] = IndexedSeq.empty
  ): UserFunctionSignature =
    UserFunctionSignature(
      FunctionName(name)(p),
      inputs,
      CTAny,
      None,
      None,
      isAggregate = false,
      id = 0,
      builtIn = builtIn
    )

  private def resolved(
    name: String,
    fcnSignature: Option[UserFunctionSignature],
    position: InputPosition,
    args: Expression*
  ): ResolvedFunctionInvocation =
    ResolvedFunctionInvocation(FunctionName(name)(position), fcnSignature, args.toIndexedSeq)(position)

  private def abac(arg: Expression, position: InputPosition = p): ResolvedFunctionInvocation =
    resolved("abac.oidc.user_attribute", Some(signature("abac.oidc.user_attribute", builtIn = true)), position, arg)

  private def resolvedBuiltin(name: String, position: InputPosition, args: Expression*): ResolvedFunctionInvocation =
    resolved(
      name,
      Some(signature(name, builtIn = true, IndexedSeq(FieldSignature("value", CTString)))),
      position,
      args: _*
    )

  private def createAuthRuleWith(fn: Expression): CreateAuthRule =
    CreateAuthRule(
      literalString("authRule"),
      IfExistsThrowError,
      List(AuthRuleCondition(Equals(fn, literalString("SE"))(p))(p))
    )(p)

  private def alterAuthRuleWith(fn: Expression): AlterAuthRule =
    AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(AuthRuleCondition(Equals(fn, literalString("SE"))(p))(p))
    )(p)

  private def nodePropertyRuleWith(fn: Expression): GrantPrivilege =
    new GrantPrivilege(
      GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
      false,
      None,
      List(PatternQualifier(
        Seq(LabelQualifier("A")(p)),
        Some(varFor("n")),
        Equals(prop(varFor("n"), "prop1"), fn)(p),
        Node
      )),
      Seq(literalString("role1"))
    )(p)

  // -- isBuiltIn ---------------------------------------------------------------

  test("ResolvedFunctionInvocation.isBuiltIn reflects the signature builtIn flag") {
    resolved("abac.oidc.user_attribute", Some(signature("abac.oidc.user_attribute", builtIn = true)), p)
      .isBuiltIn shouldBe true
    resolved("abac.oidc.user_attribute", Some(signature("abac.oidc.user_attribute", builtIn = false)), p)
      .isBuiltIn shouldBe false
  }

  test("ResolvedFunctionInvocation.isBuiltIn is false when the signature is unresolved") {
    resolved("abac.oidc.user_attribute", None, p).isBuiltIn shouldBe false
  }

  test("ResolvedFunctionInvocation.isUserDefined is true only for a resolved non-built-in signature") {
    resolved("abac.oidc.user_attribute", Some(signature("abac.oidc.user_attribute", builtIn = false)), p)
      .isUserDefined shouldBe true
    resolved("abac.oidc.user_attribute", Some(signature("abac.oidc.user_attribute", builtIn = true)), p)
      .isUserDefined shouldBe false
    resolved("abac.oidc.user_attribute", None, p).isUserDefined shouldBe false
  }

  // -- allow-list (every entry, classified as compiler vs registered built-in) -------------------

  // point() is the only allow-listed function that is a compiler built-in; the rest of the
  // compiler built-ins (range, abs, toLower, ...) are likewise raw FunctionInvocations, while the
  // temporal and abac functions are registered built-ins represented as ResolvedFunctionInvocation.
  private def asAllowListedBuiltin(fnName: String): Expression = {
    val raw = function(fnName, literalString("x"))
    if (raw.isBuiltIn) raw
    else resolved(fnName, Some(signature(fnName, builtIn = true)), p, literalString("x"))
  }

  test("CREATE AUTH RULE authRule SET CONDITION accepts every allow-listed function") {
    AdministrationCommand.authRuleAllowListedFunctions.foreach { fnName =>
      withClue(s"$fnName: ") {
        createAuthRuleWith(asAllowListedBuiltin(fnName)).semanticCheck.run(state, context).errors shouldBe empty
      }
    }
  }

  test("CREATE AUTH RULE authRule SET CONDITION accepts uppercase variants of allow-listed functions") {
    Seq("DATE", "DateTime", "TOLOWER").foreach { fnName =>
      withClue(s"$fnName: ") {
        createAuthRuleWith(asAllowListedBuiltin(fnName)).semanticCheck.run(state, context).errors shouldBe empty
      }
    }
  }

  // -- ABAC (CREATE AUTH RULE) -------------------------------------------------

  test("CREATE AUTH RULE authRule SET CONDITION abac.native.user_tags() IS NOT NULL accepts the resolved built-in") {
    val userTags = resolved("abac.native.user_tags", Some(signature("abac.native.user_tags", builtIn = true)), p)
    createAuthRuleWith(userTags).semanticCheck.run(state, context).errors shouldBe empty
  }

  test("CREATE AUTH RULE rejects abac.native.user_tags resolved to a non-built-in (shadowing) function") {
    val userTags = resolved("abac.native.user_tags", Some(signature("abac.native.user_tags", builtIn = false)), p)
    val result = createAuthRuleWith(userTags).semanticCheck.run(state, context)
    result.errors.size shouldBe 1
    result.errors.head.msg should include(
      "Invalid input 'abac.native.user_tags' for function in auth rule condition"
    )
  }

  test("CREATE AUTH RULE accepts an allow-listed abac function whose provider is not enabled (unresolved)") {
    // When the abac provider is off the function does not resolve to a signature; it is accepted at
    // semantic time (and rejected at evaluation time) rather than blocking authoring of the rule.
    Seq(
      resolved("abac.native.user_tags", None, p),
      resolved("abac.oidc.user_attribute", None, p, literalString("country"))
    ).foreach { fn =>
      withClue(s"${fn.functionName.fullName}: ") {
        createAuthRuleWith(fn).semanticCheck.run(state, context).errors shouldBe empty
      }
    }
  }

  test("CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('country') = 'SE'") {
    createAuthRuleWith(abac(literalString("country"))).semanticCheck.run(state, context).errors shouldBe empty
  }

  test("CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute($param) = 'SE'") {
    val param = parameter("param", CTAny)
    createAuthRuleWith(abac(param)).semanticCheck.run(state, context).errors shouldBe SemanticCheckResult
      .error(state, SemanticError.authRuleConditionCannotContainParameter(param)).errors
  }

  test("CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('hello' + 1) = 'SE'") {
    createAuthRuleWith(abac(Add(literalString("hello"), literalInt(1))(p)))
      .semanticCheck.run(state, context).errors shouldBe empty
  }

  test("CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute(1 + 1) = 'SE'") {
    // Does not fail since we don't evaluate the inner expression
    createAuthRuleWith(abac(Add(literalInt(1), literalInt(1))(p)))
      .semanticCheck.run(state, context).errors shouldBe empty
  }

  test("CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute(toLower('HELLO')) = 'SE'") {
    createAuthRuleWith(abac(function("toLower", literalString("HELLO"))))
      .semanticCheck.run(state, context).errors shouldBe empty
  }

  test("CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('country', 'city') = 'SE_MALMÖ'") {
    createAuthRuleWith(abac(listOf(literalString("country"), literalString("city")), pos1))
      .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
      .error(
        GqlHelper.getGql42001_42I13(
          1,
          2,
          "abac.oidc.user_attribute",
          "abac.oidc.user_attribute(attributeKey :: STRING) :: ANY",
          pos1.offset,
          pos1.line,
          pos1.column
        ),
        state,
        """Function call does not provide the required number of arguments: expected 1 got 2.
          |
          |Function abac.oidc.user_attribute has signature: abac.oidc.user_attribute(attributeKey :: STRING) :: ANY
          |meaning that it expects 1 [country, city]""".stripMargin,
        pos1
      ).errors)
  }

  test("CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute(1) = 'SE_MALMÖ'") {
    createAuthRuleWith(abac(literalInt(1, pos1))).semanticCheck.run(state, context).errors shouldBe SemanticCheckResult
      .error(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of(CTString.toCypherTypeString),
          "INTEGER",
          pos1.offset,
          pos1.line,
          pos1.column
        ),
        state,
        "Type mismatch: expected String but was Integer",
        pos1.withInputLength(1)
      ).errors
  }

  Seq("date", "datetime", "localtime", "localdatetime", "time").foreach { functionName =>
    test(
      s"CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName('2024-11-18')"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolvedBuiltin(functionName, p, literalString("2024-11-18"))
      )(p)
      CreateAuthRule(literalString("authRule"), IfExistsThrowError, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors shouldBe empty
    }

    test(s"CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName()") {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolved(functionName, Some(signature(functionName, builtIn = true)), pos1)
      )(p)
      CreateAuthRule(literalString("authRule"), IfExistsThrowError, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
        .error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(pos1.offset, pos1.line, pos1.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .atPosition(pos1.offset, pos1.line, pos1.column)
                .withParam(GqlParams.StringParam.input, functionName)
                .withParam(GqlParams.StringParam.context, "function in auth rule condition")
                .build()
            ).build(),
          state,
          s"""42001
             |22N05: Invalid input '$functionName' for function in auth rule condition.""".stripMargin,
          pos1
        ).errors)
    }

    test(
      s"CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName({timezone: 'UTC'})"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolved(
          functionName,
          Some(signature(functionName, builtIn = true)),
          pos1,
          MapExpression(Seq(PropertyKeyName("timezone")(pos1) -> literalString("UTC")))(pos1)
        )
      )(p)
      CreateAuthRule(literalString("authRule"), IfExistsThrowError, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
        .error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(pos1.offset, pos1.line, pos1.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAM)
                .atPosition(pos1.offset, pos1.line, pos1.column)
                .withParam(GqlParams.StringParam.input, s"""$functionName({timezone: 'UTC'})""")
                .withParam(GqlParams.StringParam.input1, s"""$functionName.transaction('UTC')""")
                .build()
            ).build(),
          state,
          s"""42001
             |42NAM: '$functionName({timezone: 'UTC'})' cannot be used in auth rule conditions as it retrieves the current time. Only transaction start time is available at the time of auth rule evaluation. Use '$functionName.transaction('UTC')' instead.""".stripMargin,
          pos1
        ).errors)
    }

    test(
      s"CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName({timezone: 1})"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolved(
          functionName,
          Some(signature(functionName, builtIn = true)),
          pos1,
          MapExpression(Seq(PropertyKeyName("timezone")(pos1) -> literalInt(1, pos1)))(pos1)
        )
      )(p)
      CreateAuthRule(literalString("authRule"), IfExistsThrowError, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
        .error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(pos1.offset, pos1.line, pos1.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAM)
                .atPosition(pos1.offset, pos1.line, pos1.column)
                .withParam(GqlParams.StringParam.input, s"""$functionName({timezone: 1})""")
                .withParam(GqlParams.StringParam.input1, s"""$functionName.transaction()""")
                .build()
            ).build(),
          state,
          s"""42001
             |42NAM: '$functionName({timezone: 1})' cannot be used in auth rule conditions as it retrieves the current time. Only transaction start time is available at the time of auth rule evaluation. Use '$functionName.transaction()' instead.""".stripMargin,
          pos1
        ).errors)
    }

    test(
      s"CREATE AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName.transaction('UTC')"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolvedBuiltin(s"$functionName.transaction", pos1, literalString("UTC"))
      )(p)
      CreateAuthRule(literalString("authRule"), IfExistsThrowError, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors shouldBe empty
    }
  }

  // -- ABAC (ALTER AUTH RULE) --------------------------------------------------

  test("ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('country') = 'SE'") {
    alterAuthRuleWith(abac(literalString("country"))).semanticCheck.run(state, context).errors shouldBe empty
  }

  test("ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute($param) = 'SE'") {
    val param = parameter("param", CTAny)
    alterAuthRuleWith(abac(param)).semanticCheck.run(state, context).errors shouldBe SemanticCheckResult
      .error(state, SemanticError.authRuleConditionCannotContainParameter(param)).errors
  }

  test("ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('hello' + 1) = 'SE'") {
    alterAuthRuleWith(abac(Add(literalString("hello"), literalInt(1))(p)))
      .semanticCheck.run(state, context).errors shouldBe empty
  }

  test("ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute(1 + 1) = 'SE'") {
    alterAuthRuleWith(abac(Add(literalInt(1), literalInt(1))(p)))
      .semanticCheck.run(state, context).errors shouldBe empty
  }

  test("ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute(toLower('HELLO')) = 'SE'") {
    alterAuthRuleWith(abac(function("toLower", literalString("HELLO"))))
      .semanticCheck.run(state, context).errors shouldBe empty
  }

  test("ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('country', 'city') = 'SE_MALMÖ'") {
    alterAuthRuleWith(abac(listOf(literalString("country"), literalString("city")), pos1))
      .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
      .error(
        GqlHelper.getGql42001_42I13(
          1,
          2,
          "abac.oidc.user_attribute",
          "abac.oidc.user_attribute(attributeKey :: STRING) :: ANY",
          pos1.offset,
          pos1.line,
          pos1.column
        ),
        state,
        """Function call does not provide the required number of arguments: expected 1 got 2.
          |
          |Function abac.oidc.user_attribute has signature: abac.oidc.user_attribute(attributeKey :: STRING) :: ANY
          |meaning that it expects 1 [country, city]""".stripMargin,
        pos1
      ).errors)
  }

  test("ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute(1) = 'SE_MALMÖ'") {
    alterAuthRuleWith(abac(literalInt(1, pos1))).semanticCheck.run(state, context).errors shouldBe SemanticCheckResult
      .error(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of(CTString.toCypherTypeString),
          "INTEGER",
          pos1.offset,
          pos1.line,
          pos1.column
        ),
        state,
        "Type mismatch: expected String but was Integer",
        pos1.withInputLength(1)
      ).errors
  }

  Seq("date", "datetime", "localtime", "localdatetime", "time").foreach { functionName =>
    test(
      s"ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName('2024-11-18')"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolvedBuiltin(functionName, p, literalString("2024-11-18"))
      )(p)
      AlterAuthRule(literalString("authRule"), ifExists = false, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors shouldBe empty
    }

    test(s"ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName()") {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolved(functionName, Some(signature(functionName, builtIn = true)), pos1)
      )(p)
      AlterAuthRule(literalString("authRule"), ifExists = false, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
        .error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(pos1.offset, pos1.line, pos1.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .atPosition(pos1.offset, pos1.line, pos1.column)
                .withParam(GqlParams.StringParam.input, functionName)
                .withParam(GqlParams.StringParam.context, "function in auth rule condition")
                .build()
            ).build(),
          state,
          s"""42001
             |22N05: Invalid input '$functionName' for function in auth rule condition.""".stripMargin,
          pos1
        ).errors)
    }

    test(
      s"ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName({timezone: 'UTC'})"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolved(
          functionName,
          Some(signature(functionName, builtIn = true)),
          pos1,
          MapExpression(Seq(PropertyKeyName("timezone")(pos1) -> literalString("UTC")))(pos1)
        )
      )(p)
      AlterAuthRule(literalString("authRule"), ifExists = false, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
        .error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(pos1.offset, pos1.line, pos1.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAM)
                .atPosition(pos1.offset, pos1.line, pos1.column)
                .withParam(GqlParams.StringParam.input, s"""$functionName({timezone: 'UTC'})""")
                .withParam(GqlParams.StringParam.input1, s"""$functionName.transaction('UTC')""")
                .build()
            ).build(),
          state,
          s"""42001
             |42NAM: '$functionName({timezone: 'UTC'})' cannot be used in auth rule conditions as it retrieves the current time. Only transaction start time is available at the time of auth rule evaluation. Use '$functionName.transaction('UTC')' instead.""".stripMargin,
          pos1
        ).errors)
    }

    test(
      s"ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName({timezone: 1})"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolved(
          functionName,
          Some(signature(functionName, builtIn = true)),
          pos1,
          MapExpression(Seq(PropertyKeyName("timezone")(pos1) -> literalInt(1, pos1)))(pos1)
        )
      )(p)
      AlterAuthRule(literalString("authRule"), ifExists = false, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors should equal(SemanticCheckResult
        .error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(pos1.offset, pos1.line, pos1.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAM)
                .atPosition(pos1.offset, pos1.line, pos1.column)
                .withParam(GqlParams.StringParam.input, s"""$functionName({timezone: 1})""")
                .withParam(GqlParams.StringParam.input1, s"""$functionName.transaction()""")
                .build()
            ).build(),
          state,
          s"""42001
             |42NAM: '$functionName({timezone: 1})' cannot be used in auth rule conditions as it retrieves the current time. Only transaction start time is available at the time of auth rule evaluation. Use '$functionName.transaction()' instead.""".stripMargin,
          pos1
        ).errors)
    }

    test(
      s"ALTER AUTH RULE authRule SET CONDITION abac.oidc.user_attribute('start_date') > $functionName.transaction('UTC')"
    ) {
      val condition = GreaterThan(
        abac(literalString("start_date")),
        resolvedBuiltin(s"$functionName.transaction", pos1, literalString("UTC"))
      )(p)
      AlterAuthRule(literalString("authRule"), ifExists = false, List(AuthRuleCondition(condition)(p)))(p)
        .semanticCheck.run(state, context).errors shouldBe empty
    }
  }

  // -- PBAC (property rule privilege) ------------------------------------------

  test("property rules with allow-listed temporal functions resolved as built-ins should pass semantic checking") {
    val cases = Seq[(String, Expression)](
      ("date", resolvedBuiltin("date", p, literalString("2024-08-23"))),
      ("datetime", resolvedBuiltin("datetime", p, literalString("2024-08-24T12:50:35+01:00"))),
      ("localdatetime", resolvedBuiltin("localdatetime", p, literalString("2024-08-24T12:50:35"))),
      ("localtime", resolvedBuiltin("localtime", p, literalString("12:50:35"))),
      ("time", resolvedBuiltin("time", p, literalString("12:50:35+01:00"))),
      ("duration", resolvedBuiltin("duration", p, literalString("PT30S"))),
      ("point", function("point", mapOfInt("x" -> 1, "y" -> 2)))
    )
    cases.map(_._1) should contain theSameElementsAs AdministrationCommand.propertyRuleAllowedTemporalFunctions
    cases.foreach { case (name, call) =>
      withClue(s"$name: ") {
        nodePropertyRuleWith(call).semanticCheck.run(state, context).errors shouldBe empty
      }
    }
  }

  test("property rule with a temporal function resolved to a non-built-in should fail semantic checking") {
    val notBuiltIn = resolved(
      "date",
      Some(signature("date", builtIn = false, IndexedSeq(FieldSignature("value", CTString)))),
      p,
      literalString("2024-08-23")
    )
    val result = nodePropertyRuleWith(notBuiltIn).semanticCheck.run(state, context)
    result.errors.size shouldBe 1
    result.errors.head.msg should include("is not supported")
  }
}
