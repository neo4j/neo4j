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
package org.neo4j.cypher.internal.ast

import org.junit.jupiter.api.Assertions.assertEquals
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionHelpers.versionedSemanticContext
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.FeatureError
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.reflections.Reflections

import java.lang.reflect.Modifier
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

class AdministrationCommandTest extends CypherFunSuite with AstConstructionTestSupport with CypherVersionTestSupport
    with AstConstructionTestSupportWithPosConversion {

  implicit val windowsSafe: WindowsSemanticErrorDefSeqStringSafe.type = WindowsSemanticErrorDefSeqStringSafe

  private val p = InputPosition.withLength(13, 12, 11, 10)
  private val pos1 = InputPosition(2, 1, 3).withInputLength(2)
  private val pos2 = InputPosition(16, 5, 4).withInputLength(4)
  private val pos3 = InputPosition(23, 7, 6).withInputLength(2)
  private val pos4 = InputPosition(37, 8, 9).withInputLength(2)

  private def gqlAuthProviderNotAllowedEmpty(pos: InputPosition) =
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
      .atPosition(pos.offset, pos.line, pos.column)
      .withParam(GqlParams.StringParam.item, "Auth provider").build()

  private def gqlCannotCombineOldAndNewSyntax(pos: InputPosition) = ErrorGqlStatusObjectImplementation
    .from(GqlStatusInfoCodes.STATUS_42N92)
    .atPosition(pos.offset, pos.line, pos.column)
    .build()

  private def gqlMissingAuth(pos: InputPosition) =
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
      .withParam(GqlParams.ListParam.inputList, List("Auth provider").asJava)
      .atPosition(pos.offset, pos.line, pos.column)
      .build()

  private def gqlIncompleteAuthCommand(pos: InputPosition) =
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N94)
      .atPosition(pos.offset, pos.line, pos.column)
      .build()

  private def gqlStringOrStringListWrongType(
    expr: String,
    context: String,
    pos: InputPosition,
    allowEmptyList: Boolean = false
  ) = {
    val listForm = if (allowEmptyList) "List of non-empty Strings" else "non-empty List of non-empty Strings"
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
      .withParam(GqlParams.StringParam.input, expr)
      .withParam(GqlParams.StringParam.context, context)
      .withParam(
        GqlParams.ListParam.inputList,
        List("non-empty String", listForm, "Parameter").asJava
      )
      .atPosition(pos.offset, pos.line, pos.column)
      .build()
  }

  private def gqlWrongType(expr: String, context: String, expectedTypes: Seq[String], pos: InputPosition) =
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
          .withParam(GqlParams.StringParam.input, expr)
          .withParam(GqlParams.StringParam.context, context)
          .withParam(GqlParams.ListParam.valueTypeList, expectedTypes.asJava)
          .withParam(GqlParams.StringParam.hint, "")
          .atPosition(pos.offset, pos.line, pos.column)
          .build()
      ).atPosition(pos.offset, pos.line, pos.column)
      .build()

  private val initialState =
    SemanticState.clean
      .withFeature(SemanticFeature.MultipleDatabases)

  private val initialStateWithFeatureFlags =
    SemanticState.clean
      .withFeature(SemanticFeature.MultipleDatabases)
      .withFeature(SemanticFeature.RelationshipPropertyValueAccessRules)
      .withFeature(SemanticFeature.AttributeBasedAccessControl)
      .withFeature(SemanticFeature.UserTags)
      .withFeature(SemanticFeature.ValueInListProperty)

  // Privilege command tests

  testVersions("GRANT WRITE ON GRAPH * TO role, 42") { version =>
    val grantPrivilege = GrantPrivilege(
      GraphPrivilege(
        WriteAction,
        AllGraphsScope()(pos1)
      )(pos2),
      immutable = false,
      None,
      List(LabelAllQualifier()(_)),
      List(literalString("role", pos3), literalInt(42, pos4))
    )(p)

    grantPrivilege.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlWrongType("42", "rolename", Seq("STRING NOT NULL"), pos4),
        initialState,
        "rolename must be a String, or a String parameter.",
        pos4
      ).errors
  }

  testVersions("DENY WRITE ON GRAPH * TO role, 42") { version =>
    val denyPrivilege = DenyPrivilege(
      GraphPrivilege(
        WriteAction,
        AllGraphsScope()(pos1)
      )(pos2),
      immutable = false,
      None,
      List(LabelAllQualifier()(_)),
      List(literalString("role", pos3), literalInt(42, pos4))
    )(p)

    denyPrivilege.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlWrongType("42", "rolename", Seq("STRING NOT NULL"), pos4),
        initialState,
        "rolename must be a String, or a String parameter.",
        pos4
      ).errors
  }

  testVersions("REVOKE WRITE ON GRAPH * FROM role, 42") { version =>
    val revokePrivilege = RevokePrivilege(
      GraphPrivilege(
        WriteAction,
        AllGraphsScope()(pos1)
      )(pos2),
      immutableOnly = false,
      None,
      List(LabelAllQualifier()(_)),
      List(literalString("role", pos3), literalInt(42, pos4)),
      RevokeBothType()(pos)
    )(p)

    revokePrivilege.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlWrongType("42", "rolename", Seq("STRING NOT NULL"), pos4),
        initialState,
        "rolename must be a String, or a String parameter.",
        pos4
      ).errors
  }

  object attrLoader {

    def loadAuthAttributes(): List[AuthAttribute] = {
      val discovered = new Reflections("org.neo4j.cypher.internal.ast")
        .getSubTypesOf(classOf[AuthAttribute])
        .asScala
        .iterator
        .filter(c => !c.isInterface && !Modifier.isAbstract(c.getModifiers))
        .map { clazz =>
          val constructor = clazz.getDeclaredConstructors.minBy(_.getParameterCount)
          constructor.newInstance(mockParams(constructor.getParameterTypes): _*).asInstanceOf[AuthAttribute]
        }
        .toList
      require(
        discovered.nonEmpty,
        "Failed to discover AuthAttribute subclasses via classpath scanning"
      )
      discovered
    }

    def mockParams(paramTypes: Array[Class[_]]): Array[AnyRef] =
      paramTypes.map {
        case java.lang.Integer.TYPE    => Integer.valueOf(0)
        case java.lang.Boolean.TYPE    => java.lang.Boolean.FALSE
        case java.lang.Double.TYPE     => java.lang.Double.valueOf(0.0)
        case java.lang.Long.TYPE       => java.lang.Long.valueOf(0L)
        case t if t == classOf[String] => null
        case _                         => null
      }
  }

  def getGql42N97_missingMandatoryAuthClause(
    clause: String,
    authProvider: String,
    position: Option[InputPosition]
  ): ErrorGqlStatusObject = {
    val builder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N97)
      .withParam(GqlParams.StringParam.clause, clause)
      .withParam(GqlParams.StringParam.auth, authProvider)

    position.map(p => builder.atPosition(p.offset, p.line, p.column)).getOrElse(builder)
      .build()
  }

  def getGql42N19_duplicateClause(clause: String, position: InputPosition): ErrorGqlStatusObject = {
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.offset, position.line, position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N19)
        .withParam(GqlParams.StringParam.syntax, clause)
        .atPosition(position.offset, position.line, position.column)
        .build())
      .build()
  }

  test("allAuthAttributes should be up to date") {
    val allAuthAttributes = attrLoader.loadAuthAttributes().map(_.name).toSet
    assertEquals(
      allAuthAttributes,
      allAuthAttr.allAuthsAttributes.map(_.name).toSet
    ) // Note: if this fails allAuthAttributes in Administration is probably out of date
  }

  // Property Rules

  type QualifierFn = (Option[Variable], Expression) => List[PrivilegeQualifier]
  val allLabelPatternQualifier: QualifierFn = (v, e) => List(PatternQualifier(Seq(LabelAllQualifier()(p)), v, e, Node))

  val singleLabelPatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(LabelQualifier("A")(p)), v, e, Node))

  val multiLabelPatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(LabelQualifier("A")(p), LabelQualifier("B")(p)), v, e, Node))

  val allRelTypePatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(RelationshipAllQualifier()(p)), v, e, Relationship))

  val singleRelTypePatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(RelationshipQualifier("A")(p)), v, e, Relationship))

  val multiRelTypePatternQualifier: QualifierFn =
    (v, e) =>
      List(PatternQualifier(Seq(RelationshipQualifier("A")(p), RelationshipQualifier("B")(p)), v, e, Relationship))

  val mixedList: ListLiteral =
    listOf(literalInt(1), literalString("s"), literalFloat(1.1), falseLiteral, parameter("value1", CTAny))

  Seq(
    (allLabelPatternQualifier, "all labels", Node),
    (singleLabelPatternQualifier, "single label", Node),
    (multiLabelPatternQualifier, "multiple labels", Node),
    (allRelTypePatternQualifier, "all rel types", Relationship),
    (singleRelTypePatternQualifier, "single rel type", Relationship),
    (multiRelTypePatternQualifier, "multiple rel types", Relationship)
  ).foreach {
    case (qualifierFn: QualifierFn, qualifierDescription, element) =>
      // e.g. FOR (n{prop1:val1, prop2:val2}) for node property rules
      // e.g. FOR ()-[r{prop1:val1, prop2:val2}]-() for relationship property rules
      // NOTE: comment examples after this ^ one all cite node property rules only, but are all applicable to relationships too.

      testVersions(
        s"property rules with more than one property should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val privilege = GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            MapExpression(Seq(
              (PropertyKeyName("prop1")(p), StringLiteral("val1")(p)),
              (PropertyKeyName("prop2")(p), StringLiteral("val2")(p))
            ))(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.size shouldBe 1
        val e = result.errors.head
        e.msg shouldBe "Failed to administer property rule. The expression: `{prop1: \"val1\", prop2: \"val2\"}` is not supported. Property rules can only contain one property."
        e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
        e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA2.getStatusString
      }

      Seq(
        ("=", " Use `IS NULL` instead.", (lhs: Expression, rhs: Expression) => Equals(lhs, rhs)(p)),
        ("<>", " Use `IS NOT NULL` instead.", (lhs: Expression, rhs: Expression) => NotEquals(lhs, rhs)(p)),
        (">", "", (lhs: Expression, rhs: Expression) => GreaterThan(lhs, rhs)(p)),
        (">=", "", (lhs: Expression, rhs: Expression) => GreaterThanOrEqual(lhs, rhs)(p)),
        ("<", "", (lhs: Expression, rhs: Expression) => LessThan(lhs, rhs)(p)),
        ("<=", "", (lhs: Expression, rhs: Expression) => LessThanOrEqual(lhs, rhs)(p))
      ).foreach { case (operator, suggestionPartOfErrorMessage, op) =>
        // e.g. FOR (n) WHERE n.prop1 = 1 AND n.prop2 = 1
        testVersions(
          s"property rules using WHERE syntax with multiple predicates via AND should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              And(
                op(
                  Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                  literalInt(1, p)
                ),
                op(
                  Property(varFor("n", p), PropertyKeyName("prop2")(p))(p),
                  literalInt(1, p)
                )
              )(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.msg shouldBe s"Failed to administer property rule. The expression: `n.prop1 $operator 1 AND n.prop2 $operator 1` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
          e.gqlStatusObject.cause().get().statusDescription() shouldBe s"error: data exception - invalid property-based access control rule involving nontrivial predicates. The expression: 'n.prop1 $operator 1 AND n.prop2 $operator 1' is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
        }

        // e.g. FOR (n) WHERE n.prop1 = 1 OR n.prop2 = 1
        testVersions(
          s"property rules using WHERE syntax with multiple predicates via OR should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Or(
                op(
                  Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                  literalInt(1, p)
                ),
                op(
                  Property(varFor("n", p), PropertyKeyName("prop2")(p))(p),
                  literalInt(1, p)
                )
              )(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.msg shouldBe s"Failed to administer property rule. The expression: `n.prop1 $operator 1 OR n.prop2 $operator 1` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
        }

        // e.g. FOR (n) WHERE n.prop1 = NULL
        testVersions(
          s"property rules using n.prop $operator NULL should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(Property(varFor("n", p), PropertyKeyName("prop1")(p))(p), Null.NULL)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          result.errors.head.msg shouldBe s"Failed to administer property rule. The property value access rule pattern `prop1 $operator NULL` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
        }

        // e.g. FOR (n) WHERE NULL = n.prop1
        testVersions(
          s"property rules using NULL $operator n.prop should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(Null.NULL, Property(varFor("n", p), PropertyKeyName("prop1")(p))(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          result.errors.head.msg shouldBe s"Failed to administer property rule. The property value access rule pattern `NULL $operator prop1` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
        }

        // e.g. FOR (n) WHERE NOT n.prop = NULL
        testVersions(
          s"property rules using NOT n.prop $operator NULL should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Not(op(Property(varFor("n", p), PropertyKeyName("prop")(p))(p), Null.NULL))(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          result.errors.head.msg shouldBe s"Failed to administer property rule. The property value access rule pattern `prop $operator NULL` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
        }

        // e.g. FOR (n) WHERE NOT NULL = n.prop
        testVersions(
          s"property rules using NOT NULL $operator n.prop should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Not(op(Null.NULL, Property(varFor("n", p), PropertyKeyName("prop")(p))(p)))(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          result.errors.head.msg shouldBe s"Failed to administer property rule. The property value access rule pattern `NULL $operator prop` always evaluates to `NULL`.$suggestionPartOfErrorMessage"
        }

        // e.g. FOR (n) WHERE n.prop1 = NaN
        testVersions(
          s"property rules using n.prop $operator NaN should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(Property(varFor("n", p), PropertyKeyName("prop1")(p))(p), NaN()(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          result.errors.head.msg shouldBe "Failed to administer property rule. `NaN` is not supported for property-based access control."
          result.errors.head.gqlStatusObject should be(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22NA0,
              "error: data exception - invalid property-based access control rule. Failed to administer property rule."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22NA3,
                "error: data exception - invalid property-based access control rule involving NaN. 'NaN' is not supported for property-based access control."
              )
          )
        }

        // e.g. FOR (n) WHERE NaN = n.prop1
        testVersions(
          s"property rules using NaN $operator n.prop should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(NaN()(p), Property(varFor("n", p), PropertyKeyName("prop1")(p))(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          result.errors.head.msg shouldBe "Failed to administer property rule. `NaN` is not supported for property-based access control."
          result.errors.head.gqlStatusObject should be(gqlStatus(
            GqlStatusInfoCodes.STATUS_22NA0,
            "error: data exception - invalid property-based access control rule. Failed to administer property rule."
          )
            .withCause(
              GqlStatusInfoCodes.STATUS_22NA3,
              "error: data exception - invalid property-based access control rule involving NaN. 'NaN' is not supported for property-based access control."
            ))
        }

        // e.g. FOR (n) WHERE n.prop1 = 1+2
        testVersions(
          s"property rules using WHERE syntax with non-literal predicates should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                Add(literalInt(1, p), literalInt(2, p))(p)
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.msg shouldBe s"Failed to administer property rule. The expression: `n.prop1 $operator 1 + 2` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
        }

        // e.g. FOR (n) WHERE n.prop1 = date.realtime()
        testVersions(
          s"property rules using WHERE syntax with sub functions of temporal functions should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                prop(varFor("n"), "prop1"),
                function("date.realtime")
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.msg shouldBe s"Failed to administer property rule. The expression: `n.prop1 $operator `date.realtime`()` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
        }

        // e.g. FOR (n) WHERE n.prop1 = point({x: 1, y: 2})
        // `point` is the only allow-listed function that is a compiler built-in; the temporal
        // functions (date, datetime, ...) only become built-ins once resolved, so they are covered
        // in AdministrationCommandResolvedFunctionSemanticAnalysisTest.
        testVersions(
          s"property rules using WHERE syntax with an allow-listed built-in function should pass semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                prop(varFor("n"), "prop1"),
                function("point", mapOfInt("x" -> 1, "y" -> 2))
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.isEmpty shouldBe true
        }

        // e.g. FOR (n) WHERE n.prop1 = POINT({x: 1, y: 2})
        testVersions(
          s"property rules using WHERE syntax with an allow-listed built-in function are case-insensitive ($qualifierDescription)($operator)"
        ) { version =>
          Seq[(String, Expression)](
            ("POINT", function("POINT", mapOfInt("x" -> 1, "y" -> 2)))
          ).foreach { case (name, call) =>
            withClue(s"$name: ") {
              val privilege = GrantPrivilege(
                GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
                false,
                None,
                qualifierFn(
                  Some(varFor("n", p)),
                  op(
                    prop(varFor("n"), "prop1"),
                    call
                  )
                ),
                Seq(literalString("role1"))
              )(p)

              val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
              result.errors.isEmpty shouldBe true
            }
          }
        }

        // e.g. FOR (n) WHERE n.prop1 = toLower('x') — built-in but not allow-listed
        testVersions(
          s"property rules using WHERE syntax with non-allow-listed built-in functions should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          Seq[(String, Expression)](
            ("toLower", function("toLower", literalString("X"))),
            ("coalesce", function("coalesce", literalString("a"), literalString("b")))
          ).foreach { case (name, call) =>
            withClue(s"$name: ") {
              val privilege = GrantPrivilege(
                GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
                false,
                None,
                qualifierFn(
                  Some(varFor("n", p)),
                  op(
                    prop(varFor("n"), "prop1"),
                    call
                  )
                ),
                Seq(literalString("role1"))
              )(p)

              val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
              result.errors.size shouldBe 1
              val e = result.errors.head
              e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
              e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
              e.msg should startWith("Failed to administer property rule.")
              e.msg should include(
                "is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
              )
            }
          }
        }

        // e.g. FOR (n) WHERE n.prop1 = my.test.func() — namespaced UDF call
        testVersions(
          s"property rules using WHERE syntax with a namespaced user-defined function should fail semantic checking on DENY ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = DenyPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                prop(varFor("n"), "prop1"),
                function(Seq("my", "test"), "func")
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
          e.msg should startWith("Failed to administer property rule.")
          e.msg should include(
            "is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          )
        }

        // e.g. FOR (n) WHERE n.prop1 = my.test.date() — namespaced UDF whose tail name shadows an allow-listed temporal
        testVersions(
          s"property rules using WHERE syntax with a namespaced user-defined function shadowing a temporal name should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                prop(varFor("n"), "prop1"),
                function(Seq("my", "test"), "date")
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
          e.msg should startWith("Failed to administer property rule.")
          e.msg should include(
            "is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          )
        }

        // e.g. FOR (n) WHERE n.prop1 = point({x: 1, y: 2}) where point() is shadowed
        testVersions(
          s"property rules using WHERE syntax with a shadowed allow-listed built-in function should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(
                prop(varFor("n"), "prop1"),
                function("point", mapOfInt("x" -> 1, "y" -> 2)).copy(isShadowed = true)(p)
              )
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
          e.msg should startWith("Failed to administer property rule.")
          e.msg should include(
            "is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          )
        }

        // e.g. FOR (n) WHERE n.prop1 = [1, 2]
        testVersions(
          s"property rules using WHERE syntax with List of literals should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val expressionStringifier = ExpressionStringifier()
          Seq(
            // List of ints
            op(prop(varFor("n"), "prop1"), listOfInt(1, 2)), // n.prop = [1, 2]

            // List of strings
            op(prop(varFor("n"), "prop1"), listOfString("s1", "s2")), // n.prop = ['s1', 's2']

            // List of booleans
            op(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral)), // n.prop = [true, false]

            // List of floats
            op(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1), literalFloat(2.2))), // n.prop = [1.1, 2.2]

            // List of parameters
            op(
              prop(varFor("n"), "prop1"),
              listOf(parameter("value", CTAny), parameter("value2", CTAny))
            ), // n.prop = [$value, $value2]

            // List of temporal values
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("date", literalString("2024-10-09")),
                function("date", literalString("2024-10-11"))
              )
            ), // n.prop = [date("2024-10-09"), date("2024-10-11")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("datetime", literalString("2024-10-09T12:10:09:40+02:00")),
                function("datetime", literalString("2024-10-11T11:10:09:40+02:00"))
              )
            ), // n.prop = [datetime("2024-10-09T12:10:09:40+02:00"), datetime("2024-10-11T11:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localdatetime", literalString("2024-10-09T12:50:35.5")),
                function("localdatetime", literalString("2024-10-09T12:55:35.5"))
              )
            ), // n.prop = [localdatetime("2024-10-09T12:50:35.5"), localdatetime("2024-10-09T12:55:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("time", literalString("12:10:09:40+02:00")),
                function("time", literalString("11:10:09:40+02:00"))
              )
            ), // n.prop = [time("12:10:09:40+02:00"), time("11:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localtime", literalString("12:50:35.5")),
                function("localtime", literalString("12:55:35.5"))
              )
            ), // n.prop = [localtime("2024-10-09T12:50:35.5"), localtime("2024-10-09T12:55:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("duration", literalString("PT1S")),
                function("duration", literalString("PT2S"))
              )
            ), // n.prop = [duration("PT1S"), duration("PT2S")]

            // List of points
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2)),
                function("point", mapOfInt("x" -> 3, "y" -> 4))
              )
            ), // n.prop = [point({x: 1, y: 2}), point({x: 3, y: 4})]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)),
                function("point", mapOfInt("x" -> 3, "y" -> 4, "z" -> 2))
              )
            ), // n.prop = [point({x: 1, y: 2, z: 1}), point({x: 3, y: 4, z: 2})]

            // Mixed list
            op(prop(varFor("n"), "prop1"), mixedList) // n.prop = [1, 's', 1.1, false, $value1]
          ).foreach { expression =>
            withClue(expressionStringifier(expression)) {
              val privilege = GrantPrivilege(
                GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
                false,
                None,
                qualifierFn(
                  Some(varFor("n", p)),
                  expression
                ),
                Seq(literalString("role1"))
              )(p)

              val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
              result.errors.size shouldBe 1
              val e = result.errors.head
              e.msg shouldBe "Failed to administer property rule. " +
                s"The expression: `${expressionStringifier(expression)}` is not supported. " +
                "Only single, literal-based predicate expressions are allowed for property-based access control."
              e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
              e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
            }
          }
        }

        // e.g. FOR (n) WHERE n.prop1 = [1]
        testVersions(
          s"property rules using WHERE syntax with single-item list of literals should fail semantic checking ($qualifierDescription)($operator)"
        ) { version =>
          val expressionStringifier = ExpressionStringifier()
          Seq(
            // List of ints
            op(prop(varFor("n"), "prop1"), listOfInt(1)), // n.prop = [1]

            // List of strings
            op(prop(varFor("n"), "prop1"), listOfString("s1")), // n.prop = ['s1']

            // List of booleans
            op(prop(varFor("n"), "prop1"), listOf(trueLiteral)), // n.prop = [true]

            // List of floats
            op(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1))), // n.prop = [1.1]

            // List of temporal values
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("date", literalString("2024-10-09"))
              )
            ), // n.prop = [date("2024-10-09")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("datetime", literalString("2024-10-09T12:10:09:40+02:00"))
              )
            ), // n.prop = [datetime("2024-10-09T12:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localdatetime", literalString("2024-10-09T12:50:35.5"))
              )
            ), // n.prop = [localdatetime("2024-10-09T12:50:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("time", literalString("12:10:09:40+02:00"))
              )
            ), // n.prop = [time("12:10:09:40+02:00")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("localtime", literalString("12:50:35.5"))
              )
            ), // n.prop = [localtime("2024-10-09T12:50:35.5")]
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("duration", literalString("PT1S"))
              )
            ), // n.prop = [duration("PT1S")]

            // List of points
            op(
              prop(varFor("n"), "prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2))
              )
            ), // n.prop = [point({x: 1, y: 2})]

            // List of parameters
            op(
              prop(varFor("n"), "prop1"),
              listOf(parameter("value", CTAny))
            ) // n.prop = [$value]
          ).foreach { expression =>
            withClue(expressionStringifier(expression)) {
              val privilege = GrantPrivilege(
                GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
                false,
                None,
                qualifierFn(
                  Some(varFor("n", p)),
                  expression
                ),
                Seq(literalString("role1"))
              )(p)

              val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
              result.errors.size shouldBe 1
              val e = result.errors.head
              e.msg shouldBe "Failed to administer property rule. " +
                s"The expression: `${expressionStringifier(expression)}` is not supported. " +
                "Only single, literal-based predicate expressions are allowed for property-based access control."
              e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
              e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
            }
          }
        }

        // e.g. FOR (n) WHERE NOT NOT n.prop1 = 1
        testVersions(
          s"using more than one NOT keyword combined with an '$operator' should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              Not(Not(op(
                Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
                literalInt(1, p)
              ))(p))(p)
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.msg shouldBe s"Failed to administer property rule. The expression: `NOT (NOT n.prop1 $operator 1)` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
        }

        // e.g. FOR (n) WHERE 1 = n.prop1
        testVersions(
          s"property rules having n.prop on right hand side of operator $operator should fail semantic checking ($qualifierDescription)"
        ) { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              Some(varFor("n", p)),
              op(literalInt(1, p), Property(varFor("n", p), PropertyKeyName("prop1")(p))(p))
            ),
            Seq(literalString("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          val e = result.errors.head
          e.msg shouldBe s"Failed to administer property rule. The property `prop1` must appear on the left hand side of the `$operator` operator."
          e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
          e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA1.getStatusString
        }
      }

      // e.g. FOR ({n:NULL})
      testVersions(
        s"property rules NULL in map syntax should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val privilege = GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(None, MapExpression(Seq((PropertyKeyName("prop1")(p), Null.NULL)))(p)),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.size shouldBe 1
        result.errors.head.msg shouldBe "Failed to administer property rule. The property value access rule pattern `{prop1:NULL}` always evaluates to `NULL`. Use `WHERE` syntax in combination with `IS NULL` instead."
      }

      // e.g. FOR ({prop1:1+2})
      testVersions(
        s"property rules using map syntax with non-literal predicates should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val privilege = GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            MapExpression(Seq((
              PropertyKeyName("prop1")(p),
              Add(literalInt(1, p), literalInt(2, p))(p)
            )))(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.size shouldBe 1
        val e = result.errors.head
        e.msg shouldBe "Failed to administer property rule. The expression: `{prop1: 1 + 2}` is not supported. Only single, literal-based predicate expressions are allowed for property-based access control."
        e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
        e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
      }

      // e.g. FOR (n {prop1: [1, 2]})
      testVersions(
        s"property rules using map expression syntax with List of literals should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val expressionStringifier = ExpressionStringifier()
        Seq(
          // List of ints
          MapExpression(Seq((propName("prop1"), listOfInt(1, 2))))(p), // {prop1: [1, 2]}

          // List of strings
          MapExpression(Seq((propName("prop1"), listOfString("s1", "s2"))))(p), // {prop1: ['s1', 's2']}

          // List of booleans
          MapExpression(Seq((propName("prop1"), listOf(trueLiteral, falseLiteral))))(p), // {prop1: [true, false]}

          // List of floats
          MapExpression(
            Seq((propName("prop1"), listOf(literalFloat(1.1), literalFloat(1.2))))
          )(p), // {prop1: [1.1, 2.2]}

          // List of temporal values
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("date", literalString("2024-10-09")),
                function("date", literalString("2024-10-11"))
              )
            ))
          )(p), // {prop1: [date("2024-10-09"), date("2024-10-11")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("datetime", literalString("2024-10-09T12:10:09:40+02:00")),
                function("datetime", literalString("2024-10-11T11:10:09:40+02:00"))
              )
            ))
          )(p), // {prop1: [datetime("2024-10-09T12:10:09:40+02:00"), datetime("2024-10-11T11:10:09:40+02:00")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("localdatetime", literalString("2024-10-09T12:50:35.5")),
                function("localdatetime", literalString("2024-10-09T12:55:35.5"))
              )
            ))
          )(p), // {prop1: [localdatetime("2024-10-09T12:50:35.5"), localdatetime("2024-10-09T12:55:35.5")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("time", literalString("12:10:09:40+02:00")),
                function("time", literalString("11:10:09:40+02:00"))
              )
            ))
          )(p), // {prop1: [time("12:10:09:40+02:00"), time("11:10:09:40+02:00")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("localtime", literalString("12:50:35.5")),
                function("localtime", literalString("12:55:35.5"))
              )
            ))
          )(p), // {prop1: [localtime("2024-10-09T12:50:35.5"), localtime("2024-10-09T12:55:35.5")]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("duration", literalString("PT1S")),
                function("duration", literalString("PT2S"))
              )
            ))
          )(p), // {prop1: [duration("PT1S"), duration("PT2S")]}

          // List of points
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2)),
                function("point", mapOfInt("x" -> 3, "y" -> 4))
              )
            ))
          )(p), // {prop1: [point({x: 1, y: 2}), point({x: 3, y: 4})]}
          MapExpression(
            Seq((
              propName("prop1"),
              listOf(
                function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)),
                function("point", mapOfInt("x" -> 3, "y" -> 4, "z" -> 2))
              )
            ))
          )(p), // {prop1: [point({x: 1, y: 2, z: 1}), point({x: 3, y: 4, z: 2})]}

          // List of parameters
          MapExpression(
            Seq((propName("prop1"), listOf(parameter("value", CTAny), parameter("value2", CTAny))))
          )(p), // {prop1: [$value, $value2]}

          // Mixed list
          MapExpression(Seq((propName("prop1"), mixedList)))(p) // {prop1: [1, 's', 1.1, false, $value1]}

        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
            result.errors.size shouldBe 1
            val e = result.errors.head
            e.msg shouldBe "Failed to administer property rule. " +
              s"The expression: `${expressionStringifier(expression)}` is not supported. " +
              "Only single, literal-based predicate expressions are allowed for property-based access control."
            e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
            e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
          }
        }
      }

      // e.g. FOR (n {prop1: [1]})
      testVersions(
        s"property rules using map expression syntax with single-item list of literals should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val expressionStringifier = ExpressionStringifier()
        Seq(
          // List of ints
          MapExpression(Seq((propName("prop1"), listOfInt(1))))(p), // {prop1: [1]}

          // List of strings
          MapExpression(Seq((propName("prop1"), listOfString("s1"))))(p), // {prop1: ['s1']}

          // List of booleans
          MapExpression(Seq((propName("prop1"), listOf(trueLiteral))))(p), // {prop1: [true]}

          // List of floats
          MapExpression(Seq((propName("prop1"), listOf(literalFloat(1.1)))))(p), // {prop1: [1.1]}

          // List of temporal values
          MapExpression(Seq((propName("prop1"), listOf(function("date", literalString("2024-10-09"))))))(
            p
          ), // {prop1: [date("2024-10-09")]}
          MapExpression(Seq((
            propName("prop1"),
            listOf(function("datetime", literalString("2024-10-09T12:10:09:40+02:00")))
          )))(p), // {prop1: [datetime("2024-10-09T12:10:09:40+02:00")]}
          MapExpression(Seq((
            propName("prop1"),
            listOf(function("localdatetime", literalString("2024-10-09T12:50:35.5")))
          )))(p), // {prop1: [localdatetime("2024-10-09T12:50:35.5")]}
          MapExpression(Seq((propName("prop1"), listOf(function("time", literalString("12:10:09:40+02:00"))))))(
            p
          ), // {prop1: [time("12:10:09:40+02:00")]}
          MapExpression(Seq((propName("prop1"), listOf(function("localtime", literalString("12:50:35.5"))))))(
            p
          ), // {prop1: [localtime("2024-10-09T12:50:35.5")]}
          MapExpression(Seq((propName("prop1"), listOf(function("duration", literalString("PT1S"))))))(
            p
          ), // {prop1: [duration("PT1S")]}

          // List of points
          MapExpression(Seq((propName("prop1"), listOf(function("point", mapOfInt("x" -> 1, "y" -> 2))))))(
            p
          ), // {prop1: [point({x: 1, y: 2})]}
          MapExpression(Seq((propName("prop1"), listOf(function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1))))))(
            p
          ), // {prop1: [point({x: 1, y: 2, z: 1})]}

          // List of parameters
          MapExpression(Seq((propName("prop1"), listOf(parameter("value", CTAny)))))(p) // {prop1: [$value]}
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
            result.errors.size shouldBe 1
            val e = result.errors.head
            e.msg shouldBe "Failed to administer property rule. " +
              s"The expression: `${expressionStringifier(expression)}` is not supported. " +
              "Only single, literal-based predicate expressions are allowed for property-based access control."
            e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
            e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1]
      testVersions(
        s"property rules using WHERE syntax with property IN List of one literal should pass semantic checking($qualifierDescription)"
      ) { version =>
        val expressionStringifier = ExpressionStringifier()

        Seq(
          // List of ints
          In(prop(varFor("n"), "prop1"), listOfInt(1))(p), // n.prop IN [1]
          Not(In(prop(varFor("n"), "prop1"), listOfInt(1))(p))(p), // NOT n.prop IN [1]

          // List of strings
          In(prop(varFor("n"), "prop1"), listOfString("s1"))(p), // n.prop IN ['s1']
          Not(In(prop(varFor("n"), "prop1"), listOfString("s1"))(p))(p), // NOT n.prop IN ['s1']

          // List of booleans
          In(prop(varFor("n"), "prop1"), listOf(trueLiteral))(p), // n.prop IN [true]
          Not(In(prop(varFor("n"), "prop1"), listOf(trueLiteral))(p))(p), // NOT n.prop IN [true]

          // List of floats
          In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1)))(p), // n.prop IN [1.1]
          Not(In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1)))(p))(p), // NOT n.prop IN [1.1]

          // List of built-in function values (point is the only allow-listed compiler built-in;
          // the temporal functions are covered as resolved functions in
          // AdministrationCommandResolvedFunctionSemanticAnalysisTest)
          In(
            prop(varFor("n"), "prop1"),
            listOf(function("point", mapOfInt("x" -> 1, "y" -> 2)))
          )(p), // n.prop IN [point({x: 1, y: 2})]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)))
          )(p))(p), // NOT n.prop IN [point({x: 1, y: 2, z: 1})]

          // List of parameters
          In(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny)))(p), // n.prop IN [$value]
          Not(In(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny)))(p))(p), // NOT n.prop IN [$value]

          // Parameter list
          In(prop(varFor("n"), "prop1"), parameter("value", CTList(CTAny)))(p), // n.prop IN $paramList
          Not(In(prop(varFor("n"), "prop1"), parameter("value", CTList(CTAny)))(p))(p) // NOT n.prop IN $paramList
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
            result.errors.isEmpty shouldBe true
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1, 2]
      testVersions(
        s"property rules using WHERE syntax with property IN List of more than one literal should pass semantic checking($qualifierDescription)"
      ) { version =>
        val expressionStringifier = ExpressionStringifier()

        Seq(
          // List of ints
          In(prop(varFor("n"), "prop1"), listOfInt(1, 2))(p), // n.prop IN [1, 2]
          Not(In(prop(varFor("n"), "prop1"), listOfInt(1, 2))(p))(p), // NOT n.prop IN [1, 2]

          // List of strings
          In(prop(varFor("n"), "prop1"), listOfString("s1", "s2"))(p), // n.prop IN ['s1', 's2']
          Not(In(prop(varFor("n"), "prop1"), listOfString("s1", "s2"))(p))(p), // NOT n.prop IN ['s1', 's2']

          // List of booleans
          In(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral))(p), // n.prop IN [true, false]
          Not(In(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral))(p))(p), // NOT n.prop IN [true, false]

          // List of floats
          In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1), literalFloat(2.2)))(p), // n.prop IN [1.1, 2.2]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(literalFloat(1.1), literalFloat(2.2))
          )(p))(p), // NOT n.prop IN [1.1, 2.2]

          // List of built-in function values (point is the only allow-listed compiler built-in;
          // the temporal functions are covered as resolved functions in
          // AdministrationCommandResolvedFunctionSemanticAnalysisTest)
          In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("point", mapOfInt("x" -> 1, "y" -> 2)),
              function("point", mapOfInt("x" -> 3, "y" -> 4))
            )
          )(p), // n.prop IN [point({x: 1, y: 2}), point({x: 3, y: 4})]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(
              function("point", mapOfInt("x" -> 1, "y" -> 2, "z" -> 1)),
              function("point", mapOfInt("x" -> 3, "y" -> 4, "z" -> 2))
            )
          )(p))(p), // NOT n.prop IN [point({x: 1, y: 2, z: 1}), point({x: 3, y: 4, z: 2})]

          // List of parameters
          In(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny), parameter("value2", CTAny))
          )(p), // n.prop IN [$value, $value2]
          Not(In(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny), parameter("value2", CTAny))
          )(p))(p), // NOT n.prop IN [$value, $value2]

          // Mixed list
          In(prop(varFor("n"), "prop1"), mixedList)(p), // n.prop IN [1, 's', 1.1, false, $value1]
          Not(In(prop(varFor("n"), "prop1"), mixedList)(p))(p) // NOT n.prop IN [1, 's', 1.1, false, $value1]

        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(varFor("n", p)),
                expression
              ),
              Seq(literalString("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
            result.errors.isEmpty shouldBe true
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1, [2]]
      testVersions(
        s"property rules using WHERE syntax with property IN List of literal value and non literal value should fail semantic checking($qualifierDescription)"
      ) { version =>
        val expressionStringifier = ExpressionStringifier()
        val expression =
          In(
            prop(varFor("n"), "prop1"),
            listOf(literalInt(1), listOfString("stringValue"))
          )(p) // n.prop IN [1, ['stringValue']]

        val privilege = GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(Some(varFor("n", p)), expression),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.size shouldBe 1
        result.errors.head.msg shouldBe "Failed to administer property rule. " +
          s"The expression: `${expressionStringifier(expression)}` is not supported. " +
          "All elements in a list must be literals of the same type for property-based access control."
        result.errors.head.gqlStatusObject should be(
          gqlStatus(
            GqlStatusInfoCodes.STATUS_22NA0,
            "error: data exception - invalid property-based access control rule. Failed to administer property rule."
          )
            .withCause(
              GqlStatusInfoCodes.STATUS_22NAB,
              "error: data exception - mixed type list for property-based access control rule. " +
                s"The expression '${expressionStringifier(expression)}' is not supported. " +
                "All elements in a list must be literals of the same type for property-based access control."
            )
        )
      }

      // e.g. FOR (n) WHERE 1 IN n.prop1 — a scalar tested for membership in a list-valued property
      test(
        s"property rules using WHERE syntax with scalar IN property should pass semantic checking($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        Seq(
          In(literalInt(1), prop(varFor("n"), "prop1"))(p), // 1 IN n.prop
          Not(In(literalInt(1), prop(varFor("n"), "prop1"))(p))(p), // NOT 1 IN n.prop
          In(literalString("s1"), prop(varFor("n"), "prop1"))(p), // 's1' IN n.prop
          In(trueLiteral, prop(varFor("n"), "prop1"))(p), // true IN n.prop
          In(literalFloat(1.1), prop(varFor("n"), "prop1"))(p), // 1.1 IN n.prop
          In(function("point", mapOfInt("x" -> 1, "y" -> 2)), prop(varFor("n"), "prop1"))(p), // point(...) IN n.prop
          In(parameter("value", CTAny), prop(varFor("n"), "prop1"))(p), // $value IN n.prop
          Not(In(parameter("value", CTAny), prop(varFor("n"), "prop1"))(p))(p) // NOT $value IN n.prop
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(Some(varFor("n", p)), expression),
              Seq(literalString("role1"))
            )(p)

            val result =
              privilege.semanticCheck.run(
                initialStateWithFeatureFlags,
                versionedSemanticContext(CypherVersion.Cypher25)
              )
            result.errors.isEmpty shouldBe true
          }
        }
      }

      // value IN a list property is gated behind the ValueInListProperty feature flag
      test(
        s"property rules using WHERE syntax with scalar IN property should fail when the feature is disabled($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        val stateWithoutValueInListProperty =
          SemanticState.clean
            .withFeature(SemanticFeature.MultipleDatabases)

        Seq(
          In(literalInt(1), prop(varFor("n"), "prop1"))(p), // 1 IN n.prop
          Not(In(literalInt(1), prop(varFor("n"), "prop1"))(p))(p), // NOT 1 IN n.prop
          Not(Not(In(literalInt(1), prop(varFor("n"), "prop1"))(p))(p))(p) // NOT (NOT 1 IN n.prop)
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(Some(varFor("n", p)), expression),
              Seq(literalString("role1"))
            )(p)

            val result =
              privilege.semanticCheck.run(
                stateWithoutValueInListProperty,
                versionedSemanticContext(CypherVersion.Cypher25)
              )
            result.errors.map(_.msg) should contain(
              "The `GRANT TRAVERSE` clause using a `<value> IN <property>` predicate is not available in this implementation of Cypher " +
                "due to lack of support for access rules checking for a value in a list property."
            )
          }
        }
      }

      // e.g. FOR (n) WHERE NULL IN n.prop1 — value-in-list-property rejects NULL/NaN scalars and list-on-left, like other PBAC rules
      test(
        s"property rules using WHERE syntax with invalid scalar IN property should fail semantic checking($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        Seq(
          (
            In(nullLiteral, prop(varFor("n"), "prop1"))(p), // NULL IN n.prop
            "The property value access rule pattern `NULL IN prop1` always evaluates to `NULL`.",
            GqlStatusInfoCodes.STATUS_22NA4
          ),
          (
            In(NaN()(p), prop(varFor("n"), "prop1"))(p), // NaN IN n.prop
            "`NaN` is not supported for property-based access control.",
            GqlStatusInfoCodes.STATUS_22NA3
          ),
          (
            In(listOfInt(1, 2), prop(varFor("n"), "prop1"))(p), // [1, 2] IN n.prop (list on left)
            "Only single, literal-based predicate expressions are allowed for property-based access control.",
            GqlStatusInfoCodes.STATUS_22NA7
          ),
          (
            Not(Not(Not(In(literalInt(1), prop(varFor("n"), "prop1"))(p))(p))(p))(p), // NOT (NOT (NOT 1 IN n.prop))
            "Only single, literal-based predicate expressions are allowed for property-based access control.",
            GqlStatusInfoCodes.STATUS_22NA7
          )
        ).foreach { case (expression, reason, causeCode) =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(Some(varFor("n", p)), expression),
              Seq(literalString("role1"))
            )(p)

            val result =
              privilege.semanticCheck.run(
                initialStateWithFeatureFlags,
                versionedSemanticContext(CypherVersion.Cypher25)
              )
            result.errors.size shouldBe 1
            result.errors.head.msg should endWith(reason)
            result.errors.head.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
            result.errors.head.gqlStatusObject.cause().get().gqlStatus() shouldBe causeCode.getStatusString
          }
        }
      }

      // `<value> IN n.property` is only supported in Cypher 25; under Cypher 5 it falls through to the generic 22NA7 rejection
      test(
        s"property rules using WHERE syntax with scalar IN property should fail semantic checking in Cypher 5($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        Seq(
          In(literalInt(1), prop(varFor("n"), "prop1"))(p), // 1 IN n.prop
          Not(In(literalInt(1), prop(varFor("n"), "prop1"))(p))(p), // NOT 1 IN n.prop
          In(literalString("s1"), prop(varFor("n"), "prop1"))(p), // 's1' IN n.prop
          In(parameter("value", CTAny), prop(varFor("n"), "prop1"))(p), // $value IN n.prop
          In(nullLiteral, prop(varFor("n"), "prop1"))(p), // NULL IN n.prop
          In(NaN()(p), prop(varFor("n"), "prop1"))(p) // NaN IN n.prop
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(Some(varFor("n", p)), expression),
              Seq(literalString("role1"))
            )(p)

            val stateWithoutValueInListProperty =
              SemanticState.clean
                .withFeature(SemanticFeature.MultipleDatabases)
                .withFeature(SemanticFeature.RelationshipPropertyValueAccessRules)
                .withFeature(SemanticFeature.AttributeBasedAccessControl)
                .withFeature(SemanticFeature.UserTags)
            Seq(initialStateWithFeatureFlags, stateWithoutValueInListProperty).foreach { state =>
              val result =
                privilege.semanticCheck.run(state, versionedSemanticContext(CypherVersion.Cypher5))
              result.errors.size shouldBe 1
              result.errors.head.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
              result.errors.head.gqlStatusObject
                .cause()
                .get()
                .gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
            }
          }
        }
      }

      // e.g. FOR (node) WHERE n.prop1 = 1
      testVersions(
        s"property rules using WHERE syntax using two different variable names should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val privilege = GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(varFor("node", p)),
            Equals(
              Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
              literalInt(1, p)
            )(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.size shouldBe 1
        result.errors.head.msg shouldBe "Variable `n` not defined"
      }

      // e.g. FOR () WHERE n.prop1 = 1
      testVersions(
        s"property rules using WHERE syntax with no variable should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val privilege = GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            Equals(
              Property(varFor("n", p), PropertyKeyName("prop1")(p))(p),
              literalInt(1, p)
            )(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.size shouldBe 1
        result.errors.head.msg shouldBe "Variable `n` not defined"
      }

      // e.g. FOR (n) WHERE 1 = n.prop1 (foo) TO role
      testVersions(
        s"Valid property rule, extra (foo) gets parsed as a function and should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val privilege = GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(varFor("n", p)),
            Equals(
              literalInt(1, p),
              FunctionInvocation(
                FunctionName(Namespace(List("n"))(p), "prop1")(p),
                distinct = false,
                Vector(varFor("foo", p))
              )(p)
            )(p)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.exists(s =>
          s.msg == "Failed to administer property rule. " +
            "The expression: `1 = n.prop1(foo)` is not supported. " +
            "Only single, literal-based predicate expressions are allowed for property-based access control."
            && s.asInstanceOf[SemanticError].gqlStatusObject.gqlStatus() == "22NA0"
            && s.asInstanceOf[SemanticError].gqlStatusObject.cause().isPresent
            && s.asInstanceOf[SemanticError].gqlStatusObject.cause().get().gqlStatus() == "22NA7"
        ) shouldBe true
      }

      // e.g. FOR (n:A WHERE EXISTS { MATCH (n) }) TO role1
      testVersions(
        s"EXIST MATCH pattern in property rule should fail semantic checking ($qualifierDescription)"
      ) { version =>
        val privilege = GrantPrivilege(
          GraphPrivilege(ReadAction, AllGraphsScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(varFor("n", p)),
            ExistsExpression(
              SingleQuery(
                List(
                  Match(
                    optional = false,
                    MatchMode.DifferentRelationships(implicitlyCreated = true)(p),
                    ForMatch(List(PrefixedPatternPart(
                      AllPaths()(p),
                      PathPatternPart(
                        element match {
                          case Node => NodePattern(Some(varFor("n", pos)), None, None, None)(p)
                          case Relationship => RelationshipChain(
                              NodePattern(None, None, None, None)(pos),
                              RelationshipPattern(
                                Some(varFor("n", pos)),
                                None,
                                None,
                                None,
                                None,
                                SemanticDirection.OUTGOING
                              )(p),
                              NodePattern(None, None, None, None)(pos)
                            )(p)
                        }
                      )
                    )))(p),
                    List(),
                    None,
                    None
                  )(p)
                )
              )(p)
            )(p, None, None)
          ),
          Seq(literalString("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
        result.errors.size shouldBe 1
        val e = result.errors.head
        val pattern = element match {
          case Node         => "(n)"
          case Relationship => "()-[n]->()"
        }
        e.msg shouldBe s"Failed to administer property rule. " +
          s"The expression: `EXISTS { MATCH $pattern }` is not supported. " +
          s"Only single, literal-based predicate expressions are allowed for property-based access control."
        e.gqlStatusObject.gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA0.getStatusString
        e.gqlStatusObject.cause().get().gqlStatus() shouldBe GqlStatusInfoCodes.STATUS_22NA7.getStatusString
      }

      Seq(
        AllGraphAction,
        MergeAdminAction,
        CreateElementAction,
        DeleteElementAction,
        SetLabelAction,
        RemoveLabelAction,
        SetPropertyAction,
        WriteAction
      ).foreach(invalidAction => {
        testVersions(s"invalid actions: $invalidAction for property rules ($qualifierDescription)") { version =>
          val privilege = GrantPrivilege(
            GraphPrivilege(invalidAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              None,
              MapExpression(Seq(
                (PropertyKeyName("prop1")(p), StringLiteral("val1")(p.withInputLength(0)))
              ))(p)
            ),
            Seq(literalString("role1"))
          )(p)
          val result = privilege.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version))
          result.errors.size shouldBe 1
          result.errors.head.msg shouldBe s"${invalidAction.name} is not supported for property value access rules."
        }
      })
  }

  // User command tests

  private def authId(id: String)(p: InputPosition): AuthId = authId(literalString(id))(p)
  private def authId(id: Expression)(p: InputPosition): AuthId = AuthId(id)(p)

  private def password(pass: Expression, isEncrypted: Boolean = false)(p: InputPosition): Password =
    Password(pass, isEncrypted)(p)

  private def passwordChange(requireChange: Boolean)(p: InputPosition): PasswordChange =
    PasswordChange(requireChange)(p)

  private val password: SensitiveStringLiteral =
    SensitiveStringLiteral("password".getBytes(StandardCharsets.UTF_8))(p)

  private val passwordEmpty: SensitiveStringLiteral =
    SensitiveStringLiteral("".getBytes(StandardCharsets.UTF_8))(p)
  private val paramPassword: Parameter = parameter("password", CTString)

  testVersions("CREATE USER foo SET PASSWORD 'password' SET PASSWORD 'password'") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)(pos1), password(password)(pos2)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos2),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  testVersions("CREATE USER foo SET PASSWORD $password SET PASSWORD 'password'") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(paramPassword)(pos1), password(password)(pos2)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos2),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  testVersions("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(),
      Some(Auth("native", List(password(password)(pos)))(pos))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.offset, p.line, p.column),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  testVersions("CREATE OR REPLACE USER foo IF NOT EXISTS SET AUTH 'native' { SET PASSWORD 'password' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("native", List(password(password)(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.offset, p.line, p.column),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  testVersions("CREATE OR REPLACE USER foo IF NOT EXISTS SET AUTH 'foo' { SET ID 'bar' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("foo", List(authId("bar")(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.offset, p.line, p.column),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  testVersions("CREATE OR REPLACE USER $foo IF NOT EXISTS SET PASSWORD 'password'") { version =>
    val createUser = CreateUser(
      parameter("foo", CTString),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(),
      Some(Auth("native", List(password(password)(pos)))(pos))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.offset, p.line, p.column),
          "Failed to create the specified user '$foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  testVersions("CREATE OR REPLACE USER $foo IF NOT EXISTS SET AUTH 'native' { SET PASSWORD 'password' }") { version =>
    val createUser = CreateUser(
      parameter("foo", CTString),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("native", List(password(password)(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.offset, p.line, p.column),
          "Failed to create the specified user '$foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  testVersions("CREATE OR REPLACE USER $foo IF NOT EXISTS SET AUTH 'foo' { SET ID 'bar' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("foo", List(authId("bar")(pos)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.offset, p.line, p.column),
          "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
          p
        )
      ).errors
  }

  testVersions("CREATE USER foo SET PASSWORD CHANGE REQUIRED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  testVersions("CREATE USER foo SET STATUS SUSPENDED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(true), None),
      IfExistsThrowError,
      List(),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gqlMissingAuth(p), initialState, "No auth given for user.", p).errors
  }

  testVersions("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(false), None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  testVersions("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE REQUIRED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  testVersions("CREATE USER foo IF NOT EXISTS SET STATUS ACTIVE") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(false), None),
      IfExistsDoNothing,
      List(),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gqlMissingAuth(p), initialState, "No auth given for user.", p).errors
  }

  testVersions("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(true), None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = false)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  testVersions("CREATE OR REPLACE USER foo SET PASSWORD CHANGE NOT REQUIRED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = false)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  testVersions("CREATE OR REPLACE USER foo SET STATUS SUSPENDED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(true), None),
      IfExistsReplace,
      List(),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gqlMissingAuth(p), initialState, "No auth given for user.", p).errors
  }

  testVersions("CREATE OR REPLACE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(Some(false), None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos1)))(pos1))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  testVersions("CREATE USER foo SET PASSWORD $password CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(
          password(paramPassword)(pos1),
          passwordChange(requireChange = false)(pos2),
          passwordChange(requireChange = true)(pos3)
        )
      )(pos))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD CHANGE [NOT] REQUIRED", pos3),
        initialState,
        "Duplicate `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos3
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(requireChange = true)(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos1
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'foo' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("foo")(pos2)))(pos1)),
      None
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos1
    ).errors
    val gql2 =
      GqlHelper.getGql42001_22N04(
        "SET ID",
        "auth provider 'native' attribute",
        java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
        pos2.offset,
        pos2.line,
        pos2.column
      )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `native` does not allow `SET ID` clause.",
        pos2
      ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error1 ++ error2
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(password(password)(pos2)))(pos1)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "foo", None),
        initialState,
        "Clause `SET ID` is mandatory for auth provider `foo`.",
        pos1
      ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error1 ++ error2
  }

  testVersions(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(
        Auth("native", List(password(password)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N19_duplicateClause("SET AUTH 'native'", pos3),
      initialState,
      "Duplicate `SET AUTH 'native'` clause.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error1 ++ error2
  }

  testVersions(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(
        Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N19_duplicateClause("SET AUTH 'native'", pos3),
      initialState,
      "Duplicate `SET AUTH 'native'` clause.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos1
    ).errors
    val error3 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      error1 ++ error2 ++ error3
  }

  testVersions(
    "CREATE USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD 'password' }"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos3)))(pos2)),
      Some(Auth("native", List(password(password)(pos1)))(pos1))
    )(p)

    val error = SemanticCheckResult.error(
      gqlCannotCombineOldAndNewSyntax(pos1),
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos1
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error
  }

  testVersions(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD CHANGE REQUIRED"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3))
    )(p)

    val error1 = SemanticCheckResult.error(
      gqlCannotCombineOldAndNewSyntax(pos3),
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error1 ++ error2
  }

  testVersions(
    "CREATE USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos2)),
      Some(Auth("native", List(password(password)(pos1)))(pos1))
    )(p)

    val error1 = SemanticCheckResult.error(
      gqlCannotCombineOldAndNewSyntax(pos1),
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos1
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos2
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error1 ++ error2
  }

  testVersions(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } SET PASSWORD CHANGE REQUIRED"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3))
    )(p)

    val error1 = SemanticCheckResult.error(
      gqlCannotCombineOldAndNewSyntax(pos3),
      initialState,
      "Cannot combine old and new auth syntax for the same auth provider.",
      pos3
    ).errors
    val error2 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos1
    ).errors
    val error3 = SemanticCheckResult.error(
      getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
      initialState,
      "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
      pos3
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      error1 ++ error2 ++ error3
  }

  testVersions("CREATE USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'foo' { SET ID 'bar' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("bar")(pos4)))(pos3)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'", pos3),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'", pos3),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD 'password'}") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos2), password(password)(pos3)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos3),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD $password}") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)(pos2), password(paramPassword)(pos3)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos3),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID $qux }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1), authId(parameter("qux", CTString))(pos2)))(pos)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET ID", pos2),
        initialState,
        "Duplicate `SET ID` clause.",
        pos2
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None
    )(p)
    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD` clause.", pos2).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD 'password' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None
    )(p)
    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      pos1.offset,
      pos1.line,
      pos1.column
    )
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD CHANGE REQUIRED }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1), passwordChange(requireChange = true)(pos2)))(pos)),
      None
    )(p)
    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gql,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos2
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET PASSWORD CHANGE REQUIRED") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")(pos1)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos2)))(pos2))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos2
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE REQUIRED }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("bar")(pos1), passwordChange(requireChange = true)(pos2)))(pos3)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET PASSWORD", "native", None),
        initialState,
        "Clause `SET PASSWORD` is mandatory for auth provider `native`.",
        pos3
      ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      pos1.offset,
      pos1.line,
      pos1.column
    )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `native` does not allow `SET ID` clause.",
        pos1
      ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error1 ++ error2
  }

  testVersions(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE REQUIRED SET PASSWORD 'password' }"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth(
        "native",
        List(authId("bar")(pos1), passwordChange(requireChange = true)(pos2), password(password)(pos4))
      )(pos3)),
      None
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      pos1.offset,
      pos1.line,
      pos1.column
    )
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  testVersions(
    "CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED }"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth(
        "foo",
        List(authId("bar")(pos1), password(password)(pos2), passwordChange(requireChange = true)(pos4))
      )(pos3)),
      None
    )(p)
    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD` clause.", pos2).errors
  }

  testVersions("CREATE USER foo SET AUTH '' { SET PASSWORD '' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(password(passwordEmpty)(pos2)))(pos1)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "", None),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider '' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error3 = SemanticCheckResult.error(
      gqlAuthProviderNotAllowedEmpty(pos1),
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      error1 ++ error2 ++ error3
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER '' { SET ID '' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(authId("")(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlAuthProviderNotAllowedEmpty(pos1),
        initialState,
        "Invalid input. Auth provider is not allowed to be an empty string.",
        pos1
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH '' { SET PASSWORD 'password' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(password(password)(pos2)))(pos1)),
      None
    )(p)

    val error1 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "", None),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider '' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    val error2 =
      SemanticCheckResult.error(
        gql2,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error3 = SemanticCheckResult.error(
      gqlAuthProviderNotAllowedEmpty(pos1),
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      error1 ++ error2 ++ error3
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER '' { SET ID 'bar' }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(authId("bar")(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlAuthProviderNotAllowedEmpty(pos1),
        initialState,
        "Invalid input. Auth provider is not allowed to be an empty string.",
        pos1
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 42 }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId(literalInt(42, pos3))(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlWrongType("42", "id", Seq("STRING NOT NULL"), pos3),
        initialState,
        "id must be a String, or a String parameter.",
        pos3
      ).errors
  }

  testVersions("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID $numberParam }") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId(parameter("numberParam", CTInteger, position = pos3))(pos2)))(pos1)),
      None
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlWrongType("$numberParam", "id", Seq("STRING NOT NULL"), pos3),
        initialState,
        "id must be a String, or a String parameter.",
        pos3
      ).errors
  }

  testVersions("RENAME USER foo TO true") { version =>
    val renameUser = RenameUser(
      literalString("foo"),
      literalBoolean(booleanValue = true, pos2),
      ifExists = false
    )(p)

    renameUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlWrongType("true", "to username", Seq("STRING NOT NULL"), pos2),
        initialState,
        "to username must be a String, or a String parameter.",
        pos2
      ).errors
  }

  testVersions("ALTER USER foo") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gqlIncompleteAuthCommand(p), initialState, "`ALTER USER` requires at least one clause.", p).errors
  }

  testVersions("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD $password") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)(pos1), password(paramPassword, isEncrypted = true)(pos2)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos2),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  testVersions("ALTER USER foo SET PASSWORD $password SET PASSWORD 'password'") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(paramPassword)(pos1), password(password)(pos2)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos2),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  testVersions("ALTER USER foo SET PASSWORD 'password' SET PASSWORD 'password'") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)(pos1), password(password)(pos2)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos2),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos2
      ).errors
  }

  testVersions("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth(
        "native",
        List(passwordChange(requireChange = false)(pos1), passwordChange(requireChange = true)(pos2))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD CHANGE [NOT] REQUIRED", pos2),
        initialState,
        "Duplicate `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos2
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'foo' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("foo")(pos1)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql =
      GqlHelper.getGql42001_22N04(
        "SET ID",
        "auth provider 'native' attribute",
        java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
        pos1.offset,
        pos1.line,
        pos1.column
      )
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(password(password)(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql1 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    val error1 =
      SemanticCheckResult.error(
        gql1,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error2 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "foo", None),
        initialState,
        "Clause `SET ID` is mandatory for auth provider `foo`.",
        pos1
      ).errors
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe error1 ++ error2
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(
        Auth("native", List(password(password)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'native'", pos3),
        initialState,
        "Duplicate `SET AUTH 'native'` clause.",
        pos3
      ).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } " +
      "SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(
        Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1),
        Auth("native", List(passwordChange(requireChange = true)(pos4)))(pos3)
      ),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'native'", pos3),
        initialState,
        "Duplicate `SET AUTH 'native'` clause.",
        pos3
      ).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD 'password'"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2)))(pos1)),
      Some(Auth("native", List(password(password)(pos3)))(pos3)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlCannotCombineOldAndNewSyntax(pos3),
        initialState,
        "Cannot combine old and new auth syntax for the same auth provider.",
        pos3
      ).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD CHANGE REQUIRED"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlCannotCombineOldAndNewSyntax(pos3),
        initialState,
        "Cannot combine old and new auth syntax for the same auth provider.",
        pos3
      ).errors
  }

  testVersions(
    "ALTER USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos2)),
      Some(Auth("native", List(password(password)(pos1)))(pos1)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlCannotCombineOldAndNewSyntax(pos1),
        initialState,
        "Cannot combine old and new auth syntax for the same auth provider.",
        pos1
      ).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE NOT REQUIRED } SET PASSWORD CHANGE REQUIRED"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(passwordChange(requireChange = false)(pos2)))(pos1)),
      Some(Auth("native", List(passwordChange(requireChange = true)(pos3)))(pos3)),
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlCannotCombineOldAndNewSyntax(pos3),
        initialState,
        "Cannot combine old and new auth syntax for the same auth provider.",
        pos3
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'foo' { SET ID 'bar' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("bar")(pos4)))(pos3)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'", pos3),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'", pos3),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD 'password'}") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2), password(password)(pos3)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos3),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH 'native' {SET PASSWORD 'password' SET PASSWORD $password}") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)(pos2), password(paramPassword)(pos3)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET PASSWORD", pos3),
        initialState,
        "Duplicate `SET PASSWORD` clause.",
        pos3
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID 'qux' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), authId("qux")(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET ID", pos2),
        initialState,
        "Duplicate `SET ID` clause.",
        pos2
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID $qux }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), authId(parameter("qux", CTString))(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET ID", pos2),
        initialState,
        "Duplicate `SET ID` clause.",
        pos2
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD` clause.", pos2).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD 'password' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("bar")(pos1), password(password)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      pos1.offset,
      pos1.line,
      pos1.column
    )
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gql,
        initialState,
        "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
        pos2
      ).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      pos1.offset,
      pos1.line,
      pos1.column
    )
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD 'password' }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth(
        "native",
        List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2), password(password)(pos3))
      )(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET ID",
      "auth provider 'native' attribute",
      java.util.List.of("SET PASSWORD", "SET PASSWORD CHANGE [NOT] REQUIRED"),
      pos1.offset,
      pos1.line,
      pos1.column
    )
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `native` does not allow `SET ID` clause.", pos1).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD 'password' }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth(
        "foo",
        List(authId("bar")(pos1), passwordChange(requireChange = false)(pos2), password(password)(pos3))
      )(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(gql, initialState, "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.", pos2)
      .errors
  }

  testVersions("ALTER USER foo SET AUTH '' { SET PASSWORD '' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(password(passwordEmpty)(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql1 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider '' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    val error1 =
      SemanticCheckResult.error(
        gql1,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error2 = SemanticCheckResult.error(
      gqlAuthProviderNotAllowedEmpty(pos1),
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    val error3 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "", None),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      error1 ++ error2 ++ error3
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER '' { SET ID '' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(authId("")(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlAuthProviderNotAllowedEmpty(pos1),
        initialState,
        "Invalid input. Auth provider is not allowed to be an empty string.",
        pos1
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH '' { SET PASSWORD 'password' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(password(password)(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val gql1 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD",
      "auth provider '' attribute",
      java.util.List.of("SET ID"),
      pos2.offset,
      pos2.line,
      pos2.column
    )
    val error1 =
      SemanticCheckResult.error(
        gql1,
        initialState,
        "Auth provider `` does not allow `SET PASSWORD` clause.",
        pos2
      ).errors
    val error2 = SemanticCheckResult.error(
      gqlAuthProviderNotAllowedEmpty(pos1),
      initialState,
      "Invalid input. Auth provider is not allowed to be an empty string.",
      pos1
    ).errors
    val error3 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "", None),
        initialState,
        "Clause `SET ID` is mandatory for auth provider ``.",
        pos1
      ).errors
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      error1 ++ error2 ++ error3
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER '' { SET ID 'bar' }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(authId("bar")(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlAuthProviderNotAllowedEmpty(pos1),
        initialState,
        "Invalid input. Auth provider is not allowed to be an empty string.",
        pos1
      ).errors
  }

  testVersions("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 42 }") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId(literalInt(42, pos3))(pos2)))(pos1)),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlWrongType("42", "id", Seq("STRING NOT NULL"), pos3),
        initialState,
        "id must be a String, or a String parameter.",
        pos3
      ).errors
  }

  testVersions("ALTER USER foo REMOVE AUTH PROVIDER 42") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(literalInt(42, pos1)))
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlStringOrStringListWrongType("42", "REMOVE AUTH", pos1),
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersions("ALTER USER foo REMOVE AUTH PROVIDER [42, 69]") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1, literalInt(42, pos2), literalInt(69, pos3))))
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlStringOrStringListWrongType("[42, 69]", "REMOVE AUTH", pos1),
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersions("ALTER USER foo REMOVE AUTH PROVIDER ['bar', 69]") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1, literalString("bar"), literalInt(69, pos3))))
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlStringOrStringListWrongType("""["bar", 69]""", "REMOVE AUTH", pos1),
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersions("ALTER USER foo REMOVE AUTH PROVIDER [69, 'bar']") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1, literalInt(69, pos3), literalString("bar"))))
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlStringOrStringListWrongType("""[69, "bar"]""", "REMOVE AUTH", pos1),
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersions("ALTER USER foo REMOVE AUTH PROVIDER []") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfWithPosition(pos1)))
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlStringOrStringListWrongType("[]", "REMOVE AUTH", pos1),
        initialState,
        "Expected a non-empty String, non-empty List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersions(
    "ALTER USER foo SET AUTH 'foo' {SET PASSWORD CHANGE NOT REQUIRED} SET AUTH 'foo' {SET ID 'bar'}"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(
        Auth("foo", List(PasswordChange(requireChange = false)(pos1)))(pos2),
        Auth("foo", List(AuthId(literalString("bar"))(pos4)))(pos3)
      ),
      None,
      RemoveAuth(all = false, List.empty)
    )(p)

    val error1 = SemanticCheckResult.error(
      getGql42N19_duplicateClause("SET AUTH 'foo'", pos3),
      initialState,
      "Duplicate `SET AUTH 'foo'` clause.",
      pos3
    ).errors
    val gql2 = GqlHelper.getGql42001_22N04(
      "SET PASSWORD CHANGE [NOT] REQUIRED",
      "auth provider 'foo' attribute",
      java.util.List.of("SET ID"),
      pos1.offset,
      pos1.line,
      pos1.column
    )
    val error2 = SemanticCheckResult.error(
      gql2,
      initialState,
      "Auth provider `foo` does not allow `SET PASSWORD CHANGE [NOT] REQUIRED` clause.",
      pos1
    ).errors
    val error3 =
      SemanticCheckResult.error(
        getGql42N97_missingMandatoryAuthClause("SET ID", "foo", None),
        initialState,
        "Clause `SET ID` is mandatory for auth provider `foo`.",
        pos2
      ).errors
    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      error1 ++ error2 ++ error3
  }

  testVersions(
    "ALTER USER foo REMOVE ALL AUTH SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None,
      RemoveAuth(all = true, List.empty)
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'", pos3),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  testVersions(
    "ALTER USER foo REMOVE AUTH 'foo' SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")(pos2)))(pos1), Auth("foo", List(authId("qux")(pos4)))(pos3)),
      None,
      RemoveAuth(all = false, List(literalString("foo")))
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        getGql42N19_duplicateClause("SET AUTH 'foo'", pos3),
        initialState,
        "Duplicate `SET AUTH 'foo'` clause.",
        pos3
      ).errors
  }

  // Role command test

  testVersions("DROP ROLE 3.5 IF EXISTS") { version =>
    val dropRole = DropRole(
      literalFloat(3.5, pos.withInputLength(3)),
      ifExists = true
    )(p)

    dropRole.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(
        gqlWrongType("3.5", "rolename", Seq("STRING NOT NULL"), pos),
        initialState,
        "rolename must be a String, or a String parameter.",
        pos.withInputLength(3)
      ).errors
  }

  // Auth rule command tests

  private def authRuleFeatureToggleError(verb: String) = FeatureError.notAvailableInThisImplementation(
    SemanticFeature.AttributeBasedAccessControl,
    s"The `$verb AUTH RULE` clause",
    p
  )

  testVersionsExcept5("Create auth rule without feature flag") { version =>
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsThrowError,
      List(
        AuthRuleCondition(equals(literalInt(1), literalInt(1)))(p)
      )
    )(p)

    authRule.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(initialState, authRuleFeatureToggleError("CREATE")).errors
  }

  testVersionsExcept5("Rename auth rule without feature flag") { version =>
    val authRule = RenameAuthRule(
      literalString("authRule"),
      literalString("authRule2"),
      ifExists = false
    )(p)

    authRule.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(initialState, authRuleFeatureToggleError("RENAME")).errors
  }

  testVersionsExcept5("Alter auth rule without feature flag") { version =>
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleCondition(equals(literalInt(1), literalInt(1)))(p)
      )
    )(p)

    authRule.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(initialState, authRuleFeatureToggleError("ALTER")).errors
  }

  testVersionsExcept5("Drop auth rule without feature flag") { version =>
    val authRule = DropAuthRule(
      literalString("authRule"),
      ifExists = false
    )(p)

    authRule.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe SemanticCheckResult
      .error(initialState, authRuleFeatureToggleError("DROP")).errors
  }

  testVersionsExcept5("CREATE AUTH RULE authRule") { version =>
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsThrowError,
      List.empty
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors should equal(SemanticCheckResult
      .error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(p.offset, p.line, p.column)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
            .atPosition(p.offset, p.line, p.column)
            .withParam(GqlParams.ListParam.inputList, List("SET CONDITION").asJava)
            .build())
          .build(),
        initialStateWithFeatureFlags,
        """42001
          |22N06: Invalid input. 'SET CONDITION' needs to be specified.""".stripMargin,
        p
      ).errors)
  }

  testVersionsExcept5("CREATE OR REPLACE AUTH RULE authRule") { version =>
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsReplace,
      List.empty
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors should equal(SemanticCheckResult
      .error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(p.offset, p.line, p.column)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
            .atPosition(p.offset, p.line, p.column)
            .withParam(GqlParams.ListParam.inputList, List("SET CONDITION").asJava)
            .build())
          .build(),
        initialStateWithFeatureFlags,
        """42001
          |22N06: Invalid input. 'SET CONDITION' needs to be specified.""".stripMargin,
        p
      ).errors)
  }

  testVersionsExcept5("CREATE OR REPLACE AUTH RULE IF EXISTS authRule SET CONDITION 1=1 SET ENABLED true") { version =>
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsInvalidSyntax,
      List(
        AuthRuleCondition(equals(literalInt(1), literalInt(1)))(p),
        AuthRuleEnabled(enabled = true)(pos1),
        AuthRuleEnabled(enabled = false)(pos2)
      )
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors shouldBe SemanticCheckResult
      .error(
        GqlHelper.getGql42001_42N14("OR REPLACE", "IF NOT EXISTS", p.offset, p.line, p.column),
        initialStateWithFeatureFlags,
        "Failed to create the specified auth rule 'authRule': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
        p
      ).errors

  }

  testVersionsExcept5("CREATE AUTH RULE authRule SET CONDITION toLoWer('HELLO') = 'hello'") { version =>
    val functionInvocation = FunctionInvocation(
      name = FunctionName("toLoWer")(p),
      argument = literalString("HELLO")
    )(p)
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsThrowError,
      List(
        AuthRuleCondition(Equals(functionInvocation, literalString("hello"))(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("CREATE OR REPLACE AUTH RULE authRule SET CONDITION toLoWer('HELLO') = 'hello'") { version =>
    val functionInvocation = FunctionInvocation(
      name = FunctionName("toLoWer")(p),
      argument = literalString("HELLO")
    )(p)
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsReplace,
      List(
        AuthRuleCondition(Equals(functionInvocation, literalString("hello"))(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("CREATE AUTH RULE authRule SET CONDITION $param") { version =>
    val param = parameter("param", CTAny)
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsThrowError,
      List(
        AuthRuleCondition(param)(p)
      )
    )(p)

    // This is not supported yet
    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors shouldBe SemanticCheckResult
      .error(
        initialStateWithFeatureFlags,
        SemanticError.authRuleConditionCannotContainParameter(param)
      ).errors
  }

  testVersionsExcept5(
    "ALTER AUTH RULE authRule SET CONDITION my.test.date('x') = 'v' — UDF shadowing an allow-listed name"
  ) { version =>
    // Uses ALTER (rather than CREATE like its siblings) so the AuthRule check chain is exercised
    // through both entry points.
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleCondition(Equals(
          FunctionInvocation(
            name = FunctionName(Namespace(List("my", "test"))(pos1), "date")(pos1),
            argument = literalString("x")
          )(pos1),
          literalString("v")
        )(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors should equal(SemanticCheckResult
      .error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(pos1.offset, pos1.line, pos1.column)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
              .atPosition(pos1.offset, pos1.line, pos1.column)
              .withParam(GqlParams.StringParam.input, "my.test.date")
              .withParam(GqlParams.StringParam.context, "function in auth rule condition")
              .build()
          ).build(),
        initialStateWithFeatureFlags,
        """42001
          |22N05: Invalid input 'my.test.date' for function in auth rule condition.""".stripMargin,
        pos1
      ).errors)
  }

  testVersionsExcept5("CREATE AUTH RULE authRule SET CONDITION unknown.function('HELLO') = 'SE'") { version =>
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsThrowError,
      List(
        AuthRuleCondition(Equals(
          FunctionInvocation(
            name = FunctionName("unknown.function")(pos1),
            argument = literalString("HELLO")
          )(pos1),
          literalString("SE")
        )(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors should equal(SemanticCheckResult
      .error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(pos1.offset, pos1.line, pos1.column)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
              .atPosition(pos1.offset, pos1.line, pos1.column)
              .withParam(GqlParams.StringParam.input, "unknown.function")
              .withParam(GqlParams.StringParam.context, "function in auth rule condition")
              .build()
          ).build(),
        initialStateWithFeatureFlags,
        """42001
          |22N05: Invalid input 'unknown.function' for function in auth rule condition.""".stripMargin,
        pos1
      ).errors)
  }

  testVersionsExcept5("CREATE AUTH RULE authRule SET CONDITION graph.byName('HELLO') = 'SE'") { version =>
    val authRule = CreateAuthRule(
      literalString("authRule"),
      IfExistsThrowError,
      List(
        AuthRuleCondition(Equals(
          FunctionInvocation(
            name = FunctionName("graph.byName")(pos1),
            argument = literalString("HELLO")
          )(pos1),
          literalString("SE")
        )(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors should equal(SemanticCheckResult
      .error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(pos1.offset, pos1.line, pos1.column)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
              .atPosition(pos1.offset, pos1.line, pos1.column)
              .withParam(GqlParams.StringParam.input, "graph.byName")
              .withParam(GqlParams.StringParam.context, "function in auth rule condition")
              .build()
          ).build(),
        initialStateWithFeatureFlags,
        """42001
          |22N05: Invalid input 'graph.byName' for function in auth rule condition.""".stripMargin,
        pos1
      ).errors)
  }

  testVersionsExcept5("RENAME AUTH RULE authRule TO authRule2") { version =>
    val authRule = RenameAuthRule(
      literalString("authRule"),
      literalString("authRule2"),
      ifExists = false
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("RENAME AUTH RULE authRule IF EXISTS TO authRule2") { version =>
    val authRule = RenameAuthRule(
      literalString("authRule"),
      literalString("authRule2"),
      ifExists = true
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("RENAME AUTH RULE $param1 TO $param2") { version =>
    val authRule = RenameAuthRule(
      parameter("param1", CTString),
      parameter("param2", CTString),
      ifExists = false
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("RENAME AUTH RULE $param1 IF EXISTS TO $param2") { version =>
    val authRule = RenameAuthRule(
      parameter("param1", CTString),
      parameter("param2", CTString),
      ifExists = true
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE authRule SET CONDITION 1=1") { version =>
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleCondition(equals(literalInt(1), literalInt(1)))(pos1)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE $param SET CONDITION 1=1") { version =>
    val authRule = AlterAuthRule(
      parameter("param", CTString),
      ifExists = false,
      List(
        AuthRuleCondition(equals(literalInt(1), literalInt(1)))(pos1)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE authRule SET ENABLED true") { version =>
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleEnabled(enabled = true)(pos1)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE authRule IF EXISTS SET CONDITION 1=1") { version =>
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = true,
      List(
        AuthRuleCondition(equals(literalInt(1), literalInt(1)))(pos1)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE authRule IF EXISTS SET ENABLED true") { version =>
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = true,
      List(
        AuthRuleEnabled(enabled = true)(pos1)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE $param IF EXISTS SET ENABLED true") { version =>
    val authRule = AlterAuthRule(
      parameter("param", CTString),
      ifExists = true,
      List(
        AuthRuleEnabled(enabled = true)(pos1)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE authRule SET CONDITION toLoWer('HELLO') = 'hello'") { version =>
    val functionInvocation = FunctionInvocation(
      name = FunctionName("toLoWer")(p),
      argument = literalString("HELLO")
    )(p)
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleCondition(Equals(functionInvocation, literalString("hello"))(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER AUTH RULE authRule SET CONDITION $param") { version =>
    val param = parameter("param", CTAny)
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleCondition(param)(p)
      )
    )(p)

    // This is not supported yet
    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors shouldBe SemanticCheckResult
      .error(
        initialStateWithFeatureFlags,
        SemanticError.authRuleConditionCannotContainParameter(param)
      ).errors
  }

  testVersionsExcept5("ALTER AUTH RULE authRule SET CONDITION unknown.function('HELLO') = 'SE'") { version =>
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleCondition(Equals(
          FunctionInvocation(
            name = FunctionName("unknown.function")(pos1),
            argument = literalString("HELLO")
          )(pos1),
          literalString("SE")
        )(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors should equal(SemanticCheckResult
      .error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(pos1.offset, pos1.line, pos1.column)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
              .atPosition(pos1.offset, pos1.line, pos1.column)
              .withParam(GqlParams.StringParam.input, "unknown.function")
              .withParam(GqlParams.StringParam.context, "function in auth rule condition")
              .build()
          ).build(),
        initialStateWithFeatureFlags,
        """42001
          |22N05: Invalid input 'unknown.function' for function in auth rule condition.""".stripMargin,
        pos1
      ).errors)
  }

  testVersionsExcept5("ALTER AUTH RULE authRule SET CONDITION graph.byName('HELLO') = 'SE'") { version =>
    val authRule = AlterAuthRule(
      literalString("authRule"),
      ifExists = false,
      List(
        AuthRuleCondition(Equals(
          FunctionInvocation(
            name = FunctionName("graph.byName")(pos1),
            argument = literalString("HELLO")
          )(pos1),
          literalString("SE")
        )(p))(p)
      )
    )(p)

    authRule.semanticCheck.run(
      initialStateWithFeatureFlags,
      versionedSemanticContext(version)
    ).errors should equal(SemanticCheckResult
      .error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(pos1.offset, pos1.line, pos1.column)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
              .atPosition(pos1.offset, pos1.line, pos1.column)
              .withParam(GqlParams.StringParam.input, "graph.byName")
              .withParam(GqlParams.StringParam.context, "function in auth rule condition")
              .build()
          ).build(),
        initialStateWithFeatureFlags,
        """42001
          |22N05: Invalid input 'graph.byName' for function in auth rule condition.""".stripMargin,
        pos1
      ).errors)
  }

  testVersionsExcept5("DROP AUTH RULE authRule") { version =>
    val authRule = DropAuthRule(
      literalString("authRule"),
      ifExists = false
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("DROP AUTH RULE authRule IF EXISTS") { version =>
    val authRule = DropAuthRule(
      literalString("authRule"),
      ifExists = true
    )(p)

    authRule.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  // User tags

  testVersionsExcept5(
    "CREATE USER foo SET PASSWORD 'password' SET TAGS 'label' — fails without UserTags feature"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)(pos1)))(pos1)),
      Some(SetTags(literalString("label"))(pos2))
    )(p)

    createUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors should equal(Seq(
      FeatureError.notAvailableInThisImplementation(SemanticFeature.UserTags, "The SET TAGS clause", pos2)
    ))
  }

  testVersionsExcept5(
    "CREATE USER foo SET PASSWORD 'password' SET TAGS 'label' — passes with UserTags feature"
  ) { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)(pos1)))(pos1)),
      Some(SetTags(literalString("label"))(pos2))
    )(p)

    createUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS 'x' — fails without UserTags feature") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(literalString("x"))(pos1))
    )(p)

    alterUser.semanticCheck.run(initialState, versionedSemanticContext(version)).errors should equal(Seq(
      FeatureError.notAvailableInThisImplementation(SemanticFeature.UserTags, "The ADD TAGS clause", pos1)
    ))
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS 'x' — passes with UserTags feature") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(literalString("x"))(pos1))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USER foo REMOVE TAGS 'x' SET TAGS ['a'] — SET cannot combine with REMOVE") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(RemoveTags(literalString("x"))(pos1), SetTags(listOf(literalString("a")))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors should equal(
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
          .atPosition(pos2.offset, pos2.line, pos2.column).build(),
        initialStateWithFeatureFlags,
        "SET TAGS cannot be combined with ADD TAGS or REMOVE TAGS.",
        pos2
      ).errors
    )
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS 'x' SET TAGS ['a'] — SET cannot combine with ADD") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(literalString("x"))(pos1), SetTags(listOf(literalString("a")))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors should equal(
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
          .atPosition(pos2.offset, pos2.line, pos2.column).build(),
        initialStateWithFeatureFlags,
        "SET TAGS cannot be combined with ADD TAGS or REMOVE TAGS.",
        pos2
      ).errors
    )
  }

  testVersionsExcept5("ALTER USERS alice ADD TAGS 'x' — fails without UserTags feature") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(AddTags(literalString("x"))(pos1))
    )(p)

    alterUsers.semanticCheck.run(initialState, versionedSemanticContext(version)).errors should equal(Seq(
      FeatureError.notAvailableInThisImplementation(SemanticFeature.UserTags, "The ALTER USERS command", p)
    ))
  }

  testVersionsExcept5("ALTER USERS alice ADD TAGS 'x' — passes with UserTags feature") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(AddTags(literalString("x"))(pos1))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USERS alice ADD TAGS [] — empty list passes") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(AddTags(listOfWithPosition(pos1))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USERS alice SET TAGS [] — empty list passes") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(SetTags(listOfWithPosition(pos1))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USERS alice REMOVE TAGS [] — empty list passes") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(RemoveTags(listOfWithPosition(pos1))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USERS alice (no tag clause) — requires at least one tag clause") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq.empty
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors should equal(
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N94)
          .atPosition(p.offset, p.line, p.column).build(),
        initialStateWithFeatureFlags,
        "`ALTER USERS` requires at least one tag clause.",
        p
      ).errors
    )
  }

  testVersionsExcept5("ALTER USERS alice REMOVE TAGS 'x' SET TAGS ['a'] — SET cannot combine with REMOVE") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(RemoveTags(literalString("x"))(pos1), SetTags(listOf(literalString("a")))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors should equal(
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
          .atPosition(pos2.offset, pos2.line, pos2.column).build(),
        initialStateWithFeatureFlags,
        "SET TAGS cannot be combined with ADD TAGS or REMOVE TAGS.",
        pos2
      ).errors
    )
  }

  testVersionsExcept5("ALTER USERS alice ADD TAGS 'x' SET TAGS ['a'] — SET cannot combine with ADD") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(AddTags(literalString("x"))(pos1), SetTags(listOf(literalString("a")))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors should equal(
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
          .atPosition(pos2.offset, pos2.line, pos2.column).build(),
        initialStateWithFeatureFlags,
        "SET TAGS cannot be combined with ADD TAGS or REMOVE TAGS.",
        pos2
      ).errors
    )
  }

  testVersionsExcept5("ALTER USER foo REMOVE ALL TAGS SET TAGS ['a'] — SET cannot combine with REMOVE ALL") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(RemoveAllTags()(pos1), SetTags(listOf(literalString("a")))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors should equal(
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
          .atPosition(pos2.offset, pos2.line, pos2.column).build(),
        initialStateWithFeatureFlags,
        "SET TAGS cannot be combined with ADD TAGS or REMOVE TAGS.",
        pos2
      ).errors
    )
  }

  testVersionsExcept5(
    "ALTER USERS alice REMOVE ALL TAGS SET TAGS ['a'] — SET cannot combine with REMOVE ALL"
  ) { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(RemoveAllTags()(pos1), SetTags(listOf(literalString("a")))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors should equal(
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
          .atPosition(pos2.offset, pos2.line, pos2.column).build(),
        initialStateWithFeatureFlags,
        "SET TAGS cannot be combined with ADD TAGS or REMOVE TAGS.",
        pos2
      ).errors
    )
  }

  testVersionsExcept5(
    "ALTER USER foo REMOVE ALL TAGS ADD TAGS 'x' — passes (remove-all then add is allowed)"
  ) { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(RemoveAllTags()(pos1), AddTags(literalString("x"))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5(
    "ALTER USERS alice REMOVE ALL TAGS ADD TAGS 'x' — passes (remove-all then add is allowed)"
  ) { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(RemoveAllTags()(pos1), AddTags(literalString("x"))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  // Empty / wrong-type tag values

  testVersionsExcept5("ALTER USER foo ADD TAGS '' — empty string rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(StringLiteral("")(pos1))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("\"\"", "ADD TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo SET TAGS '' — empty string rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(SetTags(StringLiteral("")(pos1))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("\"\"", "SET TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo REMOVE TAGS '' — empty string rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(RemoveTags(StringLiteral("")(pos1))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("\"\"", "REMOVE TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS ['x', ''] — list with empty element rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(listOfWithPosition(pos1, literalString("x"), literalString("")))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("""["x", ""]""", "ADD TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS [] — empty list passes") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(listOfWithPosition(pos1))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USER foo SET TAGS [] — empty list passes") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(SetTags(listOfWithPosition(pos1))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USER foo REMOVE TAGS [] — empty list passes") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(RemoveTags(listOfWithPosition(pos1))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USER foo SET TAGS [''] — list of only empty string still rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(SetTags(listOfWithPosition(pos1, literalString("")))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("""[""]""", "SET TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS 42 — wrong type rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(literalInt(42, pos1))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("42", "ADD TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS [1, 2, 3] — non-string list rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(listOfWithPosition(pos1, literalInt(1, pos2), literalInt(2, pos3), literalInt(3, pos4)))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("[1, 2, 3]", "ADD TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS [0, 'hello', 'world'] — heterogeneous list rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(listOfWithPosition(pos1, literalInt(0, pos2), literalString("hello"), literalString("world")))(pos3))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("""[0, "hello", "world"]""", "ADD TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo SET TAGS [1, 2, 3] — non-string list rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(SetTags(listOfWithPosition(pos1, literalInt(1, pos2), literalInt(2, pos3), literalInt(3, pos4)))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("[1, 2, 3]", "SET TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo SET TAGS [0, 'hello', 'world'] — heterogeneous list rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(SetTags(listOfWithPosition(pos1, literalInt(0, pos2), literalString("hello"), literalString("world")))(pos3))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("""[0, "hello", "world"]""", "SET TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo REMOVE TAGS [1, 2, 3] — non-string list rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(RemoveTags(listOfWithPosition(pos1, literalInt(1, pos2), literalInt(2, pos3), literalInt(3, pos4)))(pos2))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("[1, 2, 3]", "REMOVE TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo REMOVE TAGS [0, 'hello', 'world'] — heterogeneous list rejected") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(RemoveTags(listOfWithPosition(
        pos1,
        literalInt(0, pos2),
        literalString("hello"),
        literalString("world")
      ))(pos3))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("""[0, "hello", "world"]""", "REMOVE TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS $param — parameter accepted") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(parameter("tags", CTString))(pos1))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USERS alice ADD TAGS '' — empty string rejected") { version =>
    val alterUsers = AlterUsers(
      Seq(literalString("alice")),
      ifExists = false,
      Seq(AddTags(StringLiteral("")(pos1))(pos2))
    )(p)

    alterUsers.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("\"\"", "ADD TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("CREATE USER foo SET TAGS '' — empty string rejected") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)(p)))(p)),
      Some(SetTags(StringLiteral("")(pos1))(pos2))
    )(p)

    createUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        gqlStringOrStringListWrongType("\"\"", "SET TAGS", pos1, allowEmptyList = true),
        initialStateWithFeatureFlags,
        "Expected a non-empty String, List of non-empty Strings, or Parameter.",
        pos1
      ).errors
  }

  testVersionsExcept5("CREATE USER foo SET TAGS [] — empty list passes") { version =>
    val createUser = CreateUser(
      literalString("foo"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)(p)))(p)),
      Some(SetTags(listOfWithPosition(pos1))(pos2))
    )(p)

    createUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }

  testVersionsExcept5("ALTER USER foo ADD TAGS ['a', 'b'] — non-empty list of non-empty strings passes") { version =>
    val alterUser = AlterUser(
      literalString("foo"),
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty),
      Seq(AddTags(listOf(literalString("a"), literalString("b")))(pos1))
    )(p)

    alterUser.semanticCheck.run(initialStateWithFeatureFlags, versionedSemanticContext(version)).errors shouldBe empty
  }
}
