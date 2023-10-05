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

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._

class AdministrationCommandTest extends CypherFunSuite with AstConstructionTestSupport {

  // Privilege command tests
  private val p = InputPosition(0, 0, 0)
  private val initialState = SemanticState.clean

  test("it should not be possible to administer privileges pertaining to an unassignable action") {

    val privilegeManagementActions =
      Table("PrivilegeManagementActions", AssignImmutablePrivilegeAction, RemoveImmutablePrivilegeAction)

    val grant = (pma: PrivilegeManagementAction) =>
      new GrantPrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(Left("role1"))
      )(p)

    val deny = (pma: PrivilegeManagementAction) =>
      new DenyPrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(Left("role1"))
      )(p)

    val revoke = (pma: PrivilegeManagementAction, rt: RevokeType) =>
      new RevokePrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(Left("role1")),
        rt
      )(p)

    val revokeBoth = revoke(_, RevokeBothType()(p))
    val revokeGrant = revoke(_, RevokeGrantType()(p))
    val revokeDeny = revoke(_, RevokeDenyType()(p))
    val privilegeCommands = Table("PrivilegeCommand", grant, deny, revokeBoth, revokeGrant, revokeDeny)

    forAll(privilegeManagementActions) { pma =>
      forAll(privilegeCommands) { privilegeCommand =>
        val privilege = privilegeCommand(pma)
        privilege.semanticCheck.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult
          .error(initialState, s"`GRANT`, `DENY` and `REVOKE` are not supported for `${pma.name}`", p)
      }
    }
  }

  test("it should not be possible to administer privileges on the default graph") {
    val privilege = new GrantPrivilege(
      GraphPrivilege(AllGraphAction, DefaultGraphScope()(p))(p),
      false,
      Some(DatabaseResource()(p)),
      List(AllQualifier()(p)),
      Seq(Left("role1"))
    )(p)

    privilege.semanticCheck.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult
      .error(initialState, "`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.", p)
  }

  test("it should not be possible to administer privileges on the default database") {
    val privilege = new GrantPrivilege(
      DatabasePrivilege(AllConstraintActions, DefaultDatabaseScope()(p))(p),
      false,
      Some(DatabaseResource()(p)),
      List(AllQualifier()(p)),
      Seq(Left("role1"))
    )(p)

    privilege.semanticCheck.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult
      .error(initialState, "`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.", p)
  }

  type QualifierFn = (Option[Variable], Expression) => List[PrivilegeQualifier]
  val allLabelPatternQualifier: QualifierFn = (v, e) => List(PatternQualifier(Seq(LabelAllQualifier()(p)), v, e))

  val singleLabelPatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(LabelQualifier("A")(p)), v, e))

  val multiLabelPatternQualifier: QualifierFn =
    (v, e) => List(PatternQualifier(Seq(LabelQualifier("A")(p), LabelQualifier("B")(p)), v, e))

  val mixedList = listOf(literalInt(1), literalString("s"), literalFloat(1.1), falseLiteral, parameter("value1", CTAny))

  Seq(
    (allLabelPatternQualifier, "all labels"),
    (singleLabelPatternQualifier, "single label"),
    (multiLabelPatternQualifier, "multiple labels")
  ).foreach {
    case (qualifierFn: QualifierFn, qualifierDescription) =>
      // e.g. FOR (n{prop1:val1, prop2:val2})
      test(s"property rules with more than one property should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
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
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Property rules can only contain one property."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 = 1 AND n.prop2 = 1
      test(
        s"property rules using WHERE syntax with multiple predicates via AND should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            And(
              Equals(
                Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p),
                SignedDecimalIntegerLiteral("1")(p)
              )(p),
              Equals(
                Property(Variable("n")(p), PropertyKeyName("prop2")(p))(p),
                SignedDecimalIntegerLiteral("1")(p)
              )(p)
            )(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: n.prop1 = 1 AND n.prop2 = 1 is not supported."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 = 1 OR n.prop2 = 1
      test(
        s"property rules using WHERE syntax with multiple predicates via OR should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Or(
              Equals(
                Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p),
                SignedDecimalIntegerLiteral("1")(p)
              )(p),
              Equals(
                Property(Variable("n")(p), PropertyKeyName("prop2")(p))(p),
                SignedDecimalIntegerLiteral("1")(p)
              )(p)
            )(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: n.prop1 = 1 OR n.prop2 = 1 is not supported."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 = NULL
      test(s"property rules using = NULL should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Equals(Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p), Null.NULL)(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "prop1 = NULL always evaluates to NULL. Use IS NULL instead."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE NOT n.prop = NULL
      test(s"property rules using NOT n.prop = NULL should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Not(Equals(Property(Variable("n")(p), PropertyKeyName("prop")(p))(p), Null.NULL)(p))(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "prop = NULL always evaluates to NULL. Use IS NULL instead."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 <> NULL
      test(s"property rules using <> NULL should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            NotEquals(Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p), Null.NULL)(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "prop1 <> NULL always evaluates to NULL. Use IS NOT NULL instead."
        }) shouldBe true
      }

      // e.g. FOR ({n:NULL})
      test(s"property rules NULL in map syntax should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(None, MapExpression(Seq((PropertyKeyName("prop1")(p), Null.NULL)))(p)),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "{prop1:NULL} always evaluates to NULL. Use WHERE syntax in combination with IS NULL instead."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 = NaN
      test(s"property rules using = NaN should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Equals(Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p), NaN()(p))(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "NaN is not supported for property-based access control."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 <> NaN
      test(s"property rules using <> NaN should fail semantic checking ($qualifierDescription)") {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            NotEquals(Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p), NaN()(p))(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "NaN is not supported for property-based access control."
        }) shouldBe true
      }

      // e.g. FOR ({prop1:1+2})
      test(
        s"property rules using map syntax with non-literal predicates should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            MapExpression(Seq((
              PropertyKeyName("prop1")(p),
              Add(SignedDecimalIntegerLiteral("1")(p), SignedDecimalIntegerLiteral("2")(p))(p)
            )))(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: {prop1: 1 + 2} is not supported."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 = 1+2
      test(
        s"property rules using WHERE syntax with non-literal predicates should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Equals(
              Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p),
              Add(SignedDecimalIntegerLiteral("1")(p), SignedDecimalIntegerLiteral("2")(p))(p)
            )(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: n.prop1 = 1 + 2 is not supported."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE n.prop1 = [1, 2]
      test(
        s"property rules using WHERE syntax with List of literals should fail semantic checking ($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()
        Seq(
          // List of ints
          equals(prop(varFor("n"), "prop1"), listOfInt(1, 2)), // n.prop = [1, 2]
          notEquals(prop(varFor("n"), "prop1"), listOfInt(1, 2)), // n.prop <> [1, 2]
          MapExpression(Seq((propName("prop1"), listOfInt(1, 2))))(p), // {prop1: [1, 2]}

          // List of strings
          equals(prop(varFor("n"), "prop1"), listOfString("s1", "s2")), // n.prop = ['s1', 's2']
          notEquals(prop(varFor("n"), "prop1"), listOfString("s1", "s2")), // n.prop <> ['s1', 's2']
          MapExpression(Seq((propName("prop1"), listOfString("s1", "s2"))))(p), // {prop1: ['s1', 's2']}

          // List of booleans
          equals(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral)), // n.prop = [true, false]
          notEquals(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral)), // n.prop <> [true, false]
          MapExpression(Seq((propName("prop1"), listOf(trueLiteral, falseLiteral))))(p), // {prop1: [true, false]}

          // List of floats
          equals(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1), literalFloat(2.2))), // n.prop = [1.1, 2.2]
          notEquals(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1), literalFloat(2.2))), // n.prop <> [1.1, 2.2]
          MapExpression(
            Seq((propName("prop1"), listOf(literalFloat(1.1), literalFloat(1.2))))
          )(p), // {prop1: [1.1, 2.2]}

          // List of parameters
          equals(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny), parameter("value2", CTAny))
          ), // n.prop = [$value, $value2]
          notEquals(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny), parameter("value2", CTAny))
          ), // n.prop <> [$value, $value2]
          MapExpression(
            Seq((propName("prop1"), listOf(parameter("value", CTAny), parameter("value2", CTAny))))
          )(p), // {prop1: [$value, $value2]}

          // Mixed list
          equals(prop(varFor("n"), "prop1"), mixedList), // n.prop = [1, 's', 1.1, false, $value1]
          notEquals(prop(varFor("n"), "prop1"), mixedList), // n.prop <> [1, 's', 1.1, false, $value1]
          MapExpression(Seq((propName("prop1"), mixedList)))(p) // {prop1: [1, 's', 1.1, false, $value1]}

        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(Variable("n")(p)),
                expression
              ),
              Seq(Left("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
                s"Expression: ${expressionStringifier(expression)} is not supported."
            }) shouldBe true
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 = [1]
      test(
        s"property rules using WHERE syntax with single-item list of literals should fail semantic checking ($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()
        Seq(
          // List of ints
          equals(prop(varFor("n"), "prop1"), listOfInt(1)), // n.prop = [1]
          notEquals(prop(varFor("n"), "prop1"), listOfInt(1)), // n.prop <> [1]
          MapExpression(Seq((propName("prop1"), listOfInt(1))))(p), // {prop1: [1]}

          // List of strings
          equals(prop(varFor("n"), "prop1"), listOfString("s1")), // n.prop = ['s1']
          notEquals(prop(varFor("n"), "prop1"), listOfString("s1")), // n.prop <> ['s1']
          MapExpression(Seq((propName("prop1"), listOfString("s1"))))(p), // {prop1: ['s1']}

          // List of booleans
          equals(prop(varFor("n"), "prop1"), listOf(trueLiteral)), // n.prop = [true]
          notEquals(prop(varFor("n"), "prop1"), listOf(trueLiteral)), // n.prop <> [true]
          MapExpression(Seq((propName("prop1"), listOf(trueLiteral))))(p), // {prop1: [true]}

          // List of floats
          equals(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1))), // n.prop = [1.1]
          notEquals(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1))), // n.prop <> [1.1]
          MapExpression(
            Seq((propName("prop1"), listOf(literalFloat(1.1))))
          )(p), // {prop1: [1.1]}

          // List of parameters
          equals(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny))
          ), // n.prop = [$value]
          notEquals(
            prop(varFor("n"), "prop1"),
            listOf(parameter("value", CTAny))
          ), // n.prop <> [$value]
          MapExpression(
            Seq((propName("prop1"), listOf(parameter("value", CTAny))))
          )(p) // {prop1: [$value]}
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(Variable("n")(p)),
                expression
              ),
              Seq(Left("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
                s"Expression: ${expressionStringifier(expression)} is not supported."
            }) shouldBe true
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1]
      test(
        s"property rules using WHERE syntax with property IN List of one literal should pass semantic checking($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        Seq(
          // List of ints
          In(prop(varFor("n"), "prop1"), listOfInt(1))(p), // n.prop IN [1]
          In(prop(varFor("n"), "prop1"), listOfInt(1))(p), // NOT n.prop IN [1]

          // List of strings
          In(prop(varFor("n"), "prop1"), listOfString("s1"))(p), // n.prop IN ['s1']
          Not(In(prop(varFor("n"), "prop1"), listOfString("s1"))(p))(p), // NOT n.prop IN ['s1']

          // List of booleans
          In(prop(varFor("n"), "prop1"), listOf(trueLiteral))(p), // n.prop IN [true]
          Not(In(prop(varFor("n"), "prop1"), listOf(trueLiteral))(p))(p), // NOT n.prop IN [true]

          // List of floats
          In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1)))(p), // n.prop IN [1.1]
          Not(In(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1)))(p))(p), // NOT n.prop IN [1.1]

          // List of parameters
          In(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny)))(p), // n.prop IN [$value]
          Not(In(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny)))(p))(p), // NOT n.prop IN [$value]

          // Parameter list
          In(prop(varFor("n"), "prop1"), parameter("value", CTList(CTAny)))(p), // n.prop IN $paramList
          Not(In(prop(varFor("n"), "prop1"), parameter("value", CTList(CTAny)))(p))(p) // NOT n.prop IN $paramList
        ).foreach { expression =>
          withClue(expressionStringifier(expression)) {
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(Variable("n")(p)),
                expression
              ),
              Seq(Left("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
                s"Expression: ${expressionStringifier(expression)} is not supported."
            }) shouldBe false
          }
        }
      }

      // e.g. FOR (n) WHERE n.prop1 IN [1, 2]
      test(
        s"property rules using WHERE syntax with property IN List of more than one literal should fail semantic checking($qualifierDescription)"
      ) {
        val expressionStringifier = ExpressionStringifier()

        Seq(
          // List of ints
          In(prop(varFor("n"), "prop1"), listOfInt(1, 2))(p), // n.prop IN [1, 2]
          In(prop(varFor("n"), "prop1"), listOfInt(1, 2))(p), // NOT n.prop IN [1, 2]

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
            val privilege = new GrantPrivilege(
              GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
              false,
              None,
              qualifierFn(
                Some(Variable("n")(p)),
                expression
              ),
              Seq(Left("role1"))
            )(p)

            val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
            result.errors.exists(s => {
              s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
                s"Expression: ${expressionStringifier(expression)} is not supported."
            }) shouldBe true
          }
        }
      }

      // e.g. FOR (node) WHERE n.prop1 = 1
      test(
        s"property rules using WHERE syntax using two different variable names should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("node")(p)),
            Equals(
              Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p),
              SignedDecimalIntegerLiteral("1")(p)
            )(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Variable `n` not defined"
        }) shouldBe true
      }

      // e.g. FOR () WHERE n.prop1 = 1
      test(
        s"property rules using WHERE syntax with no variable should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            None,
            Equals(
              Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p),
              SignedDecimalIntegerLiteral("1")(p)
            )(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Variable `n` not defined"
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE 1 = n.prop1 (foo) TO role
      test(
        s"Valid property rule, extra (foo) gets parsed as a function and should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Equals(
              SignedDecimalIntegerLiteral("1")(p),
              FunctionInvocation(
                Namespace(List("n"))(p),
                FunctionName("prop1")(p),
                distinct = false,
                Vector(Variable("foo")(p))
              )(p)
            )(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: 1 = n.prop1(foo) is not supported."
        }) shouldBe true
      }

      // e.g. FOR (n:A WHERE EXISTS { MATCH (n) }) TO role1
      test(
        s"EXIST MATCH pattern in property rule should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(ReadAction, AllGraphsScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(_)),
            ExistsExpression(
              SingleQuery(
                List(
                  Match(
                    optional = false,
                    MatchMode.DifferentRelationships(implicitlyCreated = true)(p),
                    ForMatch(List(PatternPartWithSelector(
                      AllPaths()(p),
                      PathPatternPart(NodePattern(Some(Variable("n")(pos)), None, None, None)(p))
                    )))(p),
                    List(),
                    None
                  )(p)
                )
              )(p)
            )(p, None, None)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: EXISTS { MATCH (n) } is not supported."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE NOT NOT n.prop1 = 1
      test(
        s"using more than one NOT keyword combined with an equal '=' should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Not(Not(Equals(
              Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p),
              SignedDecimalIntegerLiteral("1")(p)
            )(p))(p))(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: NOT (NOT n.prop1 = 1) is not supported."
        }) shouldBe true
      }

      // e.g. FOR (n) WHERE NOT NOT n.prop1 <> 1
      test(
        s"using more than one NOT keyword combined with an not equal '<>' should fail semantic checking ($qualifierDescription)"
      ) {
        val privilege = new GrantPrivilege(
          GraphPrivilege(TraverseAction, HomeGraphScope()(p))(p),
          false,
          None,
          qualifierFn(
            Some(Variable("n")(p)),
            Not(Not(NotEquals(
              Property(Variable("n")(p), PropertyKeyName("prop1")(p))(p),
              SignedDecimalIntegerLiteral("1")(p)
            )(p))(p))(p)
          ),
          Seq(Left("role1"))
        )(p)

        val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
        result.errors.exists(s => {
          s.msg == "Only single literal-based predicate expressions are allowed for property-based access control. " +
            "Expression: NOT (NOT n.prop1 <> 1) is not supported."
        }) shouldBe true
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
        test(s"invalid actions: $invalidAction for property rules ($qualifierDescription)") {
          val privilege = new GrantPrivilege(
            GraphPrivilege(invalidAction, HomeGraphScope()(p))(p),
            false,
            None,
            qualifierFn(
              None,
              MapExpression(Seq(
                (PropertyKeyName("prop1")(p), StringLiteral("val1")(p))
              ))(p)
            ),
            Seq(Left("role1"))
          )(p)

          val result = privilege.semanticCheck.run(initialState, SemanticCheckContext.default)
          result.errors.exists(s => {
            s.msg == s"${invalidAction.name} is not supported for property value access rules."
          }) shouldBe true
        }
      })
  }
}
