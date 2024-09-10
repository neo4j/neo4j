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
package org.neo4j.cypher.internal.ast.factory.ddl.privilege

import org.neo4j.cypher.internal.ast.ActionResourceBase
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.MatchAction
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink
import org.scalactic.anyvals.PosInt

class ReadMatchPropertyPrivilegeAdministrationCommandParserTest
    extends PropertyPrivilegeAdministrationCommandParserTestBase
    with CypherScalaCheckDrivenPropertyChecks {
  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  case class Action(action: GraphAction, verb: String, preposition: String, func: resourcePrivilegeFunc)

  case class Resource(properties: String, resource: ActionResourceBase)

  val actions: Seq[Action] = Seq(
    Action(ReadAction, "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    Action(ReadAction, "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    Action(ReadAction, "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    Action(ReadAction, "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    Action(ReadAction, "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc),
    Action(MatchAction, "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    Action(MatchAction, "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    Action(MatchAction, "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    Action(MatchAction, "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    Action(MatchAction, "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  )

  val resources: Seq[Resource] = Seq(
    Resource("*", AllPropertyResource()(pos)),
    Resource("bar", PropertiesResource(Seq("bar"))(pos)),
    Resource("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos))
  )

  test("HOME GRAPH") {
    for {
      Action(action, verb, preposition, func) <- actions
      immutable <- Seq(true, false)
    } yield {
      val immutableString = immutableOrEmpty(immutable)

      s"$verb$immutableString ${action.name} { prop } ON HOME GRAPH FOR (a:A) WHERE a.prop2=1 $preposition role" should
        parseIn[Statements](_ =>
          _.toAst(statementToStatements(func(
            GraphPrivilege(action, HomeGraphScope()(pos))(pos),
            PropertiesResource(propSeq)(pos),
            List(PatternQualifier(
              Seq(labelQualifierA),
              Some(Variable("a")(_)),
              Equals(
                Property(Variable("a")(_), PropertyKeyName("prop2")(_))(_),
                literal(1)
              )(_)
            )),
            Seq(literalRole),
            immutable
          )(defaultPos)))
        )
    }
  }

  test("DEFAULT GRAPH") {
    for {
      Action(action, verb, preposition, func) <- actions
      immutable <- Seq(true, false)
    } yield {
      val immutableString = immutableOrEmpty(immutable)

      s"$verb$immutableString ${action.name} { prop } ON DEFAULT GRAPH FOR (a:A) WHERE a.prop2=1 $preposition role" should
        parseIn[Statements](_ =>
          _.toAst(statementToStatements(func(
            GraphPrivilege(action, DefaultGraphScope()(pos))(pos),
            PropertiesResource(propSeq)(pos),
            List(PatternQualifier(
              Seq(labelQualifierA),
              Some(Variable("a")(_)),
              Equals(
                Property(Variable("a")(_), PropertyKeyName("prop2")(_))(_),
                literal(1)
              )(_)
            )),
            Seq(literalRole),
            immutable
          )(defaultPos)))
        )
    }
  }

  case class ExpressionAndQualifier(
    literal: LiteralExpression,
    varible: Option[Variable],
    propertyRule: String,
    expectedQualifiers: Seq[PrivilegeQualifier]
  )

  private def literalExpressionAndQualifiers(): Iterator[ExpressionAndQualifier] = {
    for {
      literal <- literalExpressions.iterator
      expressionString = expressionStringifier(literal.expression)
      (variable, propertyRule, expectedQualifiers) <- literal.expression match {
        case _: MapExpression => Iterator(
            (None, s"($expressionString)", Seq(LabelAllQualifier()(pos))),
            (Some(v"n"), s"(n $expressionString)", Seq(LabelAllQualifier()(pos))),
            (None, s"(:A $expressionString)", Seq(labelQualifierA(pos))),
            (Some(v"n"), s"(n:A $expressionString)", Seq(labelQualifierA(pos))),
            (None, s"(:`A B` $expressionString)", Seq(LabelQualifier("A B")(pos))),
            (Some(v"n"), s"(n:`A B` $expressionString)", Seq(LabelQualifier("A B")(pos))),
            (None, s"(:`:A` $expressionString)", Seq(LabelQualifier(":A")(pos))),
            (Some(v"n"), s"(n:`:A` $expressionString)", Seq(LabelQualifier(":A")(pos))),
            (None, s"(:A|B $expressionString)", Seq(labelQualifierA(pos), labelQualifierB(pos))),
            (Some(v"n"), s"(n:A|B $expressionString)", Seq(labelQualifierA(pos), labelQualifierB(pos)))
          )
        case _: BooleanExpression => Iterator(
            (Some(v"n"), s"(n) WHERE $expressionString", Seq(LabelAllQualifier()(pos))),
            (Some(v"n"), s"(n WHERE $expressionString)", Seq(LabelAllQualifier()(pos))),
            // WHERE as variable
            (Some(v"WHERE"), s"(WHERE WHERE $expressionString)", Seq(LabelAllQualifier()(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"() WHERE $expressionString", Seq(LabelAllQualifier()(pos))),
            (Some(v"n"), s"(n:A) WHERE $expressionString", Seq(labelQualifierA(pos))),
            (Some(v"n"), s"(n:A WHERE $expressionString)", Seq(labelQualifierA(pos))),
            // WHERE as variable
            (Some(v"WHERE"), s"(WHERE:A WHERE $expressionString)", Seq(labelQualifierA(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:A) WHERE $expressionString", Seq(labelQualifierA(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:A WHERE $expressionString)", Seq(labelQualifierA(pos))),
            (Some(v"n"), s"(n:`A B`) WHERE $expressionString", Seq(LabelQualifier("A B")(pos))),
            (Some(v"n"), s"(n:`A B` WHERE $expressionString)", Seq(LabelQualifier("A B")(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:`A B`) WHERE $expressionString", Seq(LabelQualifier("A B")(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:`A B` WHERE $expressionString)", Seq(LabelQualifier("A B")(pos))),
            (Some(v"n"), s"(n:`:A`) WHERE $expressionString", Seq(LabelQualifier(":A")(pos))),
            (Some(v"n"), s"(n:`:A` WHERE $expressionString)", Seq(LabelQualifier(":A")(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:`:A`) WHERE $expressionString", Seq(LabelQualifier(":A")(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:`:A` WHERE $expressionString)", Seq(LabelQualifier(":A")(pos))),
            (Some(v"n"), s"(n:A|B) WHERE $expressionString", Seq(labelQualifierA(pos), labelQualifierB(pos))),
            (Some(v"n"), s"(n:A|B WHERE $expressionString)", Seq(labelQualifierA(pos), labelQualifierB(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:A|B) WHERE $expressionString", Seq(labelQualifierA(pos), labelQualifierB(pos))),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:A|B WHERE $expressionString)", Seq(labelQualifierA(pos), labelQualifierB(pos)))
          )
        case _ => fail("Unexpected expression")
      }
    } yield ExpressionAndQualifier(literal, variable, propertyRule, expectedQualifiers)
  }

  test("valid labels") {
    def genTestCase = for {
      action <- Gen.oneOf(actions)
      immutable <- Arbitrary.arbitrary[Boolean]
      graphKeyword <- Gen.oneOf(graphKeywords)
      resource <- Gen.oneOf(resources)
      scope <- Gen.oneOf(scopes)
      (roleString, expectedRoles) <- Gen.oneOf(
        ("role", Seq(literalRole)),
        ("$role", Seq(paramRole)),
        ("`r:ole`", Seq(literalRColonOle)),
        ("role1, $role2", Seq(literalRole1, paramRole2))
      )
    } yield (action, immutable, graphKeyword, resource, scope, roleString, expectedRoles)

    val sizeHint = 100_000
    // Run at least sizeHint test cases
    val minSuccess = math.max(sizeHint / literalExpressionAndQualifiers().size, 10)

    literalExpressionAndQualifiers().foreach { qualifiers =>
      forAll(genTestCase, minSuccessful(PosInt.from(minSuccess).get)) {
        case (action, immutable, graphKeyword, resource, scope, roleString, expectedRoles) =>
          val immutableString = immutableOrEmpty(immutable)
          s"""${action.verb}$immutableString ${action.action.name} {${resource.properties}}
             |ON $graphKeyword ${scope.graphName} $patternKeyword ${qualifiers.propertyRule}
             |${action.preposition} $roleString""".stripMargin should parseTo[Statements](
            action.func(
              GraphPrivilege(action.action, scope.graphScope)(pos),
              resource.resource,
              List(PatternQualifier(qualifiers.expectedQualifiers, qualifiers.varible, qualifiers.literal.expectedAst)),
              expectedRoles,
              immutable
            )(pos)
          )
      }
    }
  }

  test("additional assortment of supported graph scopes and property resources") {
    for {
      Action(action, verb, preposition, func) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      LiteralExpression(expression, propertyRuleAst) <- literalExpressions
    } yield {
      val immutableString = immutableOrEmpty(immutable)
      val expressionString = expressionStringifier(expression)

      (expression match {
        case _: MapExpression => List(
            (None, s"(:A $expressionString)"),
            (Some(Variable("n")(pos)), s"(n:A $expressionString)")
          )
        case _: BooleanExpression => List(
            (Some(Variable("n")(pos)), s"(n:A) WHERE $expressionString"),
            (Some(Variable("n")(pos)), s"(n:A WHERE $expressionString)"),
            (
              None,
              s"(:A) WHERE $expressionString"
            ), // Missing variable is valid when parsing. Fail in semantic check
            (
              None,
              s"(:A WHERE $expressionString)"
            ) // Missing variable is valid when parsing. Fail in semantic check
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String) =>
        val patternQualifier = List(PatternQualifier(Seq(labelQualifierA), variable, propertyRuleAst))
        s"$verb$immutableString ${action.name} {*} ON $graphKeyword `f:oo` $patternKeyword $propertyRule $preposition role" should
          parseIn[Statements](_ =>
            _.toAst(statementToStatements(func(
              GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
              AllPropertyResource() _,
              patternQualifier,
              Seq(literalRole),
              immutable
            )(defaultPos)))
          )
        s"$verb$immutableString ${action.name} {bar} ON $graphKeyword `f:oo` $patternKeyword $propertyRule $preposition role" should
          parseIn[Statements](_ =>
            _.toAst(statementToStatements(func(
              GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
              PropertiesResource(Seq("bar")) _,
              patternQualifier,
              Seq(literalRole),
              immutable
            )(defaultPos)))
          )
        s"$verb$immutableString ${action.name} {`b:ar`} ON $graphKeyword foo $patternKeyword $propertyRule $preposition role" should
          parseIn[Statements](_ =>
            _.toAst(statementToStatements(func(
              GraphPrivilege(action, graphScopeFoo)(pos),
              PropertiesResource(Seq("b:ar")) _,
              patternQualifier,
              Seq(literalRole),
              immutable
            )(defaultPos)))
          )
        s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, baz $patternKeyword $propertyRule $preposition role" should
          parseIn[Statements](_ =>
            _.toAst(statementToStatements(func(
              GraphPrivilege(action, graphScopeFooBaz)(pos),
              AllPropertyResource() _,
              patternQualifier,
              Seq(literalRole),
              immutable
            )(defaultPos)))
          )
        s"$verb$immutableString ${action.name} {bar} ON $graphKeyword foo, baz $patternKeyword $propertyRule $preposition role" should
          parseIn[Statements](_ =>
            _.toAst(statementToStatements(func(
              GraphPrivilege(action, graphScopeFooBaz)(pos),
              PropertiesResource(Seq("bar")) _,
              patternQualifier,
              Seq(literalRole),
              immutable
            )(defaultPos)))
          )
      }
    }
  }

  test("Allow trailing star") {
    s"GRANT READ {*} ON GRAPH * FOR (n) WHERE n.prop1 = 1 (*) TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(ReadAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelAllQualifier() _),
            Some(Variable("n")(_)),
            equals(prop(varFor("n"), "prop1"), literalInt(1))
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )

    s"GRANT MATCH {*} ON GRAPH * FOR (n) WHERE n.prop1 = 1 (*) TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(MatchAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelAllQualifier() _),
            Some(Variable("n")(_)),
            equals(prop(varFor("n"), "prop1"), literalInt(1))
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )
  }

  test(
    "Different variable should parse correctly to allow them to be rejected in the semantic check with a user-friendly explanation"
  ) {
    s"GRANT READ {*} ON GRAPH * FOR (a) WHERE b.prop1 = 1 TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(ReadAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelAllQualifier() _),
            Some(Variable("a")(_)),
            equals(prop(varFor("b"), "prop1"), literalInt(1))
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )

    s"GRANT MATCH {*} ON GRAPH * FOR (a) WHERE b.prop1 = 1 TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(MatchAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelAllQualifier() _),
            Some(Variable("a")(_)),
            equals(prop(varFor("b"), "prop1"), literalInt(1))
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )
  }

  test(
    "'FOR (n) WHERE 1 = n.prop1 (foo) TO role' parse as a function to then be rejected in semantic check"
  ) {
    s"GRANT READ {*} ON GRAPH * FOR (n) WHERE 1 = n.prop1 (foo) TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(ReadAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelAllQualifier() _),
            Some(Variable("n")(_)),
            equals(
              literalInt(1),
              FunctionInvocation.apply(
                FunctionName(Namespace(List("n"))(pos), "prop1")(pos),
                Variable("foo")(pos)
              )(pos)
            )
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )

    s"GRANT MATCH {*} ON GRAPH * FOR (n WHERE 1 = n.prop1 (foo)) TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(MatchAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelAllQualifier() _),
            Some(Variable("n")(_)),
            equals(
              literalInt(1),
              FunctionInvocation.apply(
                FunctionName(Namespace(List("n"))(pos), "prop1")(pos),
                Variable("foo")(pos)
              )(pos)
            )
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )
  }

  test(
    "'(n:A WHERE EXISTS { MATCH (n) })' parse to then be rejected in semantic check"
  ) {
    s"GRANT READ {*} ON GRAPH * FOR (n:A WHERE EXISTS { MATCH (n) }) TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(ReadAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelQualifier("A") _),
            Some(Variable("n")(_)),
            ExistsExpression(
              SingleQuery(
                List(
                  Match(
                    optional = false,
                    MatchMode.DifferentRelationships(implicitlyCreated = true)(pos),
                    ForMatch(List(PatternPartWithSelector(
                      AllPaths()(pos),
                      PathPatternPart(NodePattern(Some(Variable("n")(pos)), None, None, None)(pos))
                    )))(pos),
                    List(),
                    None
                  )(pos)
                )
              )(pos)
            )(pos, None, None)
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )

    s"GRANT MATCH {*} ON GRAPH * FOR (n:A) WHERE EXISTS { MATCH (n) } TO role" should
      parseIn[Statements](_ =>
        _.toAst(statementToStatements(grantGraphPrivilege(
          GraphPrivilege(MatchAction, AllGraphsScope()(pos))(pos),
          AllPropertyResource() _,
          List(PatternQualifier(
            Seq(LabelQualifier("A") _),
            Some(Variable("n")(_)),
            ExistsExpression(
              SingleQuery(
                List(
                  Match(
                    optional = false,
                    MatchMode.DifferentRelationships(implicitlyCreated = true)(pos),
                    ForMatch(List(PatternPartWithSelector(
                      AllPaths()(pos),
                      PathPatternPart(NodePattern(Some(Variable("n")(pos)), None, None, None)(pos))
                    )))(pos),
                    List(),
                    None
                  )(pos)
                )
              )(pos)
            )(pos, None, None)
          )),
          Seq(literalRole),
          i = false
        )(defaultPos)))
      )
  }

  test("legitimate property rules, but with problems elsewhere in the privilege command") {
    val genTestCases = for {
      Action(action, verb, preposition, _) <- Gen.oneOf(actions)
      immutable <- Arbitrary.arbitrary[Boolean]
      graphKeyword <- Gen.oneOf(graphKeywords)
      LiteralExpression(expression, _) <- Gen.oneOf(literalExpressions)
      expressionString = expressionStringifier(expression)
      Resource(properties, _) <- Gen.oneOf(resources)
      Scope(graphName, _) <- Gen.oneOf(scopes)
      propertyRule <- expression match {
        case _: MapExpression => Gen.oneOf(
            s"($expressionString)",
            s"(:A $expressionString)",
            s"(n:A $expressionString)"
          )
        case _: BooleanExpression => Gen.oneOf(
            s"(n) WHERE $expressionString",
            s"(n WHERE $expressionString)",
            s"(n:A) WHERE $expressionString",
            s"(n:A WHERE $expressionString)",
            s"(:A) WHERE $expressionString", // Missing variable is valid when parsing. Fail in semantic check
            s"() WHERE $expressionString", // Missing variable is valid when parsing. Fail in semantic check
            s"(:A WHERE $expressionString)" // Missing variable is valid when parsing. Fail in semantic check
          )
        case _ => fail("Unexpected expression")
      }
    } yield (action, verb, preposition, immutable, graphKeyword, properties, graphName, propertyRule)
    forAll(genTestCases, minSuccessful(1000)) {
      case (action, verb, preposition, immutable, graphKeyword, properties, graphName, propertyRule) =>
        val immutableString = immutableOrEmpty(immutable)

        // Missing ON
        s"""$verb$immutableString ${action.name} {$properties}
           |$graphKeyword $graphName $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Missing role
        s"""$verb$immutableString ${action.name} {$properties}
           |ON $graphKeyword $graphName $patternKeyword $propertyRule
           |""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // r:ole is invalid
        s"""$verb$immutableString ${action.name} {$properties}
           |ON $graphKeyword $graphName $patternKeyword $propertyRule
           |$preposition r:ole""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Invalid graph name
        s"""$verb$immutableString ${action.name} {$properties}
           |ON $graphKeyword f:oo $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Mixing specific graph and *
        s"""$verb$immutableString ${action.name} {$properties}
           |ON $graphKeyword foo, * $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString ${action.name} {$properties}
           |ON $graphKeyword *, foo $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Invalid property definition
        s"""$verb$immutableString ${action.name} {b:ar}
           |ON $graphKeyword foo $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Missing graph name
        s"""$verb$immutableString ${action.name} {$properties}
           |ON $graphKeyword $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString ${action.name} {$properties}
           |ON $graphKeyword $patternKeyword $propertyRule (*)
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Missing property definition
        s"""$verb$immutableString ${action.name}
           |ON $graphKeyword * $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString ${action.name}
           |ON $graphKeyword * $patternKeyword $propertyRule (*)
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString ${action.name}
           |ON $graphKeyword foo $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString ${action.name}
           |ON $graphKeyword foo $patternKeyword $propertyRule (*)
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Missing property list
        s"""$verb$immutableString ${action.name} {}
           |ON $graphKeyword * $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString ${action.name} {}
           |ON $graphKeyword * $patternKeyword $propertyRule (*)
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString ${action.name} {}
           |ON $graphKeyword foo $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"$verb$immutableString ${action.name} {} " +
          s"ON $graphKeyword foo $patternKeyword $propertyRule (*) " +
          s"$preposition role" should notParse[Statements].withMessageContaining("Invalid input")
    }
  }

  test("invalid segments") {
    for {
      Action(action, verb, preposition, _) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      segment <- invalidSegments
      Resource(properties, _) <- resources
      Scope(graphName, _) <- scopes
    } yield {
      val immutableString = immutableOrEmpty(immutable)

      Seq(
        s"(n:A) WHERE n.prop1 = 1",
        s"(n:A WHERE n.prop1 = 1)",
        s"(:A {prop1:1})",
        s"(n:A {prop1:1})"
      ).foreach { (propertyRule: String) =>
        {
          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $segment $propertyRule $preposition role" should
            notParse[Statements]
        }
      }
    }
  }

  test("disallowed property rules") {
    for {
      Action(action, verb, preposition, _) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      Resource(properties, _) <- resources
      Scope(graphName, _) <- scopes
    } yield {
      val immutableString = immutableOrEmpty(immutable)

      disallowedPropertyRules.foreach { (disallowedPropertyRule: String) =>
        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $patternKeyword $disallowedPropertyRule $preposition role" should
          notParse[Statements]
      }

      // No variable: fails in JavaCC as WHERE gets parsed as variable
      s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $patternKeyword (WHERE n.prop1 = 1) $preposition role" should
        parseIn[Statements] {
          case Cypher5JavaCc => _.withMessageStart("Invalid input 'n': expected \":\" or \"{\" (line 1, ")
          case _             => _.withoutErrors
        }
    }
  }
}
