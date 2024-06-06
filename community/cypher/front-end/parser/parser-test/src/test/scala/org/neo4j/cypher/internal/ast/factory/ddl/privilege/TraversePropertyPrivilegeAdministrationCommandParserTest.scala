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

import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TraverseAction
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

class TraversePropertyPrivilegeAdministrationCommandParserTest
    extends PropertyPrivilegeAdministrationCommandParserTestBase {
  case class Action(verb: String, preposition: String, func: noResourcePrivilegeFunc)

  val actions: Seq[Action] = Seq(
    Action("GRANT", "TO", grantGraphPrivilege),
    Action("DENY", "TO", denyGraphPrivilege),
    Action("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege),
    Action("REVOKE DENY", "FROM", revokeDenyGraphPrivilege),
    Action("REVOKE", "FROM", revokeGraphPrivilege)
  )

  test("HOME GRAPH") {
    for {
      Action(verb, preposition, func) <- actions
      immutable <- Seq(true, false)
    } yield {
      val immutableString = immutableOrEmpty(immutable)

      s"$verb$immutableString TRAVERSE ON HOME GRAPH FOR (a:A) WHERE a.prop2=1 $preposition role" should
        parseTo[Statements](func(
          GraphPrivilege(TraverseAction, HomeGraphScope()(pos))(pos),
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
        )(pos))
    }
  }

  test("DEFAULT GRAPH") {
    for {
      Action(verb, preposition, func) <- actions
      immutable <- Seq(true, false)
    } yield {
      val immutableString = immutableOrEmpty(immutable)

      s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH FOR (a:A) WHERE a.prop2=1 $preposition role" should
        parseTo[Statements](
          func(
            GraphPrivilege(TraverseAction, DefaultGraphScope()(pos))(pos),
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
          )(pos)
        )
    }
  }

  test("valid labels") {
    for {
      Action(verb, preposition, func) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      LiteralExpression(expression, propertyRuleAst) <- literalExpressions
      Scope(graphName, graphScope) <- scopes
    } {
      val immutableString = immutableOrEmpty(immutable)
      val expressionString = expressionStringifier(expression)

      // No labels
      (expression match {
        case _: MapExpression => List(
            (None, s"($expressionString)"),
            (Some(Variable("n")(pos)), s"(n $expressionString)")
          )
        case _: BooleanExpression => List(
            (Some(Variable("n")(pos)), s"(n) WHERE $expressionString"),
            (Some(Variable("n")(pos)), s"(n WHERE $expressionString)"),
            (Some(Variable("WHERE")(pos)), s"(WHERE WHERE $expressionString)"), // WHERE as variable
            (
              None,
              s"() WHERE $expressionString"
            ) // Missing variable is valid when parsing. Fail in semantic check
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String) =>
        // All labels, parameterised role
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition $$role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(Seq(LabelAllQualifier()(pos)), variable, propertyRuleAst)),
              Seq(paramRole),
              immutable
            )(pos)
          )

        // All labels, role containing colon
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition `r:ole`" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(Seq(LabelAllQualifier()(pos)), variable, propertyRuleAst)),
              Seq(literalRColonOle),
              immutable
            )(pos)
          )
      }

      // Single label name
      (expression match {
        case _: MapExpression => List(
            (None, s"(:A $expressionString)"),
            (Some(Variable("n")(pos)), s"(n:A $expressionString)")
          )
        case _: BooleanExpression => List(
            (Some(Variable("n")(pos)), s"(n:A) WHERE $expressionString"),
            (Some(Variable("n")(pos)), s"(n:A WHERE $expressionString)"),
            (Some(Variable("WHERE")(pos)), s"(WHERE:A WHERE $expressionString)"), // WHERE as variable
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
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(Seq(labelQualifierA), variable, propertyRuleAst)),
              Seq(literalRole),
              immutable
            )(pos)
          )
      }

      // Escaped multi-token label name
      (expression match {
        case _: MapExpression => List(
            (None, s"(:`A B` $expressionString)"),
            (Some(Variable("n")(pos)), s"(n:`A B` $expressionString)")
          )
        case _: BooleanExpression => List(
            (Some(Variable("n")(pos)), s"(n:`A B`) WHERE $expressionString"),
            (Some(Variable("n")(pos)), s"(n:`A B` WHERE $expressionString)"),
            (
              None,
              s"(:`A B`) WHERE $expressionString"
            ), // Missing variable is valid when parsing. Fail in semantic check
            (
              None,
              s"(:`A B` WHERE $expressionString)"
            ) // Missing variable is valid when parsing. Fail in semantic check
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(Seq(LabelQualifier("A B")(_)), variable, propertyRuleAst)),
              Seq(literalRole),
              immutable
            )(pos)
          )
      }

      // Label containing colon
      (expression match {
        case _: MapExpression => List(
            (None, s"(:`:A` $expressionString)"),
            (Some(Variable("n")(pos)), s"(n:`:A` $expressionString)")
          )
        case _: BooleanExpression => List(
            (Some(Variable("n")(pos)), s"(n:`:A`) WHERE $expressionString"),
            (Some(Variable("n")(pos)), s"(n:`:A` WHERE $expressionString)"),
            (
              None,
              s"(:`:A`) WHERE $expressionString"
            ), // Missing variable is valid when parsing. Fail in semantic check
            (
              None,
              s"(:`:A` WHERE $expressionString)"
            ) // Missing variable is valid when parsing. Fail in semantic check
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(Seq(LabelQualifier(":A")(_)), variable, propertyRuleAst)),
              Seq(literalRole),
              immutable
            )(pos)
          )
      }

      // Multiple labels
      (expression match {
        case _: MapExpression => List(
            (None, s"(:A|B $expressionString)"),
            (Some(Variable("n")(pos)), s"(n:A|B $expressionString)")
          )
        case _: BooleanExpression => List(
            (Some(Variable("n")(pos)), s"(n:A|B) WHERE $expressionString"),
            (Some(Variable("n")(pos)), s"(n:A|B WHERE $expressionString)"),
            (
              None,
              s"(:A|B) WHERE $expressionString"
            ), // Missing variable is valid when parsing. Fail in semantic check
            (
              None,
              s"(:A|B WHERE $expressionString)"
            ) // Missing variable is valid when parsing. Fail in semantic check
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role1, $$role2" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(
                PatternQualifier(Seq(labelQualifierA, labelQualifierB), variable, propertyRuleAst)
              ),
              Seq(literalRole1, paramRole2),
              immutable
            )(pos)
          )
      }
    }
  }

  test("additional assortment of supported graph scopes") {
    for {
      Action(verb, preposition, func) <- actions
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
        s"$verb$immutableString TRAVERSE ON $graphKeyword `f:oo` $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
              patternQualifier,
              Seq(literalRole),
              immutable
            )(pos)
          )

        s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScopeFooBaz)(pos),
              patternQualifier,
              Seq(literalRole),
              immutable
            )(pos)
          )
      }
    }
  }

  test("Allow trailing star") {
    s"GRANT TRAVERSE ON GRAPH * FOR (n) WHERE n.prop1 = 1 (*) TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
        List(PatternQualifier(
          Seq(LabelAllQualifier() _),
          Some(Variable("n")(_)),
          equals(prop(varFor("n"), "prop1"), literalInt(1))
        )),
        Seq(literalRole),
        i = false
      )(pos)
    )
  }

  test(
    "Different variable should parse correctly to allow them to be rejected in the semantic check with a user-friendly explanation"
  ) {
    s"GRANT TRAVERSE ON GRAPH * FOR (a) WHERE b.prop1 = 1 TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
        List(PatternQualifier(
          Seq(LabelAllQualifier() _),
          Some(Variable("a")(_)),
          equals(prop(varFor("b"), "prop1"), literalInt(1))
        )),
        Seq(literalRole),
        i = false
      )(pos)
    )
  }

  test(
    "'FOR (n) WHERE 1 = n.prop1 (foo) TO role' parse as a function to then be rejected in semantic check"
  ) {
    s"GRANT TRAVERSE ON GRAPH * FOR (n) WHERE 1 = n.prop1 (foo) TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
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
      )(pos)
    )

    s"GRANT TRAVERSE ON GRAPH * FOR (n WHERE 1 = n.prop1 (foo)) TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
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
      )(pos)
    )
  }

  test(
    "'(n:A WHERE EXISTS { MATCH (n) })' parse to then be rejected in semantic check"
  ) {
    s"GRANT TRAVERSE ON GRAPH * FOR (n:A WHERE EXISTS { MATCH (n) }) TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
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
      )(pos)
    )
  }

  test("legitimate property rules, but with problems elsewhere in the privilege command") {
    for {
      Action(verb, preposition, _) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      LiteralExpression(expression, _) <- literalExpressions
      Scope(graphName, _) <- scopes
    } yield {
      val immutableString = immutableOrEmpty(immutable)
      val expressionString = expressionStringifier(expression)

      (expression match {
        case _: MapExpression => List(
            s"($expressionString)",
            s"(:A $expressionString)",
            s"(n:A $expressionString)"
          )
        case _: BooleanExpression => List(
            s"(n) WHERE $expressionString",
            s"(n WHERE $expressionString)",
            s"(n:A) WHERE $expressionString",
            s"(n:A WHERE $expressionString)",
            s"(:A) WHERE $expressionString", // Missing variable is valid when parsing. Fail in semantic check
            s"() WHERE $expressionString", // Missing variable is valid when parsing. Fail in semantic check
            s"(:A WHERE $expressionString)" // Missing variable is valid when parsing. Fail in semantic check
          )
        case _ => fail("Unexpected expression")
      }).foreach { (propertyRule: String) =>
        // Missing ON
        s"$verb$immutableString TRAVERSE $graphKeyword $graphName $patternKeyword $propertyRule $preposition role" should
          notParse[Statements]

        // Missing role
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule" should
          notParse[Statements]

        // r:ole is invalid
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition r:ole" should
          notParse[Statements]

        // Invalid graph name
        s"$verb$immutableString TRAVERSE ON $graphKeyword f:oo $patternKeyword $propertyRule $preposition role" should
          notParse[Statements]

        // Mixing specific graph and *
        s"$verb$immutableString TRAVERSE ON $graphKeyword foo, * $patternKeyword $propertyRule $preposition role" should
          notParse[Statements]
        s"$verb$immutableString TRAVERSE ON $graphKeyword *, foo $patternKeyword $propertyRule $preposition role" should
          notParse[Statements]

        // Missing graph name
        s"$verb$immutableString TRAVERSE ON $graphKeyword $patternKeyword $propertyRule $preposition role" should
          notParse[Statements]
        s"$verb$immutableString TRAVERSE ON $graphKeyword $patternKeyword $propertyRule (*) $preposition role" should
          notParse[Statements]
      }
    }
  }

  test("invalid segments") {
    for {
      Action(verb, preposition, _) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      segment <- invalidSegments
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
          s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $segment $propertyRule $preposition role" should
            notParse[Statements]
        }
      }
    }
  }

  test("disallowed property rules") {
    for {
      Action(verb, preposition, _) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      Scope(graphName, _) <- scopes
    } yield {
      val immutableString = immutableOrEmpty(immutable)
      disallowedPropertyRules.foreach { (disallowedPropertyRule: String) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $disallowedPropertyRule $preposition role" should
          notParse[Statements]
      }

      // No variable, WHERE gets parsed as variable in javacc
      assertFailsOnlyJavaCC[Statements](
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword (WHERE n.prop1 = 1) $preposition role"
      )
    }
  }
}
