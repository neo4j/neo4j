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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupportWithPosConversion
import org.neo4j.cypher.internal.ast.Element
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.Node
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.Relationship
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.exceptions.SyntaxException
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink

class TraversePropertyPrivilegeAdministrationCommandParserTest
    extends PropertyPrivilegeAdministrationCommandParserTestBase
    with CypherScalaCheckDrivenPropertyChecks with AstConstructionTestSupportWithPosConversion {
  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  override protected def ignorePrettifier: Boolean = true

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
      val immutableString = maybeImmutable(immutable)

      s"$verb$immutableString TRAVERSE ON HOME GRAPH FOR (a:A) WHERE a.prop2=1 $preposition role" should
        parseTo[Statements](func(
          GraphPrivilege(TraverseAction, HomeGraphScope()(pos))(pos),
          List(PatternQualifier(
            Seq(labelQualifierA),
            Some(varFor("a")),
            Equals(
              Property(varFor("a"), PropertyKeyName("prop2")(_))(_),
              literal(1)
            )(_),
            Node
          )),
          Seq(literalRole),
          immutable
        )(pos))
    }
  }

  test("valid privileges") {
    for {
      Action(verb, preposition, func) <- actions
      immutable <- Seq(true, false)
      graphKeyword <- graphKeywords
      LiteralExpression(expression, propertyRuleAst) <- literalExpressions
      Scope(graphName, graphScope) <- scopes
    } {
      val immutableString = maybeImmutable(immutable)
      val expressionString = expressionStringifier(expression)

      // No labels
      (expression match {
        case _: MapExpression => List(
            // Nodes
            (None, s"($expressionString)", Node),
            (Some(varFor("n")), s"(n $expressionString)", Node),

            // Relationships
            (None, s"()-[$expressionString]-()", Relationship),
            (Some(varFor("n")), s"()-[n $expressionString]-()", Relationship),
            // Directional relationships is valid when parsing but does not add any extra information
            (None, s"()<-[$expressionString]-()", Relationship),
            (Some(varFor("n")), s"()-[n $expressionString]->()", Relationship),
            (None, s"()<-[$expressionString]->()", Relationship)
          )
        case _: BooleanExpression => List(
            // Nodes
            (Some(varFor("n")), s"(n) WHERE $expressionString", Node),
            (Some(varFor("n")), s"(n WHERE $expressionString)", Node),
            (Some(varFor("WHERE")), s"(WHERE WHERE $expressionString)", Node), // WHERE as variable
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"() WHERE $expressionString", Node),

            // Relationships
            (Some(varFor("n")), s"()-[n]-() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()-[n WHERE $expressionString]-()", Relationship),
            (Some(varFor("WHERE")), s"()-[WHERE WHERE $expressionString]-()", Relationship), // WHERE as variable
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"()-[]-() WHERE $expressionString", Relationship),
            // Directional relationships is valid when parsing but does not add any extra information
            (Some(varFor("n")), s"()<-[n WHERE $expressionString]-()", Relationship),
            (Some(varFor("WHERE")), s"()-[WHERE WHERE $expressionString]->()", Relationship), // WHERE as variable
            (Some(varFor("n")), s"()<-[n WHERE $expressionString]->()", Relationship)
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String, elementType: Element) =>
        // All labels, parameterised role
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition $$role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(
                Seq(
                  if (elementType == Node) LabelAllQualifier()(pos)
                  else RelationshipAllQualifier()(pos)
                ),
                variable,
                propertyRuleAst,
                elementType
              )),
              Seq(paramRole),
              immutable
            )(pos)
          )

        // All labels, role containing colon
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition `r:ole`" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(
                Seq(
                  if (elementType == Node) LabelAllQualifier()(pos)
                  else RelationshipAllQualifier()(pos)
                ),
                variable,
                propertyRuleAst,
                elementType
              )),
              Seq(literalRColonOle),
              immutable
            )(pos)
          )
      }

      // Single label name
      (expression match {
        case _: MapExpression => List(
            // Nodes
            (None, s"(:A $expressionString)", Node),
            (Some(varFor("n")), s"(n:A $expressionString)", Node),

            // Relationships
            (None, s"()-[:A $expressionString]-()", Relationship),
            (None, s"()-[:A $expressionString]->()", Relationship),
            (None, s"()<-[:A $expressionString]-()", Relationship),
            (Some(varFor("n")), s"()-[n:A $expressionString]-()", Relationship)
          )
        case _: BooleanExpression => List(
            // Nodes
            (Some(varFor("n")), s"(n:A) WHERE $expressionString", Node),
            (Some(varFor("n")), s"(n:A WHERE $expressionString)", Node),
            (Some(varFor("WHERE")), s"(WHERE:A WHERE $expressionString)", Node), // WHERE as variable
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:A) WHERE $expressionString", Node),
            (None, s"(:A WHERE $expressionString)", Node),

            // Relationships
            (Some(varFor("n")), s"()-[n:A]-() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()<-[n:A]-() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()-[n:A]->() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()-[n:A WHERE $expressionString]-()", Relationship),
            (Some(varFor("WHERE")), s"()-[WHERE:A WHERE $expressionString]-()", Relationship), // WHERE as variable
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"()-[:A]-() WHERE $expressionString", Relationship),
            (None, s"()-[:A WHERE $expressionString]-()", Relationship)
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String, elementType: Element) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(
                Seq(
                  if (elementType == Node) labelQualifierA
                  else relQualifierA
                ),
                variable,
                propertyRuleAst,
                elementType
              )),
              Seq(literalRole),
              immutable
            )(pos)
          )
      }

      // Escaped multi-token label name
      (expression match {
        case _: MapExpression => List(
            // Nodes
            (None, s"(:`A B` $expressionString)", Node),
            (Some(varFor("n")), s"(n:`A B` $expressionString)", Node),

            // Relationships
            (None, s"()-[:`A B` $expressionString]-()", Relationship),
            (Some(varFor("n")), s"()-[n:`A B` $expressionString]-()", Relationship)
          )
        case _: BooleanExpression => List(
            // Nodes
            (Some(varFor("n")), s"(n:`A B`) WHERE $expressionString", Node),
            (Some(varFor("n")), s"(n:`A B` WHERE $expressionString)", Node),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:`A B`) WHERE $expressionString", Node),
            (None, s"(:`A B` WHERE $expressionString)", Node),

            // Relationships
            (Some(varFor("n")), s"()-[n:`A B`]-() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()-[n:`A B` WHERE $expressionString]-()", Relationship),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"()-[:`A B`]-() WHERE $expressionString", Relationship),
            (None, s"()-[:`A B` WHERE $expressionString]-()", Relationship)
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String, elementType: Element) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](func(
            GraphPrivilege(TraverseAction, graphScope)(pos),
            List(PatternQualifier(
              Seq(
                if (elementType == Node) LabelQualifier("A B")(_)
                else RelationshipQualifier("A B")(_)
              ),
              variable,
              propertyRuleAst,
              elementType
            )),
            Seq(literalRole),
            immutable
          )(pos))
      }

      // Label containing colon
      (expression match {
        case _: MapExpression => List(
            // Nodes
            (None, s"(:`:A` $expressionString)", Node),
            (Some(varFor("n")), s"(n:`:A` $expressionString)", Node),

            // Relationships
            (None, s"()-[:`:A` $expressionString]-()", Relationship),
            (Some(varFor("n")), s"()-[n:`:A` $expressionString]-()", Relationship)
          )
        case _: BooleanExpression => List(
            // Nodes
            (Some(varFor("n")), s"(n:`:A`) WHERE $expressionString", Node),
            (Some(varFor("n")), s"(n:`:A` WHERE $expressionString)", Node),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:`:A`) WHERE $expressionString", Node),
            (None, s"(:`:A` WHERE $expressionString)", Node),

            // Relationships
            (Some(varFor("n")), s"()-[n:`:A`]-() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()-[n:`:A` WHERE $expressionString]-()", Relationship),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"()-[:`:A`]-() WHERE $expressionString", Relationship),
            (None, s"()-[:`:A` WHERE $expressionString]-()", Relationship)
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String, elementType: Element) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(
                Seq(
                  if (elementType == Node) LabelQualifier(":A")(_)
                  else RelationshipQualifier(":A")(_)
                ),
                variable,
                propertyRuleAst,
                elementType
              )),
              Seq(literalRole),
              immutable
            )(pos)
          )
      }

      // Multiple labels
      (expression match {
        case _: MapExpression => List(
            // Nodes
            (None, s"(:A|B $expressionString)", Node),
            (Some(varFor("n")), s"(n:A|B $expressionString)", Node),

            // Relationship
            (None, s"()-[:A|B $expressionString]-()", Relationship),
            (Some(varFor("n")), s"()-[n:A|B $expressionString]-()", Relationship)
          )
        case _: BooleanExpression => List(
            // Nodes
            (Some(varFor("n")), s"(n:A|B) WHERE $expressionString", Node),
            (Some(varFor("n")), s"(n:A|B WHERE $expressionString)", Node),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:A|B) WHERE $expressionString", Node),
            (None, s"(:A|B WHERE $expressionString)", Node),

            // Relationship
            (Some(varFor("n")), s"()-[n:A|B]-() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()-[n:A|B WHERE $expressionString]-()", Relationship),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"()-[:A|B]-() WHERE $expressionString", Relationship),
            (None, s"()-[:A|B WHERE $expressionString]-()", Relationship)
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String, elementType: Element) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $propertyRule $preposition role1, $$role2" should
          parseTo[Statements](
            func(
              GraphPrivilege(TraverseAction, graphScope)(pos),
              List(PatternQualifier(
                if (elementType == Node) Seq(labelQualifierA, labelQualifierB)
                else Seq(relQualifierA, relQualifierB),
                variable,
                propertyRuleAst,
                elementType
              )),
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
      val immutableString = maybeImmutable(immutable)
      val expressionString = expressionStringifier(expression)

      (expression match {
        case _: MapExpression => List(
            // Nodes
            (None, s"(:A $expressionString)", Node),
            (Some(varFor("n")), s"(n:A $expressionString)", Node),

            // Relationships
            (None, s"()-[:A $expressionString]-()", Relationship),
            (Some(varFor("n")), s"()-[n:A $expressionString]-()", Relationship)
          )
        case _: BooleanExpression => List(
            // Nodes
            (Some(varFor("n")), s"(n:A) WHERE $expressionString", Node),
            (Some(varFor("n")), s"(n:A WHERE $expressionString)", Node),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"(:A) WHERE $expressionString", Node),
            (None, s"(:A WHERE $expressionString)", Node),

            // Relationships
            (Some(varFor("n")), s"()-[n:A]-() WHERE $expressionString", Relationship),
            (Some(varFor("n")), s"()-[n:A WHERE $expressionString]-()", Relationship),
            // Missing variable is valid when parsing. Fail in semantic check
            (None, s"()-[:A]-() WHERE $expressionString", Relationship),
            (None, s"()-[:A WHERE $expressionString]-()", Relationship)
          )
        case _ => fail("Unexpected expression")
      }).foreach { case (variable: Option[Variable], propertyRule: String, elementType: Element) =>
        val patternQualifier = List(PatternQualifier(
          Seq(
            if (elementType == Node) labelQualifierA
            else relQualifierA
          ),
          variable,
          propertyRuleAst,
          elementType
        ))
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

  test("Allow trailing star in Cypher 5 but not in later versions") {
    s"GRANT TRAVERSE ON GRAPH * FOR (n) WHERE n.prop1 = 1 (*) TO role" should
      parseIn[Statements] {
        case Cypher5 =>
          _.toAst(statementToStatements(grantGraphPrivilege(
            GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
            List(PatternQualifier(
              Seq(LabelAllQualifier() _),
              Some(varFor("n")),
              equals(prop(varFor("n"), "prop1"), literalInt(1)),
              Node
            )),
            Seq(literalRole),
            i = false
          )(pos)))
        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
      }
  }

  test(
    "Different variable should parse correctly to allow them to be rejected in the semantic check with a user-friendly explanation"
  ) {
    s"GRANT TRAVERSE ON GRAPH * FOR (a) WHERE b.prop1 = 1 TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
        List(PatternQualifier(
          Seq(LabelAllQualifier() _),
          Some(varFor("a")),
          equals(prop(varFor("b"), "prop1"), literalInt(1)),
          Node
        )),
        Seq(literalRole),
        i = false
      )(pos)
    )

    s"GRANT TRAVERSE ON GRAPH * FOR ()-[a]-() WHERE b.prop1 = 1 TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
        List(PatternQualifier(
          Seq(RelationshipAllQualifier() _),
          Some(varFor("a")),
          equals(prop(varFor("b"), "prop1"), literalInt(1)),
          Relationship
        )),
        Seq(literalRole),
        i = false
      )(pos)
    )
  }

  test(
    "'WHERE 1 = n.prop1 (foo) TO role' parse as a function to then be rejected in semantic check"
  ) {
    s"GRANT TRAVERSE ON GRAPH * FOR (n) WHERE 1 = n.prop1 (foo) TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
        List(PatternQualifier(
          Seq(LabelAllQualifier() _),
          Some(varFor("n")),
          equals(
            literalInt(1),
            FunctionInvocation.apply(
              FunctionName(Namespace(List("n"))(pos), "prop1")(pos),
              varFor("foo")
            )(pos)
          ),
          Node
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
          Some(varFor("n")),
          equals(
            literalInt(1),
            FunctionInvocation.apply(
              FunctionName(Namespace(List("n"))(pos), "prop1")(pos),
              varFor("foo")
            )(pos)
          ),
          Node
        )),
        Seq(literalRole),
        i = false
      )(pos)
    )

    s"GRANT TRAVERSE ON GRAPH * FOR ()-[n]-() WHERE 1 = n.prop1 (foo) TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
        List(PatternQualifier(
          Seq(RelationshipAllQualifier() _),
          Some(varFor("n")),
          equals(
            literalInt(1),
            FunctionInvocation.apply(
              FunctionName(Namespace(List("n"))(pos), "prop1")(pos),
              varFor("foo")
            )(pos)
          ),
          Relationship
        )),
        Seq(literalRole),
        i = false
      )(pos)
    )

    s"GRANT TRAVERSE ON GRAPH * FOR ()-[n WHERE 1 = n.prop1 (foo)]-() TO role" should parseTo[Statements](
      grantGraphPrivilege(
        GraphPrivilege(TraverseAction, AllGraphsScope()(pos))(pos),
        List(PatternQualifier(
          Seq(RelationshipAllQualifier() _),
          Some(varFor("n")),
          equals(
            literalInt(1),
            FunctionInvocation.apply(
              FunctionName(Namespace(List("n"))(pos), "prop1")(pos),
              varFor("foo")
            )(pos)
          ),
          Relationship
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
          Some(varFor("n")),
          ExistsExpression(
            SingleQuery(
              List(
                Match(
                  optional = false,
                  MatchMode.DifferentRelationships(implicitlyCreated = true)(pos),
                  ForMatch(List(PrefixedPatternPart(
                    AllPaths()(pos),
                    PathPatternPart(NodePattern(Some(varFor("n")), None, None, None)(pos))
                  )))(pos),
                  List(),
                  None,
                  None
                )(pos)
              )
            )(pos)
          )(pos, None, None),
          Node
        )),
        Seq(literalRole),
        i = false
      )(pos)
    )
  }

  test("legitimate property rules, but with problems elsewhere in the privilege command") {
    val genTestCases = for {
      Action(verb, preposition, _) <- Gen.oneOf(actions)
      immutable <- Arbitrary.arbitrary[Boolean]
      graphKeyword <- Gen.oneOf(graphKeywords)
      LiteralExpression(expression, _) <- Gen.oneOf(literalExpressions)
      expressionString = expressionStringifier(expression)
      Scope(graphName, _) <- Gen.oneOf(scopes)
      propertyRule <- expression match {
        case _: MapExpression => Gen.oneOf(
            // Nodes
            s"($expressionString)",
            s"(:A $expressionString)",
            s"(n:A $expressionString)",

            // Relationships
            s"()-[$expressionString]-()",
            s"()-[:A $expressionString]-()",
            s"()-[n:A $expressionString]-()"
          )
        case _: BooleanExpression => Gen.oneOf(
            // Nodes
            s"(n) WHERE $expressionString",
            s"(n WHERE $expressionString)",
            s"(n:A) WHERE $expressionString",
            s"(n:A WHERE $expressionString)",
            // Missing variable is valid when parsing. Fail in semantic check
            s"(:A) WHERE $expressionString",
            s"() WHERE $expressionString",
            s"(:A WHERE $expressionString)",

            // Relationships
            s"()-[n]-() WHERE $expressionString",
            s"()-[n WHERE $expressionString]-()",
            s"()-[n:A]-() WHERE $expressionString",
            s"()-[n:A WHERE $expressionString]-()",
            // Missing variable is valid when parsing. Fail in semantic check
            s"()-[:A]-() WHERE $expressionString",
            s"()-[]-() WHERE $expressionString",
            s"()-[:A WHERE $expressionString]-()"
          )
        case _ => fail("Unexpected expression")
      }
    } yield (verb, preposition, immutable, graphKeyword, graphName, propertyRule)

    forAll(genTestCases, minSuccessful(1000)) {
      case (verb, preposition, immutable, graphKeyword, graphName, propertyRule) =>
        val immutableString = maybeImmutable(immutable)

        // Missing ON
        s"""$verb$immutableString TRAVERSE
           |$graphKeyword $graphName $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Missing role
        s"""$verb$immutableString TRAVERSE
           |ON $graphKeyword $graphName $patternKeyword $propertyRule
           |""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // r:ole is invalid
        s"""$verb$immutableString TRAVERSE
           |ON $graphKeyword $graphName $patternKeyword $propertyRule
           |$preposition r:ole""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Invalid graph name
        s"""$verb$immutableString TRAVERSE
           |ON $graphKeyword f:oo $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Mixing specific graph and *
        s"""$verb$immutableString TRAVERSE
           |ON $graphKeyword foo, * $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString TRAVERSE
           |ON $graphKeyword *, foo $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")

        // Missing graph name
        s"""$verb$immutableString TRAVERSE
           |ON $graphKeyword $patternKeyword $propertyRule
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
        s"""$verb$immutableString TRAVERSE
           |ON $graphKeyword $patternKeyword $propertyRule (*)
           |$preposition role""".stripMargin should notParse[Statements].withMessageContaining("Invalid input")
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
      val immutableString = maybeImmutable(immutable)

      Seq(
        // Nodes
        s"(n:A) WHERE n.prop1 = 1",
        s"(n:A WHERE n.prop1 = 1)",
        s"(:A {prop1:1})",
        s"(n:A {prop1:1})",

        // Relationships
        s"()-[n:A]-() WHERE n.prop1 = 1",
        s"()-[n:A WHERE n.prop1 = 1]-()",
        s"()-[:A {prop1:1}]-()",
        s"()-[n:A {prop1:1}]-()"
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
      val immutableString = maybeImmutable(immutable)
      disallowedPropertyRules.foreach { (disallowedPropertyRule: String) =>
        s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword $disallowedPropertyRule $preposition role" should
          notParse[Statements]
      }

      s"$verb$immutableString TRAVERSE ON $graphKeyword $graphName $patternKeyword (WHERE n.prop1 = 1) $preposition role" should
        parse[Statements]
    }
  }
}
