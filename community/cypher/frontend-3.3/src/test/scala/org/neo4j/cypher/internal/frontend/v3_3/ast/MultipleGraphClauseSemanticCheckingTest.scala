/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.helpers.StringHelper
import org.neo4j.cypher.internal.frontend.v3_3.parser.ParserTest
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, InputPosition, PlannerName, SemanticCheckResult, SemanticErrorDef, SemanticFeature, SemanticState, SemanticTable, ast, parser}
import org.parboiled.scala.Rule1

class MultipleGraphClauseSemanticCheckingTest
  extends ParserTest[ast.Statement, SemanticCheckResult]
    with parser.Statement
    with AstConstructionTestSupport {

  // INFO: Use result.dumpAndExit to debug these tests

  implicit val parser: Rule1[Query] = Query

  test("allows bound nodes in single node pattern GRAPH OF") {
    parsing(
      """MATCH (a:Swedish)
        |RETURN GRAPH result OF (a)""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  test("allows bound relationships in GRAPH OF") {
    parsing(
      """MATCH (a)-[r]->(b)
        |RETURN GRAPH result OF (a)-[r]->(b)""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  test("disallow undirected relationships in GRAPH OF") {
    parsing(
      """MATCH (a)-[r]->(b)
        |RETURN GRAPH result OF (b)-[r]-(a)""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(Set("Only directed relationships are supported in GRAPH OF"))
    }
  }

  test("Weird no context graphs error") {
    parsing(
      """MATCH (n)-->(b:B)
        |WITH count(b) AS nodes
        |RETURN nodes""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (Match(false,Pattern(List(EveryPath(RelationshipChain(NodePattern(Some(Variable(n)),List(),None),RelationshipPattern(None,List(),None,None,OUTGOING,false),NodePattern(Some(Variable(b)),List(LabelName(B)),None))))),List(),None),line 1, column 1 (offset: 0))
          |--
          |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(FunctionInvocation(Namespace(List()),FunctionName(count),false,Vector(Variable(b))),Variable(nodes)))),GraphReturnItems(true,List()),None,None,None,None),line 2, column 1 (offset: 18))
          |--
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(nodes),Variable(nodes)))),None,None,None,None,Set()),line 3, column 1 (offset: 41))
          |--
          |// End
        """

      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    b: 13 29
          |    n: 7
          |  }
          |  {
          |    nodes: 35 48
          |  }
          |  {
          |    nodes: 35 48 49
          |  }
          |}"""
    }
  }

  test("Should fail due to no context graphs") {
    parsing(
      """WITH GRAPHS foo
        |MATCH (n)-->(b:B)
        |WITH count(b) AS nodes GRAPH AT 'uri1' AS bar, GRAPH AT 'uri2' AS baz
        |RETURN nodes""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(Set("No source graph is available in scope"))
    }
  }

  test("GRAPHS * keeps existing graphs in scope (1)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN * GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |source >> target
          |// (Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 2, column 1 (offset: 59))
          |--
          |// End
        """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |}"""
    }
  }

  test("GRAPHS * keeps existing graphs in scope (2)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN GRAPHS *
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        result.formattedContexts shouldEqualFixNewlines
          """// Start
            |--
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (Return(false,DiscardCardinality(),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 2, column 1 (offset: 59))
            |--
            |// End
          """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |}"""
    }
  }

  test("WITH * passes on whatever graphs are in scope") {
    parsing(
      """WITH GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH 1 AS a
        |RETURN * GRAPHS *
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        result.formattedContexts shouldEqualFixNewlines
          """// Start
            |--
            |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(true,List()),None,None,None,None),line 2, column 1 (offset: 52))
            |source >> target
            |// (Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 3, column 1 (offset: 64))
            |--
            |// End
          """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |  { /* source >> target */
            |    a: 62
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |  {
            |    a: 62
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (1)") {
    parsing(
      """WITH GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN GRAPH foo AT 'url', GRAPH bar AT 'url'""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        result.formattedContexts shouldEqualFixNewlines
          """// Start
            |--
            |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (Return(false,DiscardCardinality(),Some(GraphReturnItems(false,List(ReturnedGraph(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(foo)),false)), ReturnedGraph(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(bar)),false))))),None,None,None,Set()),line 2, column 1 (offset: 52))
            |--
            |// End
          """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |  {
            |    GRAPH bar: 85
            |    GRAPH foo: 65
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (2)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        result.formattedContexts shouldEqualFixNewlines
          """
            |// Start
            |--
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set()),line 2, column 1 (offset: 59))
            |--
            |// End
            """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10 66
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 66 67
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (3)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH a GRAPH source
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        result.formattedContexts shouldEqualFixNewlines
          """// Start
            |--
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(source),Some(Variable(source)),false)))),None,None,None,None),line 2, column 1 (offset: 59))
            |source >> source
            |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set()),line 3, column 1 (offset: 79))
            |--
            |// End
          """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10 64
            |    GRAPH source: 18 72
            |    GRAPH target: 43
            |  }
            |  { /* source >> source */
            |    a: 10 64 65 86
            |    GRAPH source: 72
            |  }
            |  {
            |    a: 10 64 65 86 87
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (4)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH a GRAPH source
        |WITH a GRAPH target
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errorMessages should equal(Set("Variable `target` not defined"))
    }
  }

  test("Intermediary clauses don't lose graphs") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |MATCH (b)
        |WITH a
        |MATCH (c)
        |RETURN a, c""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        result.formattedContexts shouldEqualFixNewlines
          """
            |// Start
            |--
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None),line 2, column 1 (offset: 59))
            |source >> target
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),GraphReturnItems(true,List()),None,None,None,None),line 3, column 1 (offset: 69))
            |source >> target
            |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(c)),List(),None)))),List(),None),line 4, column 1 (offset: 76))
            |source >> target
            |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)), AliasedReturnItem(Variable(c),Variable(c)))),None,None,None,None,Set()),line 5, column 1 (offset: 86))
            |--
            |// End
          """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10 74
            |    b: 66
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  { /* source >> target */
            |    a: 10 74 75 93
            |    c: 83 96
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 74 75 93 94
            |    c: 83 96 97
            |  }
            |}"""
    }
  }

  test("FROM introduces new source and target graphs") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |FROM GRAPH new AT 'new'
        |MATCH (b)
        |RETURN a GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.formattedContexts shouldEqualFixNewlines
          """// Start
            |--
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(new))),Some(Variable(new)),false),None))),None,None,None,None),line 2, column 6 (offset: 64))
            |new >> new
            |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None),line 3, column 1 (offset: 83))
            |new >> new
            |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 4, column 1 (offset: 93))
            |--
            |// End
          """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  { /* new >> new */
            |    a: 10 100
            |    b: 90
            |    GRAPH new: 70
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 100 101
            |    GRAPH new: 70
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |}"""
    }
  }

  test("INTO introduces new target graph") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |INTO GRAPH new AT 'new'
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>

        result.formattedContexts shouldEqualFixNewlines
          """// Start
            |--
            |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
            |source >> target
            |// (With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List(NewTargetGraph(GraphAtAs(GraphUrl(Right(StringLiteral(new))),Some(Variable(new)),false)))),None,None,None,None),line 2, column 6 (offset: 64))
            |source >> new
            |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set()),line 3, column 1 (offset: 83))
            |--
            |// End
          """
        result.formattedScopes shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  { /* source >> new */
            |    a: 10 90
            |    GRAPH new: 70
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 90 91
            |  }
            |}"""
    }
  }

  test("graph name conflict") {
    parsing(
      """WITH GRAPH AT 'url' AS foo >> GRAPH AT 'url' AS bar
        |WITH GRAPHS >> foo AS foo, bar AS foo
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errorMessages should equal(Set("Multiple result graphs with the same name are not supported"))
    }
  }

  test("WITH is allowed to refer to missing graphs at the start of query") {
    parsing(
      """WITH GRAPHS foo >> bar
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(1)))),None,None,None,None,Set()),line 2, column 1 (offset: 23))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  {
          |    1: 31
          |  }
          |}"""
    }
  }

  test("with is not allowed to refer to missing graphs anywhere else") {
    parsing(
      """WITH GRAPHS foo >> bar
        |WITH GRAPHS zig >> zag
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(Set(
        "Variable `zig` not defined",
        "Variable `zag` not defined"
      ))
    }
  }

  test("graphs are not lost across update statements") {
    parsing(
      """WITH GRAPHS foo >> bar
        |CREATE (a)
        |MERGE (b {name: a.name})
        |CREATE (b)-[:X]->(b)
        |WITH *
        |DELETE (a)
        |RETURN GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List()),None,None,None,None),line 5, column 1 (offset: 80))
          |foo >> bar
          |// (Return(false,DiscardCardinality(),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 7, column 1 (offset: 98))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    a: 31 50
          |    b: 41 67 77
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    a: 31 50 95
          |    b: 41 67 77
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |}"""
    }
  }

  test("create graph") {
    parsing(
      """WITH GRAPHS foo >> bar
        |CREATE GRAPH zig AT 'url'
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (CreateRegularGraph(false,Variable(zig),None,GraphUrl(Right(StringLiteral(url)))),line 2, column 8 (offset: 30))
          |foo >> bar
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(1)))),None,None,None,None,Set()),line 3, column 1 (offset: 49))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |    GRAPH zig: 36
          |  }
          |  {
          |    1: 57
          |  }
          |}"""
    }
  }

  test("Resolution of unaliased SOURCE GRAPH") {
    parsing(
      """WITH GRAPHS foo >> bar, baz
        |MATCH (a)
        |WITH a GRAPH baz, SOURCE GRAPH >>
        |MATCH (b)
        |RETURN * GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))), ReturnedGraph(GraphAs(Variable(baz),Some(Variable(baz)),false)))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(),None)))),List(),None),line 2, column 1 (offset: 28))
          |foo >> bar
          |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(baz),Some(Variable(baz)),false)), NewContextGraphs(SourceGraphAs(None,false),None))),None,None,None,None),line 3, column 1 (offset: 38))
          |foo >> foo
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None),line 4, column 1 (offset: 72))
          |foo >> foo
          |// (Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 5, column 1 (offset: 82))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH baz: 24
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    a: 35 43
          |    GRAPH bar: 19
          |    GRAPH baz: 24 51
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> foo */
          |    a: 35 43 44
          |    b: 79
          |    GRAPH baz: 51
          |    GRAPH foo: 69
          |  }
          |  {
          |    a: 35 43 44
          |    b: 79
          |    GRAPH baz: 51
          |    GRAPH foo: 69
          |  }
          |}"""
    }
  }

  test("create graph and set source") {
    parsing(
      """WITH GRAPHS foo >> bar
        |CREATE GRAPH zig AT 'url' >>
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (CreateRegularGraph(false,Variable(zig),None,GraphUrl(Right(StringLiteral(url)))),line 2, column 27 (offset: 49))
          |foo >> bar
          |// (With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List(NewContextGraphs(GraphAs(Variable(zig),Some(Variable(zig)),false),None))),None,None,None,None),line 2, column 27 (offset: 49))
          |zig >> zig
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(1)))),None,None,None,None,Set()),line 3, column 1 (offset: 52))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |    GRAPH zig: 36
          |  }
          |  { /* zig >> zig */
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |    GRAPH zig: 36
          |  }
          |  {
          |    1: 60
          |  }
          |}"""
    }
  }

  test("Resolution of unaliased SOURCE GRAPH does not fail if graph is also passed on explicitly") {
    parsing(
      """WITH GRAPHS foo >> bar, baz
        |MATCH (a)
        |WITH a GRAPH baz, GRAPH foo, SOURCE GRAPH >>
        |MATCH (b)
        |RETURN * GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))), ReturnedGraph(GraphAs(Variable(baz),Some(Variable(baz)),false)))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(),None)))),List(),None),line 2, column 1 (offset: 28))
          |foo >> bar
          |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(baz),Some(Variable(baz)),false)), ReturnedGraph(GraphAs(Variable(foo),Some(Variable(foo)),false)), NewContextGraphs(SourceGraphAs(None,false),None))),None,None,None,None),line 3, column 1 (offset: 38))
          |foo >> foo
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None),line 4, column 1 (offset: 83))
          |foo >> foo
          |// (Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 5, column 1 (offset: 93))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH baz: 24
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    a: 35 43
          |    GRAPH bar: 19
          |    GRAPH baz: 24 51
          |    GRAPH foo: 12 62
          |  }
          |  { /* foo >> foo */
          |    a: 35 43 44
          |    b: 90
          |    GRAPH baz: 51
          |    GRAPH foo: 62 80
          |  }
          |  {
          |    a: 35 43 44
          |    b: 90
          |    GRAPH baz: 51
          |    GRAPH foo: 62 80
          |  }
          |}"""
    }
  }

  test("Resolution of unaliased TARGET GRAPH does not fail if graph is also passed on explicitly") {
    parsing(
      """WITH GRAPHS foo >> bar, baz
        |MATCH (a)
        |WITH a GRAPH bar, GRAPH foo >> TARGET GRAPH
        |MATCH (b)
        |RETURN * GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))), ReturnedGraph(GraphAs(Variable(baz),Some(Variable(baz)),false)))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(),None)))),List(),None),line 2, column 1 (offset: 28))
          |foo >> bar
          |// (With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(bar),Some(Variable(bar)),false)), NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(TargetGraphAs(None,false))))),None,None,None,None),line 3, column 1 (offset: 38))
          |foo >> bar
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None),line 4, column 1 (offset: 82))
          |foo >> bar
          |// (Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 5, column 1 (offset: 92))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH baz: 24
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    a: 35 43
          |    GRAPH bar: 19 51
          |    GRAPH baz: 24
          |    GRAPH foo: 12 62
          |  }
          |  { /* foo >> bar */
          |    a: 35 43 44
          |    b: 89
          |    GRAPH bar: 51
          |    GRAPH foo: 62 82
          |  }
          |  {
          |    a: 35 43 44
          |    b: 89
          |    GRAPH bar: 51
          |    GRAPH foo: 62 82
          |  }
          |}"""
    }
  }

  test("Track graphs with generated names") {
    parsing(
      """WITH GRAPH AT 'url' >> bar, GRAPH baz
        |MATCH (a)
        |MATCH (b)
        |RETURN * GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(  FRESHID21)),true),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))), ReturnedGraph(GraphAs(Variable(baz),Some(Variable(baz)),false)))),None,None,None,None),line 1, column 1 (offset: 0))
          |  FRESHID21 >> bar
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(),None)))),List(),None),line 2, column 1 (offset: 38))
          |  FRESHID21 >> bar
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None),line 3, column 1 (offset: 48))
          |  FRESHID21 >> bar
          |// (Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 4, column 1 (offset: 58))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 23
          |    GRAPH baz: 34
          |  }
          |  { /*   FRESHID21 >> bar */
          |    GRAPH   FRESHID21 (generated): 21
          |    a: 45
          |    b: 55
          |    GRAPH bar: 23
          |    GRAPH baz: 34
          |  }
          |  {
          |    GRAPH   FRESHID21 (generated): 21
          |    a: 45
          |    b: 55
          |    GRAPH bar: 23
          |    GRAPH baz: 34
          |  }
          |}"""
    }
  }

  test("UNION ALL handles graphs correctly") {
    parsing(
      """WITH GRAPH bar AT 'url' >> GRAPH foo AT 'url2'
        |MATCH (a:Person)
        |RETURN a.name GRAPHS *
        |UNION ALL
        |WITH GRAPH bar AT 'url' >> GRAPH foo AT 'url2'
        |MATCH (a:City)
        |RETURN a.name GRAPHS *
        |""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(bar)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(url2))),Some(Variable(foo)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |bar >> foo
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(LabelName(Person)),None)))),List(),None),line 2, column 1 (offset: 47))
          |bar >> foo
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Property(Variable(a),PropertyKeyName(name)),Variable(a.name)))),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 3, column 1 (offset: 64))
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(bar)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(url2))),Some(Variable(foo)),false))))),None,None,None,None),line 5, column 1 (offset: 97))
          |bar >> foo
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(LabelName(City)),None)))),List(),None),line 6, column 1 (offset: 144))
          |bar >> foo
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Property(Variable(a),PropertyKeyName(name)),Variable(a.name)))),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 7, column 1 (offset: 159))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    {
          |    }
          |    { /* bar >> foo */
          |      a: 54 71
          |      GRAPH bar: 11
          |      GRAPH foo: 33
          |    }
          |    {
          |      a.name: 74
          |      GRAPH bar: 11
          |      GRAPH foo: 33
          |    }
          |  }
          |  {
          |    {
          |    }
          |    { /* bar >> foo */
          |      a: 151 166
          |      GRAPH bar: 108
          |      GRAPH foo: 130
          |    }
          |    {
          |      a.name: 169
          |      GRAPH bar: 108
          |      GRAPH foo: 130
          |    }
          |  }
          |}"""
    }
  }

  test("UNION handles graphs correctly") {
    parsing(
      """WITH GRAPH bar AT 'url' >> GRAPH foo AT 'url2'
        |MATCH (a:Person)
        |RETURN a.name GRAPHS *
        |UNION
        |WITH GRAPH bar AT 'url' >> GRAPH foo AT 'url2'
        |MATCH (a:City)
        |RETURN a.name GRAPHS *
        |""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(bar)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(url2))),Some(Variable(foo)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |bar >> foo
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(LabelName(Person)),None)))),List(),None),line 2, column 1 (offset: 47))
          |bar >> foo
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Property(Variable(a),PropertyKeyName(name)),Variable(a.name)))),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 3, column 1 (offset: 64))
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(bar)),false),Some(GraphAtAs(GraphUrl(Right(StringLiteral(url2))),Some(Variable(foo)),false))))),None,None,None,None),line 5, column 1 (offset: 93))
          |bar >> foo
          |// (Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(LabelName(City)),None)))),List(),None),line 6, column 1 (offset: 140))
          |bar >> foo
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(Property(Variable(a),PropertyKeyName(name)),Variable(a.name)))),Some(GraphReturnItems(true,List())),None,None,None,Set()),line 7, column 1 (offset: 155))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    {
          |    }
          |    { /* bar >> foo */
          |      a: 54 71
          |      GRAPH bar: 11
          |      GRAPH foo: 33
          |    }
          |    {
          |      a.name: 74
          |      GRAPH bar: 11
          |      GRAPH foo: 33
          |    }
          |  }
          |  {
          |    {
          |    }
          |    { /* bar >> foo */
          |      a: 147 162
          |      GRAPH bar: 104
          |      GRAPH foo: 126
          |    }
          |    {
          |      a.name: 165
          |      GRAPH bar: 104
          |      GRAPH foo: 126
          |    }
          |  }
          |}"""
    }
  }

  test("create graph and set target") {
    parsing(
      """WITH GRAPHS foo >> bar
        |CREATE >> GRAPH zig AT 'url'
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (CreateRegularGraph(false,Variable(zig),None,GraphUrl(Right(StringLiteral(url)))),line 2, column 11 (offset: 33))
          |foo >> bar
          |// (With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List(NewTargetGraph(GraphAs(Variable(zig),Some(Variable(zig)),false)))),None,None,None,None),line 2, column 11 (offset: 33))
          |foo >> zig
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(1)))),None,None,None,None,Set()),line 3, column 1 (offset: 52))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |    GRAPH zig: 39
          |  }
          |  { /* foo >> zig */
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |    GRAPH zig: 39
          |  }
          |  {
          |    1: 60
          |  }
          |}"""
    }
  }

  test("using graph variable in normal variable position") {
    parsing(
      """WITH GRAPHS foo >> bar
        |RETURN foo GRAPHS bar
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("`foo` already declared as graph")
      )
    }
  }

  test("using normal variable in graph position") {
    parsing(
      """WITH 1 AS a GRAPHS foo >> bar
        |RETURN GRAPHS a
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("`a` already declared as variable")
      )
    }
  }

  test("using normal variable in graph definition") {
    parsing(
      """WITH 1 AS a GRAPHS foo >> bar
        |WITH GRAPH a
        |RETURN GRAPHS a
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("`a` already declared as variable")
      )
    }

  }

  test("persist graph") {
    parsing(
      """WITH GRAPHS foo >> bar
        |PERSIST GRAPH foo TO 'url'
        |RETURN GRAPHS bar
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (Persist(GraphAs(Variable(foo),Some(Variable(foo)),false),GraphUrl(Right(StringLiteral(url)))),line 2, column 22 (offset: 44))
          |foo >> bar
          |// (Return(false,DiscardCardinality(),Some(GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,Set()),line 3, column 1 (offset: 50))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19 64
          |    GRAPH foo: 12 37
          |  }
          |  {
          |    GRAPH bar: 64
          |  }
          |}"""
    }
  }

  test("relocate graph") {
    parsing(
      """WITH GRAPHS foo >> bar
        |RELOCATE GRAPH foo TO 'url'
        |RETURN GRAPHS bar
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (Relocate(GraphAs(Variable(foo),Some(Variable(foo)),false),GraphUrl(Right(StringLiteral(url)))),line 2, column 23 (offset: 45))
          |foo >> bar
          |// (Return(false,DiscardCardinality(),Some(GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,Set()),line 3, column 1 (offset: 51))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19 65
          |    GRAPH foo: 12 38
          |  }
          |  {
          |    GRAPH bar: 65
          |  }
          |}"""
    }
  }

  test("delete graphs") {
    parsing(
      """WITH GRAPHS foo >> bar
        |DELETE GRAPHS foo, bar
        |RETURN 1
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (DeleteGraphs(List(Variable(foo), Variable(bar))),line 2, column 15 (offset: 37))
          |foo >> bar
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(1)))),None,None,None,None,Set()),line 3, column 1 (offset: 46))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19 42
          |    GRAPH foo: 12 37
          |  }
          |  {
          |    1: 54
          |  }
          |}"""
    }
  }

  test("snapshot graph") {
    parsing(
      """WITH GRAPHS foo >> bar
        |SNAPSHOT GRAPH foo TO 'url'
        |RETURN 1
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts shouldEqualFixNewlines
        """// Start
          |--
          |// (With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo)),false),Some(GraphAs(Variable(bar),Some(Variable(bar)),false))))),None,None,None,None),line 1, column 1 (offset: 0))
          |foo >> bar
          |// (Snapshot(GraphAs(Variable(foo),Some(Variable(foo)),false),GraphUrl(Right(StringLiteral(url)))),line 2, column 23 (offset: 45))
          |foo >> bar
          |// (Return(false,ReturnItems(false,List(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(1)))),None,None,None,None,Set()),line 3, column 1 (offset: 51))
          |--
          |// End"""
      result.formattedScopes shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19
          |    GRAPH foo: 12 38
          |  }
          |  {
          |    1: 59
          |  }
          |}"""
    }
  }

  implicit class verify(actual: String) {
    def shouldEqualFixNewlines(expected: String): Unit = {
      StringHelper.RichString(actual.trim.stripMargin).fixNewLines should equal(StringHelper.RichString(expected.trim.stripMargin).fixNewLines)
    }
  }

  private def fullScopeTree(state: SemanticState): String = state.scopeTree.toStringWithoutId.trim

  import scala.compat.Platform.EOL

  private def contextsByPosition(state: SemanticState): String = {
    val astNodes = state.recordedContextGraphs.keyPositionSet ++ state.recordedScopes.keyPositionSet
    val keys = astNodes.toSeq.sortBy(x => x._2.offset -> x._1.toString)
    val values = keys.map(ast => s"${state.recordedContextGraphs.getOrElse(ast._1, "--")}$EOL// $ast")
    val finalContext = state.currentScope.contextGraphs.map(_.toString).getOrElse("--")
    s"// Start$EOL${values.mkString(EOL)}$EOL$finalContext$EOL// End"
  }

  override def convert(astNode: ast.Statement): SemanticCheckResult = {
    val rewritten = PreparatoryRewriting.transform(TestState(astNode), TestContext).statement()
    val initialState = SemanticState.clean.withFeatures(SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature)
    rewritten.semanticCheck(initialState)
  }

  implicit final class RichSemanticCheckResult(val result: SemanticCheckResult) {
    def state: SemanticState = result.state

    def errors: Seq[SemanticErrorDef] = result.errors

    def errorMessages: Set[String] = errors.map(_.msg).toSet

    def formattedContexts: String = contextsByPosition(state)

    def formattedScopes: String = fullScopeTree(state)

    // Useful for working on these tests

    def dumpAndExit(): Unit = {
      dump()
      System.exit(1)
    }

    def dumpAndFail(): Unit = {
      dump()
      fail()
    }

    def dumpAndFail(msg: String): Unit = {
      dump()
      fail(msg)
    }

    def dump(): Unit = {
      println("\n// *** ERRORS")
      println(errorMessages.mkString("\n"))
      println("\n// *** CONTEXTS")
      println(contextsByPosition(state))
      println("\n// *** SCOPES")
      println(fullScopeTree(state))
    }
  }

  //noinspection TypeAnnotation
  case class TestState(override val statement: ast.Statement) extends BaseState {
    override def queryText: String = statement.toString

    override def startPosition: None.type = None

    override object plannerName extends PlannerName {
      override def name: String = "Test"

      override def toTextOutput: String = name
    }

    override def maybeStatement = None

    override def maybeSemantics = None

    override def maybeExtractedParams = None

    override def maybeSemanticTable = None

    override def accumulatedConditions = Set.empty

    override def withStatement(s: Statement) = copy(s)

    override def withSemanticTable(s: SemanticTable) = ???

    override def withSemanticState(s: SemanticState) = ???

    override def withParams(p: Map[String, Any]) = ???
  }

  //noinspection TypeAnnotation
  object TestContext extends BaseContext {
    override def tracer = CompilationPhaseTracer.NO_TRACING

    override def notificationLogger = mock[InternalNotificationLogger]

    override object exceptionCreator extends ((String, InputPosition) => CypherException) {
      override def apply(msg: String, pos: InputPosition): CypherException = throw new CypherException() {
        override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T =
          mapper.internalException(msg, this)
      }
    }

    override def monitors = mock[Monitors]

    override def errorHandler = _ => ()
  }

}
