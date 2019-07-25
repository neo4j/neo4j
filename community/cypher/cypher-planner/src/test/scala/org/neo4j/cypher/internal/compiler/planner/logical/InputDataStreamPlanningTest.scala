/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.phases._
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticState, SemanticTable}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.parser.{Expressions, Statement}
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{InputPosition, SyntaxException}
import org.parboiled.scala.{EOI, Parser, Rule1, group}

class InputDataStreamPlanningTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("INPUT DATA STREAM a, b, c RETURN *") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c RETURN *")._2 should equal(Input(Seq("a", "b", "c")))
  }

  test("INPUT DATA STREAM a, b, c RETURN sum(a)") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c RETURN sum(a)")._2 should equal(
      Apply(Input(Seq("a", "b", "c")), Aggregation(Argument(Set("a", "b", "c")), Map.empty, Map("sum(a)" -> sum(varFor("a")))))
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * WHERE a.pid = 99 RETURN *") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c WITH * WHERE a.pid = 99 RETURN *")._2 should equal(
      Selection(ands(propEquality("a", "pid", 99)), Input(Seq("a", "b", "c")))
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * WHERE a:Employee RETURN a.name AS name ORDER BY name") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c WITH * WHERE a:Employee RETURN a.name AS name ORDER BY name")._2 should equal(
      Apply(
        Input(Seq("a", "b", "c")),
        Sort(
          Projection(
            Selection(ands(hasLabels("a", "Employee")), Argument(Set("a", "b", "c"))), Map("name" -> prop("a", "name"))
          ),
          List(Ascending("name"))
        )
      )
    )
  }

  test("INPUT DATA STREAM a, b, c UNWIND [] AS a RETURN *") {
    val caught = intercept[SyntaxException] {
      new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c UNWIND [] AS a RETURN *")
    }
    assert(caught.getMessage.startsWith("Variable `a` already declared"))
  }

  test("UNWIND [0, 1] AS x INPUT DATA STREAM a, b, c RETURN *") {
    val caught = intercept[SyntaxException] {
      new given().getLogicalPlanFor("UNWIND [0, 1] AS x INPUT DATA STREAM a, b, c RETURN *")
    }
    assert(caught.getMessage.startsWith("INPUT DATA STREAM must be the first clause in a query"))
  }

  test("INPUT DATA STREAM a INPUT DATA STREAM b RETURN *") {
    val caught = intercept[SyntaxException] {
      new given().getLogicalPlanFor("INPUT DATA STREAM a INPUT DATA STREAM b RETURN *")
    }
    assert(caught.getMessage.startsWith("There can be only one INPUT DATA STREAM in a query"))
  }

  test("INPUT DATA STREAM x RETURN x UNION MATCH (x) RETURN x") {
    val caught = intercept[SyntaxException] {
      new given().getLogicalPlanFor("INPUT DATA STREAM x RETURN x UNION MATCH (x) RETURN x")
    }
    assert(caught.getMessage.startsWith("INPUT DATA STREAM is not supported in UNION queries"))
  }

  test("MATCH (x) RETURN x UNION INPUT DATA STREAM x RETURN x") {
    val caught = intercept[SyntaxException] {
      new given().getLogicalPlanFor("MATCH (x) RETURN x UNION INPUT DATA STREAM x RETURN x")
    }
    assert(caught.getMessage.startsWith("INPUT DATA STREAM is not supported in UNION queries"))
  }

  override def pipeLine(): Transformer[PlannerContext, BaseState, LogicalPlanState] = {
    IdsTestParsing andThen super.pipeLine()
  }

  // Both the test parser that understands INPUT DATA STREAM and the standard parser are part of the pipeline,
  // the standard parser will be fed with 'RETURN 1' just to make it happy
  override def createInitState(queryString: String): BaseState = IdsTestInitialState(queryString, "RETURN 1", None, IDPPlannerName)

  case class IdsTestInitialState(idsQueryText: String,
                                 queryText: String,
                                 startPosition: Option[InputPosition],
                                 plannerName: PlannerName,
                                 initialFields: Map[String, CypherType] = Map.empty,
                                 maybeStatement: Option[ast.Statement] = None,
                                 maybeSemantics: Option[SemanticState] = None,
                                 maybeExtractedParams: Option[Map[String, Any]] = None,
                                 maybeSemanticTable: Option[SemanticTable] = None,
                                 accumulatedConditions: Set[Condition] = Set.empty) extends BaseState {


    override def withStatement(s: ast.Statement): IdsTestInitialState = {
      // the unmodified parser is part of the pipeline and it will try to set the result of parsing 'RETURN 1'
      // we simply ignore statements that do not contain InputDataStream AST node
      if (s.findByAllClass[ast.InputDataStream].isEmpty) {
        copy()
      } else {
        copy(maybeStatement = Some(s))
      }
    }

    override def withSemanticTable(s: SemanticTable): IdsTestInitialState = copy(maybeSemanticTable = Some(s))

    override def withSemanticState(s: SemanticState): IdsTestInitialState = copy(maybeSemantics = Some(s))

    override def withParams(p: Map[String, Any]): IdsTestInitialState = copy(maybeExtractedParams = Some(p))
  }

  case object IdsTestParsing extends Phase[BaseContext, BaseState, BaseState] {
    private val parser = new IdsTestCypherParser

    override def process(in: BaseState, ignored: BaseContext): BaseState = {
      val idsIn = in.asInstanceOf[IdsTestInitialState]
      idsIn.withStatement(parser.parse(idsIn.idsQueryText, in.startPosition))
    }

    override val phase = PARSING

    override val description = "parse text into an AST object"

    override def postConditions = Set(BaseContains[ast.Statement])
  }

  class IdsTestCypherParser extends Parser
    with Statement
    with Expressions {


    @throws(classOf[SyntaxException])
    def parse(queryText: String, offset: Option[InputPosition] = None): ast.Statement =
      parseOrThrow(queryText, offset, IdsTesCypherParser.Statements)
  }

  object IdsTesCypherParser extends Parser with Statement with Expressions {
    val Statements: Rule1[Seq[ast.Statement]] = rule {
      oneOrMore(WS ~ Statement ~ WS, separator = ch(';')) ~~ optional(ch(';')) ~~ EOI.label("end of input")
    }

    override def Clause: Rule1[ast.Clause] = (
      FromGraph
        | Unwind
        | With
        | Match
        | Call
        | Return
        | InputDataStream
      )

    def InputDataStream: Rule1[ast.InputDataStream] = rule("INPUT DATA STREAM") {
      group(keyword("INPUT DATA STREAM") ~~ oneOrMore(Variable, separator = CommaSep)) ~~>> (ast.InputDataStream(_))
    }
  }

}
