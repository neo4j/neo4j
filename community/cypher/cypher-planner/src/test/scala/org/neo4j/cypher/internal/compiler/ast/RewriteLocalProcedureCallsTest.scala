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
package org.neo4j.cypher.internal.compiler.ast

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersion.Cypher25
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.LocalFieldSignature
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.LocalCallables
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.LocalDefinitionsDirectory
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.ResolvedLocalCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExtractLocalDefinitions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.NodeVectorIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.RelationshipVectorIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.planner.spi.VectorIndexError
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.ProcedureOutput
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.exceptions.Neo4jException
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.constraints.ConstrainableType
import org.scalatest.Assertions

import scala.util.Random

class RewriteLocalProcedureCallsTest extends CypherPlannerTestSuite with TestName with AstConstructionTestSupport {

  // Note that the may be fewer tests actually executed, since equal fuzz tests are executed only once
  private val numberOfFuzzTests = 500

  private val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  test(
    s"""DEFINE PROCEDURE foo() {
       |  FINISH
       |}
       |
       |CALL foo()""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      finish()
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo
      )(
        resolveLocalCall(
          procedureName("foo"),
          declaredResults = false
        )
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  CREATE (:Bar)
       |}
       |
       |CALL foo()""".stripMargin
  ) {
    val createLabel = localProcedureDefinition("foo").body(
      create(nodePat(labelExpression = Some(labelLeaf("Bar"))))
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> createLabel
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        createLabel
      )(
        resolveLocalCall(
          procedureName("foo"),
          bodyContainsUpdates = true,
          declaredResults = false
        )
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN 1 AS x
       |}
       |
       |CALL foo() YIELD x
       |RETURN x""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      return_(
        aliasedReturnItem(literalInt(1), "x")
      )
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo
      )(
        resolveLocalCall(
          procedureName("foo"),
          outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
          callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
          declaredResults = true
        ),
        return_(aliasedReturnItem(v"x", "x"))
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE foo(a) {
       |  RETURN 1 AS x
       |}
       |
       |CALL foo(1) YIELD x
       |RETURN x""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo", localFieldSignature("a")).body(
      return_(
        aliasedReturnItem(literalInt(1), "x")
      )
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo
      )(
        resolveLocalCall(
          procedureName("foo"),
          inputSignature = Seq(localFieldSignature("a")),
          outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
          callArguments = Seq(literalInt(1)),
          callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
          declaredResults = true
        ),
        return_(aliasedReturnItem(v"x", "x"))
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE foo(a :: INT) {
       |  RETURN 1 AS x
       |}
       |
       |CALL foo(1) YIELD x
       |RETURN x""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo", localFieldSignature("a", symbols.CTInteger)).body(
      return_(
        aliasedReturnItem(literalInt(1), "x")
      )
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo
      )(
        resolveLocalCall(
          procedureName("foo"),
          inputSignature = Seq(localFieldSignature("a", symbols.CTInteger)),
          outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
          callArguments = Seq(coerceTo(literalInt(1), symbols.CTInteger)),
          callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
          declaredResults = true
        ),
        return_(aliasedReturnItem(v"x", "x"))
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN 1 AS x
       |}
       |
       |CALL bar() YIELD x
       |RETURN x""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      return_(
        aliasedReturnItem(literalInt(1), "x")
      )
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo
      )(
        /* NOTE:
         * The external callable resolver mockup used in this test trivially resolves every name
         * see `PlanContextMock.procedureSignature` below
         */
        resolveNonLocalCall(
          procedureName("bar"),
          callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
          declaredResults = true
        ),
        return_(aliasedReturnItem(v"x", "x"))
      )
    )
  }

  for {
    (sig, args, argsCoercedOpt) <- Seq(
      (Seq.empty[LocalFieldSignature], Seq(literalInt(1)), None),
      (Seq(localFieldSignature("a")), Seq.empty, None),
      (
        Seq(localFieldSignature("a", symbols.CTInteger)),
        Seq(literalString("abc")),
        Some(Seq(coerceTo(literalString("abc"), symbols.CTInteger)))
      ),
      (Seq(localFieldSignature("a"), localFieldSignature("b")), Seq(literalInt(1)), None),
      (
        Seq(localFieldSignature("a"), localFieldSignature("b"), localFieldSignature("c", literalInt(0))),
        Seq(literalInt(1)),
        None
      ),
      (Seq(localFieldSignature("a"), localFieldSignature("b", literalInt(0))), Seq.empty, None),
      (
        Seq(localFieldSignature("a"), localFieldSignature("b", literalInt(0))),
        Seq(literalInt(1), literalInt(2), literalInt(3)),
        None
      )
    )
    sigCypher = sig.map(f => prettifier.asString(f)).mkString(", ")
    argsCypher = args.map(a => prettifier.expr.apply(a)).mkString(", ")
  } {
    test(
      s"""DEFINE PROCEDURE foo($sigCypher) {
         |  RETURN 1 AS x
         |}
         |
         |CALL foo($argsCypher) YIELD x
         |RETURN x""".stripMargin
    ) {
      val foo = localProcedureDefinition("foo", sig: _*).body(
        return_(
          aliasedReturnItem(literalInt(1), "x")
        )
      )
      cypher25testName.hasExtractedLocalProcedures(
        procedureName("foo") -> foo
      ).isRewrittenTo(
        singleQueryWithLocalDefinitions(
          foo
        )(
          /* NOTE:
           * call resolution is purely by name
           * the arguments are checked in semantics analysis, which is not part of this test
           */
          resolveLocalCall(
            procedureName("foo"),
            inputSignature = sig,
            outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
            callArguments = argsCoercedOpt.getOrElse(args),
            callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
            declaredResults = true
          ),
          return_(aliasedReturnItem(v"x", "x"))
        )
      )
    }
  }

  for {
    parameters <- Seq(
      Seq("x"),
      Seq("x", "y"),
      Seq("x", "y", "z"),
      ('a' to 'z').map(_.toString)
    )
    (fieldsSeq, fieldDefs) <- Seq(
      (
        parameters,
        parameters.map(v => localFieldSignature(v))
      ),
      (
        parameters.map(v => s"$v :: INT"),
        parameters.map(v => localFieldSignature(v, symbols.CTInteger))
      )
    )
    fields = fieldsSeq.mkString(", ")
    argsSeq = parameters.zipWithIndex.map(p => literalInt(p._2))
    args = argsSeq.map(_.stringVal).mkString(", ")
    callArguments = fieldDefs.zip(argsSeq).map {
      case (fieldDef, arg) => fieldDef.typ.map(t => coerceTo(arg, t)).getOrElse(arg)
    }
    innerReturnStr = parameters.mkString(" + ")
    innerReturnAst = {
      val vars = parameters.map(v => varFor(v))
      vars.tail.foldLeft(vars.head.asInstanceOf[Expression]) {
        case (exp, v) => add(exp, v)
      }
    }
  } {
    test(
      s"""DEFINE PROCEDURE foo($fields) {
         |  RETURN $innerReturnStr AS x
         |}
         |
         |CALL foo($args) YIELD x
         |RETURN x""".stripMargin
    ) {
      val foo = localProcedureDefinition(
        "foo",
        fieldDefs: _*
      ).body(
        return_(
          aliasedReturnItem(innerReturnAst, "x")
        )
      )
      cypher25testName.hasExtractedLocalProcedures(
        procedureName("foo") -> foo
      ).isRewrittenTo(
        singleQueryWithLocalDefinitions(
          foo
        )(
          resolveLocalCall(
            procedureName("foo"),
            inputSignature = fieldDefs,
            outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
            callArguments = callArguments,
            callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
            declaredResults = true
          ),
          return_(aliasedReturnItem(v"x", "x"))
        )
      )
    }
  }

  for {
    returnVars <- Seq(
      Seq("x"),
      Seq("x", "y"),
      Seq("x", "y", "z"),
      ('a' to 'z').map(_.toString)
    )
    (fieldsSeq, fieldDefs) <- Seq(
      (returnVars, returnVars.map(v => localFieldSignature(v))),
      (returnVars.map(v => s"$v :: INT"), returnVars.map(v => localFieldSignature(v, symbols.CTInteger)))
    )
    fields = fieldsSeq.mkString(", ")
    indexedReturnVars = returnVars.zipWithIndex
    returns = indexedReturnVars.map { case (v, i) => s"$i AS $v" }.mkString(",")
    returnItems = indexedReturnVars.map { case (v, i) => aliasedReturnItem(literalInt(i), v) }
    procedureResultItems = returnVars.map(v => ProcedureResultItem(varFor(v))(pos))
    yields = returnVars.mkString(", ")
    outerReturnStr = returnVars.mkString(" + ")
    outerReturnAst = {
      val vars = returnVars.map(v => varFor(v))
      vars.tail.foldLeft(vars.head.asInstanceOf[Expression]) {
        case (exp, v) => add(exp, v)
      }
    }
  } {
    test(
      s"""DEFINE PROCEDURE foo() :: ($fields) {
         |  RETURN $returns
         |}
         |
         |CALL foo() YIELD $yields
         |RETURN $outerReturnStr AS res""".stripMargin
    ) {
      val foo = localProcedureDefinition(
        "foo"
      ).out(
        fieldDefs: _*
      ).body(
        return_(
          returnItems: _*
        )
      )
      cypher25testName.hasExtractedLocalProcedures(
        procedureName("foo") -> foo
      ).isRewrittenTo(
        singleQueryWithLocalDefinitions(
          foo
        )(
          resolveLocalCall(
            procedureName("foo"),
            outputSignature = Some(fieldDefs),
            callResults = procedureResultItems,
            declaredResults = true
          ),
          return_(aliasedReturnItem(outerReturnAst, "res"))
        )
      )
    }
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  DEFINE PROCEDURE bar() {
       |    RETURN 1 AS x
       |  }
       |  CALL bar() YIELD x
       |  RETURN 1 AS x
       |}
       |
       |CALL foo() YIELD x
       |RETURN x""".stripMargin
  ) {
    val bar = localProcedureDefinition("bar").body(
      return_(
        aliasedReturnItem(literalInt(1), "x")
      )
    )
    def foo(resolved: Boolean) = localProcedureDefinition("foo").body(
      singleQueryWithLocalDefinitions(
        bar
      )(
        if (resolved) resolveLocalCall(
          procedureName("bar"),
          outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
          callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
          declaredResults = true
        )
        else call(
          Seq.empty,
          "bar",
          yields = Some(Seq(varFor("x")))
        ),
        return_(
          aliasedReturnItem(literalInt(1), "x")
        )
      )
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo(resolved = false),
      procedureName("bar") -> bar
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo(resolved = true)
      )(
        resolveLocalCall(
          procedureName("foo"),
          outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
          callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
          declaredResults = true
        ),
        return_(aliasedReturnItem(v"x", "x"))
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  DEFINE PROCEDURE bar1() {
       |    RETURN 1 AS x1
       |  }
       |  DEFINE PROCEDURE bar2() {
       |    RETURN 2 AS x2
       |  }
       |  CALL bar2() YIELD x2
       |  CALL bar1() YIELD x1
       |  RETURN 1 AS x
       |}
       |
       |CALL foo() YIELD x
       |RETURN x""".stripMargin
  ) {
    def bar(i: Int) = localProcedureDefinition(s"bar$i").body(
      return_(
        aliasedReturnItem(literalInt(i), s"x$i")
      )
    )
    def foo(resolved: Boolean) = localProcedureDefinition("foo").body(
      singleQueryWithLocalDefinitions(
        bar(1),
        bar(2)
      )(
        if (resolved) resolveLocalCall(
          procedureName("bar2"),
          outputSignature = Some(Seq(localFieldSignature("x2", symbols.CTAny))),
          callResults = Seq(ProcedureResultItem(varFor("x2"))(pos)),
          declaredResults = true
        )
        else call(
          Seq.empty,
          "bar2",
          yields = Some(Seq(varFor("x2")))
        ),
        if (resolved) resolveLocalCall(
          procedureName("bar1"),
          outputSignature = Some(Seq(localFieldSignature("x1", symbols.CTAny))),
          callResults = Seq(ProcedureResultItem(varFor("x1"))(pos)),
          declaredResults = true
        )
        else call(
          Seq.empty,
          "bar1",
          yields = Some(Seq(varFor("x1")))
        ),
        return_(
          aliasedReturnItem(literalInt(1), "x")
        )
      )
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo(resolved = false),
      procedureName("bar1") -> bar(1),
      procedureName("bar2") -> bar(2)
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo(resolved = true)
      )(
        resolveLocalCall(
          procedureName("foo"),
          outputSignature = Some(Seq(localFieldSignature("x", symbols.CTAny))),
          callResults = Seq(ProcedureResultItem(varFor("x"))(pos)),
          declaredResults = true
        ),
        return_(aliasedReturnItem(v"x", "x"))
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE db.createLabel(newLabel :: STRING) {
       |  CREATE (:Label {name: newLabel})
       |  FINISH
       |}
       |
       |CALL db.createLabel("Foo")""".stripMargin
  ) {
    val fieldDef = localFieldSignature("newLabel", symbols.CTString)
    val createLabel = localProcedureDefinition("db.createLabel", fieldDef).body(
      create(nodePat(
        labelExpression = Some(labelLeaf("Label")),
        properties = Some(mapOf("name" -> varFor("newLabel")))
      )),
      finish()
    )
    cypher25testName.hasExtractedLocalProcedures(
      procedureName("db", "createLabel") -> createLabel
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        createLabel
      )(
        resolveLocalCall(
          procedureName("db", "createLabel"),
          inputSignature = Seq(fieldDef),
          bodyContainsUpdates = true,
          callArguments = Seq(coerceTo(literalString("Foo"), symbols.CTString)),
          declaredResults = false
        )
      )
    )
  }

  { // generated test cases
    val rand = new Random(0)

    def pickOne[T](list: Seq[T]): T = {
      list(rand.nextInt(list.size))
    }

    def pickOneWeighted[T](list: Seq[(Int, T)]): T = {
      val l = list.foldLeft(Seq.empty[T]) {
        case (l, (i, t)) => l ++ List.fill(i)(t)
      }
      l(rand.nextInt(l.size))
    }

    class Counter {
      private var i = -1
      def next(): Int = {
        i = i + 1
        i
      }
    }

    case class GenCtx(
      depthLimit: Int,
      availableProc: Seq[GenLocalProc] = Seq.empty,
      requestedReturnCol: Option[String] = None,
      counter: Counter = new Counter(),
      depth: Int = 0,
      inImportingWithSubquery: Boolean = false
    ) {
      def depthLimited(w: Int): Int = if (depth < depthLimit) w else 0

      def getChildCtx: GenCtx = copy(depth = depth + 1)
      def getChildCtxWithoutRequestedReturnCol: GenCtx = copy(depth = depth + 1, requestedReturnCol = None)
    }

    trait GenPart {
      def ast(resolved: Boolean): ASTNode
      def procedures: Seq[GenLocalProc]
      def cypher: String = ast(resolved = false) match {
        case s: Statement => prettifier.asString(s)
        case ast          => throw new RuntimeException(s"Unexpected AST node: $ast")
      }
    }

    trait GenExpression extends GenPart {
      def ast(resolved: Boolean): Expression
    }

    case class GenSomeExpression0(ctx: GenCtx, gen: () => Expression) extends GenExpression {
      def ast(resolved: Boolean): Expression = gen()
      override def procedures: Seq[GenLocalProc] = Seq.empty
    }

    case class GenSomeExpression1(ctx: GenCtx, op1: GenExpression, gen: Expression => Expression)
        extends GenExpression {
      def ast(resolved: Boolean): Expression = gen(op1.ast(resolved))
      override def procedures: Seq[GenLocalProc] = op1.procedures
    }

    case class GenExistsSubqueryExpression(ctx: GenCtx) extends GenExpression {
      private val body = genQuery(ctx)
      override def ast(resolved: Boolean): Expression =
        ExistsExpression(body.ast(resolved))(pos, None, None)
      override def procedures: Seq[GenLocalProc] = body.procedures
    }

    case class GenCountSubqueryExpression(ctx: GenCtx) extends GenExpression {
      private val body = genQuery(ctx)
      override def ast(resolved: Boolean): Expression =
        CountExpression(body.ast(resolved))(pos, None, None)
      override def procedures: Seq[GenLocalProc] = body.procedures
    }

    case class GenCollectSubqueryExpression(ctx: GenCtx) extends GenExpression {
      private val body = genQuery(ctx)
      override def ast(resolved: Boolean): Expression =
        CollectExpression(body.ast(resolved))(pos, None, None)
      override def procedures: Seq[GenLocalProc] = body.procedures
    }

    case class GenLocalProc(ctx: GenCtx) extends GenPart {
      val name: ProcedureName = procedureName("local", s"proc${ctx.counter.next()}")
      private val body: GenQuery = genQuery(ctx)
      val procedureResultCol = body.actualReturnCol
      override def ast(resolved: Boolean): LocalProcedureDefinition =
        localProcedureDefinition(name.fullName).body(body.ast(resolved))
      override def procedures: Seq[GenLocalProc] = Seq.empty
    }

    trait GenQuery extends GenPart {
      override def ast(resolved: Boolean): Query
      def actualReturnCol: String
    }

    case class GenQueryWithProcs(ctx: GenCtx, numProc: Int) extends GenQuery {
      {
        if (numProc < 1) throw new RuntimeException("GenQueryWithProcs.numProc shall be at least 1")
      }
      override val procedures: Seq[GenLocalProc] =
        (0 until numProc).map(_ => GenLocalProc(ctx.copy(requestedReturnCol = None)))
      private val query = genQueryAfterDefinition(ctx.copy(availableProc = ctx.availableProc ++ procedures))
      override def actualReturnCol: String = query.actualReturnCol
      override def ast(resolved: Boolean): Query = queryWithLocalDefinitions(
        procedures.map(_.ast(resolved)): _*
      )(
        query.ast(resolved)
      )
    }

    case class GenNext(ctx: GenCtx, numStmt: Int) extends GenQuery {
      {
        if (numStmt < 2) throw new RuntimeException("GenNext.numStmt shall be at least 2")
      }
      private val stmts =
        (0 until numStmt).map(_ => genQueryUnderNext(ctx))
      override def actualReturnCol: String = stmts.last.actualReturnCol
      override def ast(resolved: Boolean): Query = nextStatement(stmts.map(_.ast(resolved)): _*)
      override def procedures: Seq[GenLocalProc] = stmts.foldLeft(Seq.empty[GenLocalProc]) {
        case (agg, stmt) => agg ++ stmt.procedures
      }
    }

    case class GenConditional(numBranches: Int, ctx: GenCtx) extends GenQuery {
      {
        if (numBranches < 1) throw new RuntimeException("GenConditional.numArms shall be at least 1")
      }
      private val withDefault = numBranches > 1
      private val branchConditions =
        (0 until numBranches).map(i => {
          Some(GenSomeExpression1(
            ctx,
            genScalarQueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => parameter("cond", symbols.CTAny))),
            op1 => AstConstructionTestSupport.equals(op1, literalInt(i))
          ))
        })
      private val firstBranchBody = genPartQuery(ctx)
      override def actualReturnCol: String = firstBranchBody.actualReturnCol
      private val remainingBranchBodies =
        branchConditions.tail.map(_ => genPartQuery(ctx.copy(requestedReturnCol = Some(actualReturnCol))))
      private val branchBodies = firstBranchBody +: remainingBranchBodies
      override def ast(resolved: Boolean): Query = {
        val tail = if (withDefault) branchConditions.tail.zip(branchBodies.tail) else branchConditions.zip(branchBodies)
        val default = if (withDefault) conditionalQueryDefault(branchBodies.head.ast(resolved)) else None
        val branches = tail.collect {
          case (Some(cond), body) => conditionalQueryBranch(cond.ast(resolved), body.ast(resolved))
        }
        conditionalQueryWhen(default, branches: _*)
      }
      override def procedures: Seq[GenLocalProc] = branchBodies.foldLeft(Seq.empty[GenLocalProc]) {
        case (agg, stmt) => agg ++ stmt.procedures
      }
    }

    case class GenUnion(ctx: GenCtx, distinct: Boolean) extends GenQuery {
      private val lhs = genPartQuery(ctx)
      override def actualReturnCol: String = lhs.actualReturnCol
      private val rhs = genPartQuery(ctx.copy(requestedReturnCol = Some(actualReturnCol)))
      override def ast(resolved: Boolean): Query = {
        if (distinct) union(lhs.ast(resolved), rhs.ast(resolved))
        else unionAll(lhs.ast(resolved), rhs.ast(resolved))
      }
      override def procedures: Seq[GenLocalProc] = lhs.procedures ++ rhs.procedures
    }

    trait GenPartQuery extends GenQuery {
      override def ast(resolved: Boolean): PartQuery
    }

    case class GenTopLevelBraces(ctx: GenCtx) extends GenPartQuery {
      private val body = genQuery(ctx)
      override val actualReturnCol: String = body.actualReturnCol
      override def ast(resolved: Boolean): PartQuery = topLevelBraces(body.ast(resolved))
      override def procedures: Seq[GenLocalProc] = body.procedures
    }

    case class GenSimpleQuery(ctx: GenCtx, numClauses: Int) extends GenPartQuery {
      {
        if (numClauses < 0) throw new RuntimeException("GenSimpleQuery.numClauses shall be at least 0")
      }
      private val i = ctx.counter.next()
      override val actualReturnCol: String = ctx.requestedReturnCol.getOrElse(s"x$i")
      private val clauses = (0 until numClauses).map(_ => genClause(ctx))
      private val returnExpression = genScalarQueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => literalInt(i)))
      private def clauses(resolved: Boolean): Seq[Clause] =
        clauses.map(_.ast(resolved)) :+ return_(aliasedReturnItem(returnExpression.ast(resolved), actualReturnCol))
      override def ast(resolved: Boolean): PartQuery = singleQuery(clauses(resolved): _*)
      override def procedures: Seq[GenLocalProc] = clauses.flatMap(_.procedures)
    }

    trait GenClause extends GenPart {
      override def ast(resolved: Boolean): Clause
    }

    case class GenCall(ctx: GenCtx) extends GenClause {
      private val availableProcNames = ctx.availableProc.map(p => p.name -> Some(p.procedureResultCol)) ++ Seq(
        procedureName("external", "proc1") -> None,
        procedureName("external", "proc2") -> None
      )
      private val alias = varFor(s"x${ctx.counter.next()}")
      private val procedureToCall = pickOne(availableProcNames)
      override def ast(resolved: Boolean): Clause = procedureToCall match {
        case (p, Some(resultCol)) if resolved =>
          resolveLocalCall(
            p,
            outputSignature = Some(Seq(localFieldSignature(resultCol, symbols.CTAny))),
            callResults =
              Seq(ProcedureResultItem(ProcedureOutput(resultCol)(pos), alias)(pos)),
            declaredResults = true
          )
        case (p, Some(resultCol)) if !resolved =>
          unresolvedCallWithYield(
            p,
            yields = Seq(varFor(resultCol) -> alias)
          )
        case (p, None) if resolved => resolveNonLocalCall(p)
        case (p, _)                => unresolvedCall(p)
      }
      override def procedures: Seq[GenLocalProc] = Seq.empty
    }

    case class GenLet(ctx: GenCtx) extends GenClause {
      private val i = ctx.counter.next()
      private val expression = GenSomeExpression1(
        ctx,
        genScalarQueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => parameter("foo", symbols.CTAny))),
        op => isNotNull(op)
      )
      override def ast(resolved: Boolean): Clause = withAdditionalItemsTyped(
        ParsedAsLet,
        aliasedReturnItem(expression.ast(resolved), ctx.requestedReturnCol.getOrElse(s"x$i"))
      )
      override def procedures: Seq[GenLocalProc] = expression.procedures
    }

    case class GenFilter(ctx: GenCtx) extends GenClause {
      private val expression = GenSomeExpression1(
        ctx,
        genScalarQueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => parameter("foo", symbols.CTAny))),
        op => isNotNull(op)
      )
      override def ast(resolved: Boolean): Clause = withAllTyped(
        Some(where(expression.ast(resolved))),
        ParsedAsFilter
      )
      override def procedures: Seq[GenLocalProc] = expression.procedures
    }

    case class GenInlineCall(ctx: GenCtx, withScopeClause: Boolean) extends GenClause {
      private val subquery = genQuery(ctx.copy(inImportingWithSubquery = !withScopeClause))
      override def ast(resolved: Boolean): Clause = {
        val subqueryAst = subquery.ast(resolved)
        if (withScopeClause) scopeClauseSubqueryCall(isImportingAll = false, Seq.empty, subqueryAst)
        else importingWithSubqueryCall(subqueryAst)
      }
      override def procedures: Seq[GenLocalProc] = subquery.procedures
    }

    def genScalarQueryOrElse(ctx: GenCtx, orElse: GenCtx => GenExpression): GenExpression = {
      pickOneWeighted(Seq(
        ctx.depthLimited(1) -> (() => genScalarQuery(ctx)),
        4 -> (() => orElse(ctx))
      ))()
    }

    def genScalarQuery(ctx: GenCtx): GenExpression = {
      val childCtx = ctx.getChildCtxWithoutRequestedReturnCol
      pickOneWeighted(Seq(
        1 -> (() => GenExistsSubqueryExpression(childCtx)),
        1 -> (() => GenCountSubqueryExpression(childCtx)),
        1 -> (() => GenCollectSubqueryExpression(childCtx))
      ))()
    }

    def genClause(ctx: GenCtx): GenClause = {
      val childCtx = ctx.getChildCtxWithoutRequestedReturnCol
      pickOneWeighted(Seq(
        1 -> (() => GenLet(childCtx)),
        1 -> (() => GenFilter(childCtx)),
        1 -> (() => GenInlineCall(childCtx, withScopeClause = true)),
        1 -> (() => GenInlineCall(childCtx, withScopeClause = false)),
        3 -> (() => GenCall(childCtx))
      ))()
    }

    def genPartQuery(ctx: GenCtx): GenPartQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            1 -> (() => GenSimpleQuery(childCtx, 0)),
            1 -> (() => GenSimpleQuery(childCtx, 1)),
            1 -> (() => GenSimpleQuery(childCtx, 2)),
            1 -> (() => GenSimpleQuery(childCtx, 5))
          )
        else
          Seq(
            1 -> (() => GenSimpleQuery(childCtx, 0)),
            1 -> (() => GenSimpleQuery(childCtx, 1)),
            1 -> (() => GenSimpleQuery(childCtx, 2)),
            1 -> (() => GenSimpleQuery(childCtx, 5)),
            ctx.depthLimited(1) -> (() => GenTopLevelBraces(childCtx))
          )
      )()
    }

    def genQueryUnderNext(ctx: GenCtx): GenQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            ctx.depthLimited(2) -> (() => GenUnion(childCtx, distinct = true)),
            ctx.depthLimited(1) -> (() => GenUnion(childCtx, distinct = false)),
            1 -> (() => genPartQuery(childCtx))
          )
        else
          Seq(
            ctx.depthLimited(2) -> (() => GenUnion(childCtx, distinct = true)),
            ctx.depthLimited(1) -> (() => GenUnion(childCtx, distinct = false)),
            ctx.depthLimited(1) -> (() => GenConditional(1, childCtx)),
            ctx.depthLimited(1) -> (() => GenConditional(2, childCtx)),
            ctx.depthLimited(1) -> (() => GenConditional(3, childCtx)),
            3 -> (() => genPartQuery(childCtx))
          )
      )()
    }

    def genQueryAfterDefinition(ctx: GenCtx): GenQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            ctx.depthLimited(1) -> (() => genQueryUnderNext(childCtx)),
            1 -> (() => genPartQuery(childCtx))
          )
        else
          Seq(
            ctx.depthLimited(1) -> (() => GenNext(childCtx, 2)),
            ctx.depthLimited(1) -> (() => GenNext(childCtx, 3)),
            ctx.depthLimited(1) -> (() => GenNext(childCtx, 5)),
            ctx.depthLimited(3) -> (() => genQueryUnderNext(childCtx)),
            3 -> (() => genPartQuery(childCtx))
          )
      )()
    }

    def genQuery(ctx: GenCtx): GenQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            ctx.depthLimited(1) -> (() => genQueryAfterDefinition(childCtx)),
            ctx.depthLimited(1) -> (() => genQueryUnderNext(childCtx)),
            1 -> (() => genPartQuery(childCtx))
          )
        else
          Seq(
            ctx.depthLimited(2) -> (() => GenQueryWithProcs(childCtx, 1)),
            ctx.depthLimited(1) -> (() => GenQueryWithProcs(childCtx, 3)),
            ctx.depthLimited(3) -> (() => genQueryAfterDefinition(childCtx)),
            ctx.depthLimited(3) -> (() => genQueryUnderNext(childCtx)),
            3 -> (() => genPartQuery(childCtx))
          )
      )()
    }

    val generatedTests = for {
      _ <- 0 until numberOfFuzzTests
      testCase = genQuery(GenCtx(depthLimit = 10))
    } yield testCase

    for {
      testCase <- generatedTests.distinctBy(_.cypher)
      extractedLocalProcedures = testCase.procedures.map(p =>
        p.name -> p.ast(resolved = false)
      )
    } {
      test(testCase.cypher) {
        cypher25testName.hasExtractedLocalProcedures(
          extractedLocalProcedures: _*
        ).isRewrittenTo(testCase.ast(resolved = true))
      }
    }
  }

  def resolveLocalCall(
    procedureName: ProcedureName,
    inputSignature: Seq[LocalFieldSignature] = Seq.empty,
    outputSignature: Option[Seq[LocalFieldSignature]] = None,
    bodyContainsUpdates: Boolean = false,
    callArguments: Seq[Expression] = Seq.empty,
    callResults: Seq[ProcedureResultItem] = Seq.empty,
    declaredArguments: Boolean = true,
    declaredResults: Boolean = false,
    yieldAll: Boolean = false,
    optional: Boolean = false
  ): ResolvedLocalCall = {
    ResolvedLocalCall(
      procedureName,
      inputSignature,
      outputSignature,
      bodyContainsUpdates,
      callArguments,
      callResults.toIndexedSeq,
      declaredArguments,
      declaredResults,
      yieldAll,
      optional
    )(pos)
  }

  def resolveNonLocalCall(
    procedureName: ProcedureName,
    callArguments: Seq[Expression] = Seq.empty,
    callResults: Seq[ProcedureResultItem] = Seq.empty,
    declaredArguments: Boolean = true,
    declaredResults: Boolean = false,
    yieldAll: Boolean = false,
    optional: Boolean = false
  ): ResolvedNonLocalCall = {
    ResolvedNonLocalCall(
      PlanContextMock.procedureSignature(procedureName),
      callArguments,
      callResults.toIndexedSeq,
      declaredArguments,
      declaredResults,
      yieldAll,
      optional
    )(pos)
  }

  trait TestCase {
    def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase
    def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase
    def isRewrittenTo(ast: Statement): TestCase
  }

  case class NoOpTestCase() extends TestCase {
    def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase = this
    def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase = this
    def isRewrittenTo(ast: Statement): TestCase = this
  }

  case class FailedTestCase(
    query: String,
    astOpt: Option[Statement],
    exception: Throwable
  ) extends TestCase {
    def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase = fail()
    def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase = fail()
    def isRewrittenTo(ast: Statement): TestCase = fail()

    private def fail(): TestCase = {
      Assertions.fail(
        s"""Query:
           |
           |$testName
           |
           |has exception:
           |
           |${exception.getClass.getCanonicalName}: ${exception.getMessage}
           |
           |AST:
           |
           |${astOpt.map(pprint.apply(_, width = 200, height = 1000)).getOrElse("—")}
           |
           |AST prettified:
           |
           |${astOpt.map(prettifier.asString).getOrElse("—")}
           |""".stripMargin
      )
      NoOpTestCase()
    }
  }

  case class RanTestCase(
    query: String,
    statementBefore: Statement,
    statementAfter: Statement,
    localDefinitionsDirectory: LocalDefinitionsDirectory,
    cypherVersion: CypherVersion
  ) extends TestCase {

    override def hasExtractedLocalProcedures(expectedProcedures: (
      ProcedureName,
      LocalProcedureDefinition
    )*): TestCase = {
      withClue(s"[has extracted local procedures]") {
        val actualProcedureDefinitions = localDefinitionsDirectory.localProcedureDefinitions
        expectedProcedures.foreach {
          case (name, expected) =>
            withClue(s"[${name.fullName}]") {
              val actualProcedureDefinitionOpt = actualProcedureDefinitions.get(name)
              withClue(s"[is present]") {
                if (actualProcedureDefinitionOpt.isEmpty) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Procedure: ${name.fullName}
                       |
                       |Procedure is expected to be present in extraction but is not.
                       |
                       |AST Before:
                       |
                       |${pprint.apply(statementBefore, width = 200, height = 1000)}
                       |
                       |AST After:
                       |
                       |${pprint.apply(statementAfter, width = 200, height = 1000)}
                       |---
                       |Actual local procedure definitions:
                       |
                       |${pprint.apply(
                        localDefinitionsDirectory.localProcedureDefinitions.toSeq.sortBy(_._1.toString),
                        height = 1000
                      )}
                       |---
                       |Expected local procedure definitions:
                       |
                       |${pprint.apply(expectedProcedures.sortBy(_._1.toString), height = 1000)}
                       |---""".stripMargin
                  )
                }
              }
              withClue(s"[with expected definition]") {
                val actual = actualProcedureDefinitionOpt.get
                if (actual != expected) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Procedure: ${name.fullName}
                       |
                       |Procedure definition not as expected:
                       |---
                       |Actual:
                       |
                       |${pprint.apply(actual, height = 1000)}
                       |---
                       |Expected:
                       |
                       |${pprint.apply(expected, height = 1000)}
                       |---""".stripMargin
                  )
                }
              }
            }
        }
      }
      this
    }

    override def hasExtractedLocalFunctions(expectedFunctions: (FunctionName, LocalFunctionDefinition)*): TestCase = {
      withClue(s"[has extracted local functions]") {
        val actualFunctionDefinitions = localDefinitionsDirectory.localFunctionDefinitions
        expectedFunctions.foreach {
          case (name, expected) =>
            withClue(s"[${name.fullName}]") {
              val actualFunctionDefinitionOpt = actualFunctionDefinitions.get(name)
              withClue(s"[is present]") {
                if (actualFunctionDefinitionOpt.isEmpty) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Function: ${name.fullName}
                       |
                       |Function is expected to be present in extraction but is not.
                       |
                       |AST Before:
                       |
                       |${pprint.apply(statementBefore, height = 1000)}
                       |                       |
                       |AST After:
                       |
                       |${pprint.apply(statementAfter, height = 1000)}
                       |---
                       |Actual local function definitions:
                       |
                       |${pprint.apply(
                        localDefinitionsDirectory.localFunctionDefinitions.toSeq.sortBy(_._1.toString),
                        height = 1000
                      )}
                       |---
                       |Expected local function definitions:
                       |
                       |${pprint.apply(expectedFunctions.sortBy(_._1.toString), height = 1000)}
                       |---""".stripMargin
                  )
                }
              }
              withClue(s"[with expected definition]") {
                val actual = actualFunctionDefinitionOpt.get
                if (actual != expected) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Function: ${name.fullName}
                       |
                       |Function definition not as expected:
                       |---
                       |Actual:
                       |
                       |${pprint.apply(actual, height = 1000)}
                       |---
                       |Expected:
                       |
                       |${pprint.apply(expected, height = 1000)}
                       |---""".stripMargin
                  )
                }
              }
            }
        }
      }
      this
    }

    override def isRewrittenTo(expectedStatement: Statement): TestCase = {
      withClue(s"[is rewritten to]") {
        val actual = normalize(statementAfter)
        val expected = normalize(expectedStatement)
        if (actual != expected) {
          fail(
            s"""Version: $cypherVersion
               |Query:
               |
               |$query
               |
               |Query was not rewritten as expected
               |---
               |Actual:
               |
               |${pprint.apply(actual, height = 1000)}
               |---
               |Expected:
               |
               |${pprint.apply(expected, height = 1000)}
               |---""".stripMargin
          )
        }
      }
      this
    }
  }

  private def normalize(statement: Statement): Statement = {
    statement.endoRewrite(bottomUp(Rewriter.lift {
      case x => x
    }))
  }

  private def initContext(cypherVersion: CypherVersion) =
    ContextHelper.create(
      cypherVersion,
      semanticFeatures = Seq(LocalCallables),
      planContext = PlanContextMock
    )

  case class Ast(
    statement: Statement,
    cypherVersion: CypherVersion
  ) extends TestCase {

    private def ranAst(statement: Statement): TestCase = {
      val state = transformers.transform(initialStateWithStatement(statement), initContext(cypherVersion))
      RanTestCase(prettifier.asString(statement), statement, state.statement(), state.localDefinitions(), cypherVersion)
    }

    override def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase = {
      ranAst(statement).hasExtractedLocalProcedures(procedures: _*)
    }

    override def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase = {
      ranAst(statement).hasExtractedLocalFunctions(functions: _*)
    }

    override def isRewrittenTo(ast: Statement): TestCase = {
      ranAst(statement).isRewrittenTo(ast)
    }
  }

  case class TestName(
    cypherVersion: CypherVersion
  ) extends TestCase {

    private def ran(): TestCase = {
      var statementOpt: Option[Statement] = None
      try {
        val context = initContext(cypherVersion)
        val stateAfterParsing = Parse.transform(initialStateWithQuery(testName), context)
        statementOpt = stateAfterParsing.maybeStatement
        val state = transformers.transform(stateAfterParsing, context)
        RanTestCase(testName, stateAfterParsing.statement(), state.statement(), state.localDefinitions(), cypherVersion)
      } catch {
        case ex: Neo4jException => FailedTestCase(testName, statementOpt, ex)
      }
    }

    override def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase = {
      ran().hasExtractedLocalProcedures(procedures: _*)
    }

    override def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase = {
      ran().hasExtractedLocalFunctions(functions: _*)
    }

    override def isRewrittenTo(ast: Statement): TestCase = {
      ran().isRewrittenTo(ast)
    }
  }

  def cypher25testName: TestName = TestName(Cypher25)

  private val transformers: Transformer[PlannerContext, BaseState, BaseState] =
    PreparatoryRewriting andThen
      ScopeSurveyor andThen
      SemanticAnalysis(warn = Some(false)) andThen
      ExtractLocalDefinitions andThen
      RewriteProcedureCalls

  private def initialStateWithQuery(query: String): InitialState =
    InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)

  private def initialStateWithStatement(statement: Statement): InitialState =
    InitialState(
      prettifier.asString(statement),
      NoPlannerName,
      new AnonymousVariableNameGenerator,
      maybeStatement = Some(statement)
    )

}

object PlanContextMock extends PlanContext {

  override def procedureSignature(name: ProcedureName): ProcedureSignature =
    ProcedureSignature(
      name,
      inputSignature = IndexedSeq.empty[FieldSignature],
      outputSignature = None,
      deprecationInfo = None,
      accessMode = ProcedureReadOnlyAccess,
      description = None,
      warning = None,
      // eager = false,
      id = 0
      // systemProcedure = false,
      // allowExpiredCredentials = false,
      // threadSafe = true
    )
  override def procedureSignatureVersion: Long = 1
  override def functionSignature(name: FunctionName): Option[UserFunctionSignature] = ???

  override def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = ???
  override def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = ???
  override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = ???
  override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = ???
  override def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = ???
  override def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = ???
  override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = ???
  override def indexExistsForLabel(labelId: Int): Boolean = ???
  override def indexExistsForRelType(relTypeId: Int): Boolean = ???

  override def textIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def rangeIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def pointIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def textIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def rangeIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def pointIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???
  override def textIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = ???
  override def rangeIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = ???
  override def pointIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = ???
  override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = ???
  override def rangeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = ???
  override def pointIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = ???
  override def nodeTokenIndex: Option[TokenIndexDescriptor] = ???
  override def relationshipTokenIndex: Option[TokenIndexDescriptor] = ???
  override def nodeVectorIndexByName(indexName: String): Either[VectorIndexError, NodeVectorIndexDescriptor] = ???

  override def relationshipVectorIndexByName(indexName: String)
    : Either[VectorIndexError, RelationshipVectorIndexDescriptor] = ???
  override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = ???
  override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = ???

  override def hasRelationshipPropertyExistenceConstraint(relationshipTypeName: String, propertyKey: String): Boolean =
    ???
  override def getRelationshipPropertiesWithExistenceConstraint(relationshipTypeName: String): Set[String] = ???
  override def getPropertiesWithExistenceConstraint: Set[String] = ???
  override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[ConstrainableType]] = ???

  override def hasNodePropertyTypeConstraint(
    labelName: String,
    propertyKey: String,
    cypherType: ConstrainableType
  ): Boolean = ???

  override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[ConstrainableType]] =
    ???

  override def hasRelationshipPropertyTypeConstraint(
    relTypeName: String,
    propertyKey: String,
    cypherType: ConstrainableType
  ): Boolean = ???

  override def hasRelationshipEndpointLabelConstraint(
    relTypeName: String,
    labelName: String,
    endpointType: EndpointType
  ): Boolean = ???
  override def getRelationshipEndpointLabelConstraints(relTypeName: String): Map[EndpointType, String] = ???
  override def hasNodeLabelConstraint(constrainedLabel: String, impliedLabel: String): Boolean = ???
  override def getNodeLabelConstraints(constrainedLabel: String): Set[String] = ???
  override def lastCommittedTxIdProvider: () => Long = ???
  override def statistics: InstrumentedGraphStatistics = ???
  override def notificationLogger(): InternalNotificationLogger = ???
  override def txStateHasChanges(): Boolean = ???
  override def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlanContext = ???
  override def databaseMode: DatabaseMode = ???
  override def storageHasPropertyColocation: Boolean = ???
  override def storageSupportsFastExpandInto: Boolean = ???
  override def getLabelName(id: Int): String = ???
  override def getOptLabelId(labelName: String): Option[Int] = ???
  override def getLabelId(labelName: String): Int = ???
  override def getPropertyKeyName(id: Int): String = ???
  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = ???
  override def getPropertyKeyId(propertyKeyName: String): Int = ???
  override def getRelTypeName(id: Int): String = ???
  override def getOptRelTypeId(relType: String): Option[Int] = ???
  override def getRelTypeId(relType: String): Int = ???
  override def queryLanguage: QueryLanguage = ???

  override def functionSignatureInOtherVersion(name: FunctionName): Option[UserFunctionSignature] = ???
}
