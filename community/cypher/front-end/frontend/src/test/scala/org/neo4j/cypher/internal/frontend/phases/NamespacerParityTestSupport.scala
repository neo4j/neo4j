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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.NamespacerParityTestSupport.canonicalize
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Shared harness for asserting that the two Namespacer implementations
 * (`Namespacer.processOld` and `Namespacer.processNew`) produce equivalent
 * output for a given query.
 */
trait NamespacerParityTestSupport {
  self: CypherFunSuite with AstConstructionTestSupport with RewritePhaseTest =>

  /**
   * Runs both Namespacer paths on independent copies of `query` and asserts
   * that the rewritten statements are equivalent up to variable renaming,
   * and that the semantic tables have the same number of tracked expressions.
   */
  def assertParity(
    query: String,
    version: CypherVersion = CypherVersion.Cypher25
  ): Unit = {
    // The concrete test class is expected to override RewritePhaseTest#preProcessTransformer to
    // mirror NamespacerTest's pipeline (ScopeSurveyor, ExpandClauses, SemanticAnalysis, AstRewriting,
    // SemanticAnalysis). We just refresh the WorkingScope at the end so processNew has up-to-date
    // scope state for the post-rewrite AST.
    val prep: Transformer[BaseContext, BaseState, BaseState] = ScopeSurveyor

    val oldState = prepareFrom(version, query, prep)
    val newState = prepareFrom(version, query, prep)

    NamespacerParityTestSupport.advanceCounterPastAnonNames(oldState)
    NamespacerParityTestSupport.advanceCounterPastAnonNames(newState)

    val ctxOld = ContextHelper.create(version, query, phaseTestConfig.semanticFeatures, databaseReference)
    val ctxNew = ContextHelper.create(version, query, phaseTestConfig.semanticFeatures, databaseReference)

    val oldResult = Namespacer.processOld(oldState, ctxOld)
    val newResult = Namespacer.processNew(newState, ctxNew)

    // --- DEBUG: dump raw (pre-canonical) statements and named-variable lists --------------------
    if (NamespacerParityTestSupport.DebugDump) {
      println(s"\n========== parity debug for:\n  $query\n")
      println("--- OLD raw ---")
      println(prettifier.asString(oldResult.statement()))
      println("--- NEW raw ---")
      println(prettifier.asString(newResult.statement()))
      println("--- OLD anon-named variables (name, offset) ---")
      NamespacerParityTestSupport.dumpAnonVars(oldResult.statement()).foreach(println)
      println("--- NEW anon-named variables (name, offset) ---")
      NamespacerParityTestSupport.dumpAnonVars(newResult.statement()).foreach(println)
      println(s"========== end parity debug\n")
    }
    // --- /DEBUG --------------------------------------------------------------------------------

    val oldCanon = canonicalize(oldResult.statement())
    val newCanon = canonicalize(newResult.statement())

    val oldPretty = prettifier.asString(oldCanon)
    val newPretty = prettifier.asString(newCanon)

    withClue(
      s"""|CYPHER $version
          |Query:
          |  $query
          |
          |Old canonicalised:
          |$oldPretty
          |
          |New canonicalised:
          |$newPretty
          |
          |""".stripMargin
    ) {
      newPretty shouldEqual oldPretty
    }

    assertParitySemanticTable(oldResult.semanticTable(), newResult.semanticTable(), query, version)
  }

  def dumpSymbolGroupsAtNamespacer(
    query: String,
    version: CypherVersion = CypherVersion.Cypher25
  ): Unit = {
    val state = prepareFrom(version, query, ScopeSurveyor)
    NamespacerParityTestSupport.advanceCounterPastAnonNames(state)
    val ss = state.scopeState()
    val groups = ss.workingScope.getSymbolGroups(state.anonymousVariableNameGenerator)
    println(s"\n========== getSymbolGroups for: $query")
    println(s"Post-preprocess statement:")
    println(prettifier.asString(state.statement()))
    println(s"\n${groups.size} SymbolGroup(s):")
    groups.zipWithIndex.foreach { case (g, i) =>
      println(f"  [$i%2d]  decl=${g.declaration.value.name}%-14s @off=${g.declaration.value.position.offset}%3d  " +
        f"renaming=${g.renaming.getOrElse("<none>")}%-14s  uses=${g.uses.toSeq.map(u =>
            s"${u.value.name}@${u.value.position.offset}"
          ).mkString("[", ", ", "]")}")
    }
    println("==========")
  }

  protected def assertParitySemanticTable(
    oldTable: SemanticTable,
    newTable: SemanticTable,
    query: String,
    version: CypherVersion
  ): Unit = {
    withClue(s"CYPHER $version\nQuery: $query\nSemantic table size mismatch.\n") {
      newTable.types.size shouldEqual oldTable.types.size
    }
  }
}

trait NamespacerTestWithCanonicalization { self: RewritePhaseTest =>

  override protected def canonicalizeForComparison(stmt: Statement): Statement =
    NamespacerParityTestSupport.canonicalize(stmt)
}

object NamespacerParityTestSupport {

  /** Flip to true to dump pre-canonicalisation statements and anon-var lists when debugging. */
  val DebugDump: Boolean = false

  private val UnnamedAnonId = """ {2}UNNAMED(-?\d+)""".r

  /**
   * Walk the AST for `  UNNAMED<N>` names and advance the state's shared anon-name counter
   * past the highest observed N. Mirrors production, where the counter is never reset between
   * NameAllPatternElements and Namespacer.
   */
  def advanceCounterPastAnonNames(state: BaseState): Unit = {
    val ids: Seq[Int] = state.statement().folder.treeCollect {
      case v: LogicalVariable =>
        v.name match {
          case UnnamedAnonId(n) => n.toInt
          case _                => Int.MinValue
        }
    }.filter(_ != Int.MinValue)
    if (ids.nonEmpty) {
      val maxId = ids.max
      val gen = state.anonymousVariableNameGenerator
      // nextName consumes counter <n> and increments by 1; advance until the next mint is > maxId.
      while ({
        val peek = gen.nextName
        peek match {
          case UnnamedAnonId(n) => n.toInt <= maxId
          case _                => false
        }
      }) ()
    }
  }

  /** Dump every variable whose name starts with the anon-generator prefix ("  "), sorted by offset. */
  def dumpAnonVars(stmt: Statement): Seq[String] = {
    val primary: Seq[(String, Int, String)] = stmt.folder.treeCollect {
      case v: LogicalVariable if v.name.startsWith("  ") => (v.name, v.position.offset, "ast")
    }
    val extras: Seq[(String, Int, String)] = stmt.folder.treeCollect[Seq[(String, Int, String)]] {
      case e: ExpressionWithComputedDependencies =>
        (e.introducedVariables.iterator.collect {
          case v: LogicalVariable if v.name.startsWith("  ") => (v.name, v.position.offset, "ewcd-intro")
        } ++ e.scopeDependencies.iterator.collect {
          case v: LogicalVariable if v.name.startsWith("  ") => (v.name, v.position.offset, "ewcd-scope")
        }).toSeq
    }.flatten
    (primary ++ extras)
      .sortBy { case (n, o, t) => (o, n, t) }
      .map { case (n, o, t) => f"  off=$o%3d  [$t%-11s]  $n" }
  }

  private val CanonPrefix = "__canon@"

  /**
   * Rename every generator-produced variable (name starts with two spaces) to
   * a stable `__canon@N` name, where N is the variable's rank by first
   * appearance when walking the AST structurally (product-iteration order).
   *
   * Structural order — rather than raw position offsets — keeps canonicalisation
   * stable across parser-built ASTs and hand-constructed test expectations whose
   * positions are arbitrary.
   *
   * The rewriter also rebuilds `ExpressionWithComputedDependencies`
   * `introducedVariables` / `scopeDependencies` so those collections are
   * renamed in lockstep with regular `Variable` occurrences.
   */
  def canonicalize(stmt: Statement): Statement = {
    val primary: Seq[String] = stmt.folder.treeCollect {
      case v: LogicalVariable if v.name.startsWith("  ") => v.name
    }
    val extras: Seq[String] = stmt.folder.treeCollect[Seq[String]] {
      case e: ExpressionWithComputedDependencies =>
        (e.introducedVariables ++ e.scopeDependencies).iterator.collect {
          case v: LogicalVariable if v.name.startsWith("  ") => v.name
        }.toSeq
    }.flatten

    val ordered = scala.collection.mutable.LinkedHashSet[String]()
    (primary ++ extras).foreach(ordered += _)

    val mapping: Map[String, String] =
      ordered.toSeq.zipWithIndex.map { case (name, i) => name -> s"$CanonPrefix$i" }.toMap

    def rename(v: LogicalVariable): LogicalVariable =
      mapping.get(v.name).map(v.renameId).getOrElse(v)

    stmt.endoRewrite(bottomUp(Rewriter.lift {
      case v: Variable => rename(v)
      case e: ExpressionWithComputedDependencies =>
        val newIntroduced = e.introducedVariables.map(rename)
        val newScopeDeps = e.scopeDependencies.map(rename)
        e.withComputedIntroducedVariables(newIntroduced).withComputedScopeDependencies(newScopeDeps)
    }))
  }
}
