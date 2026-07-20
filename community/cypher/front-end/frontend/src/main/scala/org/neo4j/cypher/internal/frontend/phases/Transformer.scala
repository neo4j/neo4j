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

import org.neo4j.cypher.internal.ast.ASTAnnotationMap.PositionedNode
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.frontend.phases.Transformer.Debug.LogChangedFields
import org.neo4j.cypher.internal.frontend.phases.Transformer.Debug.LogLegacyScopeTree
import org.neo4j.cypher.internal.frontend.phases.Transformer.Debug.LogStatements
import org.neo4j.cypher.internal.frontend.phases.Transformer.Debug.LogStatementsAsQueries
import org.neo4j.cypher.internal.frontend.phases.Transformer.Debug.LogTransformerName
import org.neo4j.cypher.internal.frontend.phases.Transformer.Debug.LogWorkingScope
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.WorkingScopeStringRenderer
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.AstString
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.cypher.internal.util.inSequence

trait Transformer[-C <: BaseContext, -FROM, +TO] {
  def transform(from: FROM, context: C): TO

  def andThen[D <: C, TO2](other: Transformer[D, TO, TO2]): Transformer[D, FROM, TO2] =
    new PipeLine(this, other)

  def name: String

  def toStrings: Seq[String] = Seq(name)

  /**
   * @return the conditions that are guaranteed to be met after this step has run.
   */
  def postConditions: Set[StepSequencer.Condition]

  /**
   * @return the conditions that this step invalidates as a side-effect of its work.
   *         Must not intersect with postConditions.
   */
  def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  final protected[Transformer] def checkConditions(
    state: Any,
    conditions: Set[StepSequencer.Condition]
  )(cancellationChecker: CancellationChecker): Unit =
    Transformer.checkConditions(state, conditions, name)(cancellationChecker)

  final protected[Transformer] def printDebugInfo(
    fromState: Any,
    toState: Any
  ): Unit =
    if (Transformer.Debug.Enabled) Transformer.printDebugInfo(name, fromState, toState, toStrings.size <= 1)
}

object Transformer {
  val prettifier = Prettifier(ExpressionStringifier())

  /**
   * Applies `steps` to `statement` in order. Behaves exactly like
   * `statement.endoRewrite(inSequence(steps.map(_._2)))` (`inSequence` applies each rewriter to the
   * whole tree in turn). When [[Debug.LogInnerRewriters]] is enabled it instead applies the rewriters
   * one at a time and prints each step that changed the statement, which is the only way to see which
   * of the rewriters bundled into a single phase (e.g. ASTRewriting, PreparatoryRewriting) fired.
   */
  def applyRewritersInSequence(
    bundleName: String,
    statement: Statement,
    steps: Seq[(StepSequencer.Step, Rewriter)]
  ): Statement =
    if (Debug.LogInnerRewriters) {
      steps.foldLeft(statement) { case (stmt, (step, rewriter)) =>
        val next = stmt.endoRewrite(rewriter)
        if (next != stmt)
          println(s"######## DEBUG $bundleName/${step.getClass.getSimpleName.stripSuffix("$")} changed the statement")
        if (Debug.LogStatementsAsQueries) println(prettifier.asString(next))
        if (Debug.LogStatements) println(AstString.render(next))
        next
      }
    } else {
      statement.endoRewrite(inSequence(steps.map(_._2): _*))
    }

  object Debug {
    // Debug flags, requires that assertions are enabled to have effect (jvm option -ea)
    // Intellij Idea: You might need you to: Build -> Recompile 'Transformer.scala'
    final val LogTransformerName = false
    final val LogStatements = false
    final val LogStatementsAsQueries = false
    final val LogChangedFields = false
    final val LogWorkingScope = false
    final val LogLegacyScopeTree = false
    final val LogInnerRewriters = false

    final val Enabled =
      LogTransformerName || LogStatements || LogStatementsAsQueries || LogChangedFields || LogWorkingScope || LogLegacyScopeTree || LogInnerRewriters
  }

  /**
   * Transformer that can be inserted when debugging, to help detect
   * what part of the compilation introduces an ast issue.
   */
  def printAst(tag: String): Transformer[BaseContext, BaseState, BaseState] =
    new Transformer[BaseContext, BaseState, BaseState] {

      override def transform(from: BaseState, context: BaseContext): BaseState = {
        println("     |||||||| PRINT AST: " + tag)
        println(Prettifier(ExpressionStringifier()).asString(from.maybeStatement.get))
        from
      }

      override def postConditions: Set[StepSequencer.Condition] = Set.empty

      override def name: String = "print ast" + tag

      override def toStrings: Seq[String] = Seq(name)
    }

  def checkConditions(
    state: Any,
    conditions: Set[StepSequencer.Condition],
    name: String
  )(cancellationChecker: CancellationChecker): Unit = {
    val messages: Seq[String] = conditions.toSeq.collect {
      case v: ValidatingCondition => v(state)(cancellationChecker)
    }.flatten
    if (messages.nonEmpty) {
      val prefix = s"Conditions started failing after running these phases: $name\n"
      throw new IllegalStateException(prefix + messages.mkString(", "))
    }
  }

  def printDebugInfo(
    transformerName: String,
    fromState: Any,
    toState: Any,
    isSingleTransformer: Boolean
  ): Unit = {
    if (LogTransformerName) {
      println(s"######## DEBUG $transformerName executed")
    }

    if (LogChangedFields) {
      (fromState, toState) match {
        case (from: Product, to: Product) =>
          val changedFields = from.productIterator.zip(to.productIterator).zip(to.productElementNames)
            .collect { case ((fromVal, toVal), name) if fromVal != toVal => name }
          if (changedFields.nonEmpty) {
            println(s"######## DEBUG $transformerName, state changed in the following fields:")
            println(changedFields.mkString("", "\n", "\n"))
          }
        case _ =>
      }
    }

    if (LogStatementsAsQueries || LogStatements) {
      (fromState, toState) match {
        case (from: BaseState, to: BaseState)
          if to.maybeStatement.isDefined && from.maybeStatement != to.maybeStatement =>
          println(s"######## DEBUG $transformerName, statement changed:")
          if (LogStatementsAsQueries) println(prettifier.asString(to.statement()))
          if (LogStatements) println(AstString.render(to.statement()))
          println("\n")
        case _ =>
      }
    }

    if (LogWorkingScope) {
      (fromState, toState) match {
        case (from: BaseState, to: BaseState)
          if to.maybeScopeState.isDefined && from.maybeScopeState != to.maybeScopeState =>
          println(s"######## DEBUG $transformerName, working scope changed:")
          println(WorkingScopeStringRenderer(to.maybeScopeState.get.workingScope))
          println("\n")
        case _ =>
      }
    }

    if (LogLegacyScopeTree) {
      (fromState, toState) match {
        case (from: BaseState, to: BaseState)
          if to.maybeSemantics.isDefined &&
            from.maybeSemantics != to.maybeSemantics =>

          println(s"######## DEBUG $transformerName, legacy scope tree changed:")
          println(renderLegacyScopeTree(to.semantics()))
          println("\n")

        case _ =>
      }
    }
  }

  private def renderLegacyScopeTree(sem: SemanticState): String = {
    def scopePathOf(loc: SemanticState.ScopeZipper.Location): Vector[Int] = {
      @annotation.tailrec
      def loop(l: SemanticState.ScopeZipper.Location, acc: Vector[Int]): Vector[Int] =
        l.context match {
          case SemanticState.ScopeZipper.Top => acc
          case tc: SemanticState.ScopeZipper.TreeContext =>
            loop(tc.parent, (tc.left.size +: acc))
          case _ => acc
        }
      loop(loc, Vector.empty)
    }

    // ASTNode -> exact zipper location at time of recordCurrentScope
    val nodesByPath: Map[Vector[Int], Vector[ASTNode]] =
      sem.recordedScopes.iterator.foldLeft(Map.empty[Vector[Int], Vector[ASTNode]]) {
        case (acc, (PositionedNode(ast), scopeLoc)) =>
          val p = scopePathOf(scopeLoc.location)
          acc.updated(p, acc.getOrElse(p, Vector.empty) :+ ast)
      }

    val currentPath = scopePathOf(sem.currentScope.location)
    val rootScope: Scope = sem.scopeTree

    def whitespaceNormalization(cypher: String): String =
      cypher.trim.replaceAll("\\s+", " ")

    def renderAstString(astNode: ASTNode): String = whitespaceNormalization(astNode match {
      case s: Statement           => prettifier.asString(s)
      case c: Clause              => prettifier.asString(SingleQuery(Seq(c))(InputPosition.NONE))
      case gr: GraphReference     => prettifier.expr(gr)
      case s: Search              => prettifier.asString(s)
      case ex: Expression         => prettifier.expr(ex)
      case p: Pattern             => prettifier.expr.patterns(p)
      case p: PatternPart         => prettifier.expr.patterns(p)
      case p: PatternElement      => prettifier.expr.patterns(p)
      case p: RelationshipPattern => prettifier.expr.patterns(p)
      case lex: LabelExpression   => prettifier.expr.stringifyLabelExpression(lex)
      case cqb @ ConditionalQueryBranch(Some(_), _) =>
        prettifier.asString(ConditionalQueryWhen(Seq(cqb), None)(InputPosition.NONE))
      case cqb @ ConditionalQueryBranch(None, _) =>
        prettifier.asString(ConditionalQueryWhen(Seq(), Some(cqb))(InputPosition.NONE))
      case x => x.toString
    })

    val out = new StringBuilder

    def render(scope: Scope, path: Vector[Int], prefix: String, isLast: Boolean): Unit = {
      val branch = if (isLast) "└─" else "├─"
      val nextPrefix = prefix + (if (isLast) "  " else "│ ")

      val symbols =
        if (scope.symbolNames.isEmpty) ""
        else scope.symbolNames.toSeq.sorted.mkString(" [", ", ", "]")

      val currentMark = if (path == currentPath) "  <== current" else ""
      val pathStr = if (path.isEmpty) "root" else path.mkString(".")

      out.append(s"$prefix$branch($pathStr)$symbols$currentMark\n")

      nodesByPath.get(path).foreach { nodes =>
        nodes.sortBy(_.position.offset).foreach { n =>
          out.append(s"$nextPrefix   @${n.position.offset} ${n.getClass.getSimpleName}: ${renderAstString(n)}\n")
        }
      }

      val kids = scope.children.toVector
      kids.zipWithIndex.foreach { case (ch, i) =>
        val last = i == kids.size - 1
        render(ch, path :+ i, nextPrefix, last)
      }
    }

    // Print root plainly, then children with proper prefix
    out.append("(root)\n")
    rootScope.children.toVector.zipWithIndex.foreach { case (ch, i) =>
      render(ch, Vector(i), prefix = "", isLast = i == rootScope.children.size - 1)
    }

    out.toString()
  }
}

class PipeLine[-C <: BaseContext, FROM, MID, TO](first: Transformer[C, FROM, MID], after: Transformer[C, MID, TO])
    extends Transformer[C, FROM, TO] {

  private val lazyPostConditions: LazyVal[Set[StepSequencer.Condition]] = LazyVal {
    first.postConditions ++
      after.postConditions --
      after.invalidatedConditions
  }

  private val lazyInvalidatedConditions: LazyVal[Set[StepSequencer.Condition]] = LazyVal {
    after.invalidatedConditions ++
      first.invalidatedConditions --
      after.postConditions
  }

  override def postConditions: Set[StepSequencer.Condition] = lazyPostConditions.value

  override def invalidatedConditions: Set[StepSequencer.Condition] = lazyInvalidatedConditions.value

  override def transform(from: FROM, context: C): TO = {
    val step1 = first.transform(from, context)
    val step2 = after.transform(step1, context)

    // We do not need to check `after.postConditions`, since executing
    // `after.transform` is already doing that.
    val conditionsToCheck = first.postConditions -- after.invalidatedConditions

    // Checking conditions inside assert so they are not run in production
    if (AssertionRunner.ASSERTIONS_ENABLED) {
      checkConditions(step2, conditionsToCheck)(context.cancellationChecker)
    }

    step2
  }

  override def name: String = first.name + ", " + after.name

  override def toString: String = name

  override def toStrings: Seq[String] = first.toStrings ++ after.toStrings
}

final class If[-C <: BaseContext, FROM, STATE <: FROM](val f: STATE => Boolean)(thenT: => Transformer[C, FROM, STATE])
    extends Transformer[C, STATE, STATE] {

  override def transform(from: STATE, context: C): STATE = {
    if (f(from))
      thenT.transform(from, context)
    else
      from
  }

  override def name: String = s"if(<f>) ${thenT.name}"

  override def toString: String = name

  // We cannot guarantee the postConditions of thenT, if it is never run.
  // Also we cannot check `f(state)` to determine if we should run the post-condition
  // (in a ConditionalValidatingCondition wrapper), since `state` might have changed.
  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = thenT.invalidatedConditions
}

object If {

  def apply[C <: BaseContext, FROM, STATE <: FROM](f: STATE => Boolean)(thenT: => Transformer[C, FROM, STATE])
    : If[C, FROM, STATE] =
    new If[C, FROM, STATE](f)(thenT)
}

final class IfElse[-C <: BaseContext, FROM, TO](val f: FROM => Boolean)(
  thenT: => Transformer[C, FROM, TO],
  val elseT: Transformer[C, FROM, TO]
) extends Transformer[C, FROM, TO] {

  override def transform(from: FROM, context: C): TO = {
    if (f(from))
      thenT.transform(from, context)
    else
      elseT.transform(from, context)
  }

  override def name: String = s"if(<f>) ${thenT.name} else ${elseT.name}"

  override def toString: String = name

  // We cannot guarantee the postConditions of thenT, if it is never run.
  // Also we cannot check `f(state)` to determine if we should run the post-condition
  // (in a ConditionalValidatingCondition wrapper), since `state` might have changed.
  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = thenT.invalidatedConditions
}

object IfElse {

  def apply[C <: BaseContext, FROM, TO](f: FROM => Boolean)(
    thenT: => Transformer[C, FROM, TO],
    elseT: Transformer[C, FROM, TO]
  ): IfElse[C, FROM, TO] =
    new IfElse[C, FROM, TO](f)(thenT, elseT)
}

final class IfContext[
  -C <: BaseContext,
  FROM,
  STATE <: FROM
](val f: C => Boolean)(thenT: => Transformer[C, FROM, STATE])
    extends Transformer[C, STATE, STATE] {

  override def transform(from: STATE, context: C): STATE = {
    if (f(context))
      thenT.transform(from, context)
    else
      from
  }

  override def name: String = s"if(<f>) ${thenT.name}"

  override def toString: String = name

  // We cannot guarantee the postConditions of thenT, if it is never run.
  // Also we cannot check `f(state)` to determine if we should run the post-condition
  // (in a ConditionalValidatingCondition wrapper), since `state` might have changed.
  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = thenT.invalidatedConditions
}

object IfContext {

  def apply[C <: BaseContext, FROM, STATE <: FROM](f: C => Boolean)(thenT: => Transformer[C, FROM, STATE])
    : IfContext[C, FROM, STATE] =
    new IfContext[C, FROM, STATE](f)(thenT)
}

final class IfChanged[-C <: BaseContext, FROM, STATE <: FROM](val f: (STATE, STATE) => Boolean)(
  t: => Transformer[C, FROM, STATE]
)(u: => Transformer[C, STATE, STATE])
    extends Transformer[C, STATE, STATE] {

  override def transform(from: STATE, context: C): STATE = {
    val to = t.transform(from, context)

    if (f(from, to))
      u.transform(to, context)
    else
      to
  }

  override def name: String = s"ifChange(<f>, <u>) ${t.name}"

  override def toString: String = name

  override def postConditions: Set[StepSequencer.Condition] = t.postConditions

  override def invalidatedConditions: Set[StepSequencer.Condition] = t.invalidatedConditions
}

object IfChanged {

  def apply[C <: BaseContext, FROM, STATE <: FROM](f: (STATE, STATE) => Boolean)(
    t: => Transformer[C, FROM, STATE]
  )(u: => Transformer[C, STATE, STATE]): IfChanged[C, FROM, STATE] =
    new IfChanged[C, FROM, STATE](f)(t)(u)
}

case class NoOp[-C <: BaseContext, FROM, STATE <: FROM]() extends Transformer[C, STATE, STATE] {

  override def transform(from: STATE, context: C): STATE = from

  override def name: String = s"NoOp"

  override def toString: String = name

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty
}

case class OptionalTransformer[
  -C <: BaseContext,
  FROM,
  STATE <: FROM
](transformer: Option[Transformer[C, STATE, STATE]]) extends Transformer[C, STATE, STATE] {
  val actual: Transformer[C, STATE, STATE] = transformer.getOrElse(NoOp())

  override def transform(from: STATE, context: C): STATE = actual.transform(from, context)

  override def name: String = actual.name

  override def toString: String = actual.toString

  override def toStrings: Seq[String] = transformer.map(_.toStrings).getOrElse(Seq.empty)

  override def postConditions: Set[StepSequencer.Condition] = actual.postConditions

  override def invalidatedConditions: Set[StepSequencer.Condition] = actual.invalidatedConditions
}
