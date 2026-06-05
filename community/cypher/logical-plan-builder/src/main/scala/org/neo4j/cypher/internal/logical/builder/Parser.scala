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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AndsReorderable
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentAsc
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentDesc
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.parser.v25.ast.factory.Cypher25AstParser
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstParser
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RemoveSyntaxTracking
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.topDown

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Parser {

  protected def astParser: Parser.AstParser

  private def cleanup[T <: ASTNode](in: T): T = in.endoRewrite(Parser.Cleanup.cleanupRewriter)

  def parseProjections(projections: String*): Map[String, Expression] = {
    projections.map {
      case Parser.RegExps.asRegex(expString, VariableParser(alias)) => (alias, parseExpression(expString))
      case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as a projection")
    }.toMap
  }

  def parseAggregationProjections(projections: String*): Map[String, Expression] = projections.map {
    case Parser.RegExps.asRegex(expString, VariableParser(alias)) => (alias, parseAggregation(expString))
    case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as an aggregation projection")
  }.toMap

  def parseExpression(text: String): Expression = Try(astParser.expression(text)) match {
    case Success(expression) => cleanup(expression)
    case Failure(exception)  => throw new RuntimeException(s"Failed parsing expression `$text``", exception)
  }

  def parsePatternElement(text: String): PatternElement = Try(astParser.patternElement(text)) match {
    case Success(patternElement) => cleanup(patternElement)
    case Failure(exception)      => throw new RuntimeException(s"Failed parsing pattern element `$text``", exception)
  }

  def parseProcedureCall(text: String): UnresolvedCall = astParser.callClause(s"CALL $text") match {
    case u: UnresolvedCall => cleanup(u)
    case c                 => throw new IllegalArgumentException(s"Expected UnresolvedCall but got: $c")
  }

  def parseAggregation(cypher: String): Expression = cypher match {
    case Parser.RegExps.aggregationRegex(expString, direction) => parseExpression(expString) match {
        case f: FunctionInvocation =>
          val order =
            if ("ASC".equalsIgnoreCase(direction)) ArgumentAsc
            else if ("DESC".equalsIgnoreCase(direction)) ArgumentDesc
            else ArgumentUnordered
          f.withOrder(order)
        case e: Expression => e
      }
    case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as an aggregation expression")
  }

  def parseSort(text: Seq[String]): Seq[ColumnOrder] = text.map(parseSort)

  def parseSort(text: String): ColumnOrder = text match {
    case Parser.RegExps.sortRegex(VariableParser(variable), direction) =>
      if ("ASC".equalsIgnoreCase(direction)) Ascending(varFor(variable))
      else if ("DESC".equalsIgnoreCase(direction)) Descending(varFor(variable))
      else throw new IllegalArgumentException(s"Invalid direction $direction")
    case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as a sort item")
  }

  def parseGraphReference(text: String): GraphReference = astParser.useClause(s"USE $text").graphReference

  def parseSortItem(text: String): SortItem = astParser.sortItem(text)

  def parseDisjointByParameters(text: String): InTransactionsDisjointByParameters =
    cleanup(astParser.disjointByParameters(s"DISJOINT BY $text"))
}

object Parser {

  def apply(language: CypherVersion): Parser = language match {
    case CypherVersion.Cypher5  => Cypher5
    case CypherVersion.Cypher25 => Cypher25
  }

  val Latest: Parser = apply(CypherVersion.Legacy.legacyVersion())

  trait AstParser {
    def expression(cypher: String): Expression
    def patternElement(cypher: String): PatternElement
    def callClause(cypher: String): ASTNode
    def useClause(cypher: String): UseGraph
    def sortItem(cypher: String): SortItem

    // DISJOINT BY exists only in the Cypher 25 grammar, so always parse it with a Cypher 25
    // parser regardless of the builder's nominal language (this is version-agnostic test tooling).
    def disjointByParameters(cypher: String): InTransactionsDisjointByParameters =
      AstParserFactory(CypherVersion.Cypher25)(cypher, Neo4jCypherExceptionFactory(cypher, None), None, Seq())
        .asInstanceOf[Cypher25AstParser]
        .parse(_.subqueryInTransactionsDisjointByParameters())
  }

  private object Cypher5 extends Parser {

    override val astParser: AstParser = new AstParser {
      override def expression(cypher: String): Expression = parser(cypher).expression()
      override def patternElement(cypher: String): PatternElement = parser(cypher).parse(_.patternElement())
      override def callClause(cypher: String): ASTNode = parser(cypher).parse(_.callClause())
      override def useClause(cypher: String): UseGraph = parser(cypher).parse(_.useClause())
      override def sortItem(cypher: String): SortItem = parser(cypher).parse(_.orderItem())

      private def parser(cypher: String) =
        AstParserFactory(CypherVersion.Cypher5)(
          cypher,
          Neo4jCypherExceptionFactory(cypher, None),
          None,
          Seq()
        )
          .asInstanceOf[Cypher5AstParser]
    }
  }

  private object Cypher25 extends Parser {

    override val astParser: AstParser = new AstParser {
      override def expression(cypher: String): Expression = parser(cypher).expression()
      override def patternElement(cypher: String): PatternElement = parser(cypher).parse(_.patternElement())
      override def callClause(cypher: String): ASTNode = parser(cypher).parse(_.callClause())
      override def useClause(cypher: String): UseGraph = parser(cypher).parse(_.useClause())
      override def sortItem(cypher: String): SortItem = parser(cypher).parse(_.orderItem())

      private def parser(cypher: String) =
        AstParserFactory(CypherVersion.Cypher25)(
          cypher,
          Neo4jCypherExceptionFactory(cypher, None),
          None,
          Seq()
        )
          .asInstanceOf[Cypher25AstParser]
    }
  }

  private object RegExps {
    val asRegex = s"(.+) [Aa][Ss] (.+)".r
    val aggregationRegex = "(.+?)(?i)(ASC|DESC)?".r
    val sortRegex = "(.+) (?i)(ASC|DESC)".r
  }

  private object Cleanup {

    private val injectCachedProperties: Rewriter = topDown(Rewriter.lift {
      case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
        if name == "cache" || name == "cacheN" =>
        CachedProperty(v, v, pkn, NODE_TYPE)(AbstractLogicalPlanBuilder.pos)
      case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
        if name == "cacheFromStore" || name == "cacheNFromStore" =>
        CachedProperty(v, v, pkn, NODE_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)
      case ContainerIndex(Variable("cacheR"), Property(v: Variable, pkn: PropertyKeyName)) =>
        CachedProperty(v, v, pkn, RELATIONSHIP_TYPE)(AbstractLogicalPlanBuilder.pos)
      case ContainerIndex(Variable("cacheRFromStore"), Property(v: Variable, pkn: PropertyKeyName)) =>
        CachedProperty(v, v, pkn, RELATIONSHIP_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)
      case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
        if name == "cacheNHasProperty" =>
        CachedHasProperty(v, v, pkn, NODE_TYPE)(AbstractLogicalPlanBuilder.pos)
      case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
        if name == "cacheRHasProperty" =>
        CachedHasProperty(v, v, pkn, RELATIONSHIP_TYPE)(AbstractLogicalPlanBuilder.pos)
      case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
        if name == "cacheNHasPropertyFromStore" =>
        CachedHasProperty(v, v, pkn, NODE_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)
      case ContainerIndex(Variable(name), Property(v: Variable, pkn: PropertyKeyName))
        if name == "cacheRHasPropertyFromStore" =>
        CachedHasProperty(v, v, pkn, RELATIONSHIP_TYPE, knownToAccessStore = true)(AbstractLogicalPlanBuilder.pos)

    })

    private val invalidateInputPositions: Rewriter = topDown(Rewriter.lift {
      // Special handling of PrefixedPatternPart because it happens to not include an argument for InputPosition.
      // If more cases ends up being added this should probably be refactored. But that is left as an exercise to the reader.
      case x: PrefixedPatternPart => x
      case a: ASTNode             => a.dup(a.treeChildren.toSeq :+ AbstractLogicalPlanBuilder.pos)
    })

    private val replaceWrongFunctionInvocation: Rewriter = topDown(Rewriter.lift {
      case FunctionInvocation(FunctionName(Namespace(List()), "CoerceToPredicate"), _, Seq(expression), _, _, _, _) =>
        CoerceToPredicate(expression)
      case FunctionInvocation(
          FunctionName(Namespace(List()), "isRepeatTrailUnique"),
          _,
          Seq(variable: Variable),
          _,
          _,
          _,
          _
        ) =>
        IsRepeatTrailUnique(variable)(InputPosition.NONE)
      case FunctionInvocation(FunctionName(Namespace(List()), "andsReorderable"), _, Seq(expressions), _, _, _, _) =>
        def collectConjuncts(expr: Expression): ListSet[Expression] = expr match {
          case And(lhs, rhs) => collectConjuncts(lhs) ++ collectConjuncts(rhs)
          case Ands(exprs)   => exprs
          case other         => ListSet(other)
        }
        AndsReorderable(collectConjuncts(expressions))(InputPosition.NONE)
    })

    private val replaceRuntimeConstant: Rewriter = topDown(Rewriter.lift {
      case FunctionInvocation(
          FunctionName(Namespace(List()), "RuntimeConstant"),
          _,
          Seq(variable: Variable, inner),
          _,
          _,
          _,
          _
        ) =>
        RuntimeConstant(variable, inner)
    })

    val cleanupRewriter = inSequence(
      RemoveSyntaxTracking.instance,
      injectCachedProperties,
      invalidateInputPositions,
      replaceWrongFunctionInvocation,
      replaceRuntimeConstant,
      LabelExpressionPredicateNormalizer.instance,
      // Flattening boolean operators otherwise it is impossible to create instances of Ands / Ors
      flattenBooleanOperators.instance(CancellationChecker.NeverCancelled)
    )
  }
}
