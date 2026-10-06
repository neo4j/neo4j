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

import org.neo4j.cypher.internal.ast.Search.PatternComplexity
import org.neo4j.cypher.internal.ast.Search.SearchIndexType
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.*
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Conjunction
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.VectorFilterExpression
import org.neo4j.cypher.internal.expressions.VectorFilterExpression.InSet
import org.neo4j.cypher.internal.expressions.VectorFilterExpression.VectorFilterExpressionRange
import org.neo4j.cypher.internal.notification.IdentifierShadowsVariableNotification
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable.IteratorEq
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CTVector
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec

object Search {

  sealed trait SearchIndexType {
    def name: String
    def expectedEmbeddingType: TypeSpec

    def requireVectorIndex(clause: String, pos: InputPosition): SemanticCheck
    def requireFulltextIndex(clause: String, pos: InputPosition): SemanticCheck
  }

  case object Vector extends SearchIndexType {
    override val name = "VECTOR INDEX"
    override val expectedEmbeddingType: TypeSpec = CTVector.union(CTList(CTNumber).covariant)

    override def requireVectorIndex(clause: String, pos: InputPosition): SemanticCheck = SemanticCheck.success

    override def requireFulltextIndex(clause: String, pos: InputPosition): SemanticCheck =
      SemanticCheck.error(SemanticError.invalidClauseCombination(clause, name, pos))
  }

  case object Fulltext extends SearchIndexType {
    override val name = "FULLTEXT INDEX"
    override val expectedEmbeddingType: TypeSpec = CTString

    override def requireVectorIndex(clause: String, pos: InputPosition): SemanticCheck =
      SemanticCheck.error(SemanticError.invalidClauseCombination(clause, name, pos))

    override def requireFulltextIndex(clause: String, pos: InputPosition): SemanticCheck = SemanticCheck.success
  }

  enum PatternComplexity {
    case Simple
    case Complex(reason: PatternComplexity.Reason, position: InputPosition)

    def isSimple: Boolean = this == Simple
  }

  object PatternComplexity {
    def default: PatternComplexity = Simple

    enum Reason {
      case InvalidPredicate
      case MultipleBoundVariables
      case MultiplePatternParts
      case TooComplexMatch
    }

    /**
     * Classify the pattern from the MATCH statement.
     */
    def from(pattern: Pattern.ForMatch, bindingVariable: LogicalVariable): PatternComplexity = {
      val patternParts = pattern.patternParts
      val patternVariables = patternParts.flatMap(p => p.allVariables).toSet
      val patternPart = patternParts.head.part

      if (patternParts.size > 1) {
        Complex(Reason.MultiplePatternParts, patternParts(1).position)
      } else if (!patternPart.isInstanceOf[NamedPatternPart] && patternVariables.size > 1) {
        val firstUnrelatedVar = patternVariables.filterNot(v => v == bindingVariable).head
        Complex(Reason.MultipleBoundVariables, firstUnrelatedVar.position)
      } else if (patternPart.isInstanceOf[NamedPatternPart] && patternVariables.size > 2) {
        val firstUnrelatedVar = patternVariables.filterNot(v =>
          v == bindingVariable || v == patternPart.asInstanceOf[NamedPatternPart].variable
        ).head
        Complex(Reason.MultipleBoundVariables, firstUnrelatedVar.position)
      } else {
        checkPatternElement(patternPart.element)
      }
    }

    private def checkPatternElement(element: PatternElement): PatternComplexity = {
      element match {
        case n: NodePattern => checkNodePattern(n)
        case relChain @ RelationshipChain(_: RelationshipChain, _, _) =>
          Complex(Reason.TooComplexMatch, relChain.position)
        case RelationshipChain(p, r, n) =>
          Seq(
            checkPatternElement(p),
            checkRelationshipPattern(r),
            checkNodePattern(n)
          ).dropWhile(_.isSimple)
            .headOption
            .getOrElse(Simple)
        case _ => Complex(Reason.TooComplexMatch, element.position)
      }
    }

    private def checkNodePattern(nodePattern: NodePattern): PatternComplexity = {
      nodePattern match {
        case NodePattern(None, Some(pred), _, _) =>
          Complex(Reason.InvalidPredicate, pred.position)
        case NodePattern(None, _, Some(pred), _) =>
          Complex(Reason.InvalidPredicate, pred.position)
        case NodePattern(None, _, _, Some(pred)) =>
          Complex(Reason.InvalidPredicate, pred.position)
        case _ => Simple
      }
    }

    private def checkRelationshipPattern(relationshipPattern: RelationshipPattern): PatternComplexity = {
      relationshipPattern match {
        case RelationshipPattern(None, Some(pred), _, _, _, _) =>
          Complex(Reason.InvalidPredicate, pred.position)
        case RelationshipPattern(None, _, _, Some(pred), _, _) =>
          Complex(Reason.InvalidPredicate, pred.position)
        case RelationshipPattern(None, _, _, _, Some(pred), _) =>
          Complex(Reason.InvalidPredicate, pred.position)
        // Variable length relationship
        case r @ RelationshipPattern(None, _, Some(_), _, _, _) =>
          Complex(Reason.TooComplexMatch, r.position)
        case _ => Simple
      }
    }
  }

  opaque type CheckedPatternComplexity = Search

  private def withCheckedPatternComplexity(
    search: Search,
    pattern: Pattern.ForMatch
  ): Search & CheckedPatternComplexity = {
    search.copy()(
      position = search.position,
      patternComplexity = PatternComplexity.from(pattern, search.bindingVariable)
    )
  }
}

case class Search(
  bindingVariable: LogicalVariable,
  score: Option[LogicalVariable],
  indexType: SearchIndexType,
  indexName: Expression,
  embedding: Expression,
  where: Option[Where],
  analyzer: Option[Expression],
  skip: Option[Skip],
  limit: Limit
)(val position: InputPosition, val patternComplexity: Search.PatternComplexity)
    extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {

  def semanticCheck: SemanticCheck = {
    checkBindingVariable() chain
      checkScore() chain
      checkIndexName() chain
      checkEmbedding() chain
      checkLimit() chain
      checkWhere() chain
      checkSkip() chain
      checkAnalyzer()
  }

  private def checkIndexName(): SemanticCheck = {
    indexName match {
      case name: StringLiteral =>
        notifyIfIndexNameShadowsVariable(name.value, indexName.position)
      case parameter: Parameter if parameter.parameterType == CTString =>
        // This is a restriction for the MVP which we intend to lift later
        // TODO: Once SEARCH can handle parameters, re-enable the auto-parametrization of the Search clause
        SemanticError.invalidIndexParameter(indexType.name, parameter.position)
      case exp =>
        // We only parse the index name as an identifier (saved as StringLiteral) or string Parameter
        // This is the same exception as for create index for this case
        SemanticCheck.error(SemanticError.invalidEntityType(
          ExpressionStringifier()(exp),
          "index name",
          Seq("STRING NOT NULL"),
          "index name must be a String, or a String parameter.",
          exp.position
        ))
    }
  }

  private def notifyIfIndexNameShadowsVariable(
    name: String,
    position: InputPosition
  ): SemanticState => Either[SemanticError, SemanticState] = {
    (s: SemanticState) =>
      s.symbol(name) match {
        case None => Right(s)
        case Some(_) =>
          Right(s.addNotification(IdentifierShadowsVariableNotification(
            position,
            name,
            indexType.name
          )))
      }
  }

  private def checkEmbedding(): SemanticCheck = {
    SemanticExpressionCheck.simple(embedding) chain
      expectType(indexType.expectedEmbeddingType, embedding)
  }

  private def checkSkip(): SemanticCheck = {
    skip.foldSemanticCheck(s =>
      indexType.requireFulltextIndex(s.name, s.position) chain
        s.semanticCheckWithUpperBound(Int.MaxValue)
    )
  }

  private def checkLimit(): SemanticCheck = limit.semanticCheckWithUpperBound(Int.MaxValue)

  private def checkBindingVariable(): SemanticCheck =
    SemanticExpressionCheck.check(Expression.SemanticContext.Simple, bindingVariable) chain
      expectType(CTNode.covariant | CTRelationship.covariant, bindingVariable)

  private def checkScore(): SemanticCheck = {
    if (score.isDefined) {
      declareVariable(score.get, CTFloat)
    } else {
      SemanticCheck.success
    }
  }

  private def checkWhere(): SemanticCheck = {
    SemanticCheck.fromState { state =>
      /*
       * It is only possible to verify the restriction on the WHERE clause in the first pass of semantic checking.
       * Later, rewriters will have modified the predicates e.g. by turning m.prop = 5 to m.prop IN [$`  AUTOINT1`].
       * This also means that type checking for e.g. unresolved functions like date() needs to happen at runtime.
       */
      where.foldSemanticCheck(wh =>
        indexType.requireVectorIndex("WHERE", wh.position) chain
          wh.semanticCheck ifOkChain
          when(!state.semanticCheckHasRunOnce) { checkExpressionsRangeOrExact(wh.expression) }
      )
    }
  }

  private def checkAnalyzer(): SemanticCheck = {
    analyzer.foldSemanticCheck(expr =>
      indexType.requireFulltextIndex("WITH ANALYZER", expr.position) chain
        SemanticExpressionCheck.simple(expr) chain
        SemanticExpressionCheck.expectType(CTString, expr)
    )
  }

  private def checkExpressionsRangeOrExact(expression: Expression): SemanticCheck = {
    val CheckedVectorFilterExpression(check, expressions) = asFilterExpressions(expression)
    check ifOkChain {
      val expressionsByProperty = expressions.groupBy(_.propertyName).values
      // TODO: Improve error reporting to show offending expressions (see SURF-489)
      val invalid = expressionsByProperty.exists {
        case Seq(_)                            => false
        case VectorFilterExpressionRange(_, _) => false
        case _                                 => true
      }
      when(invalid) {
        SemanticError.singleStageWithInvalidPredicate(expression, expression.position)
      }
    }
  }

  private case class CheckedVectorFilterExpression(
    check: SemanticCheck,
    expressions: Seq[VectorFilterExpression]
  ) {

    def ++(other: CheckedVectorFilterExpression): CheckedVectorFilterExpression =
      CheckedVectorFilterExpression(
        this.check chain other.check,
        this.expressions ++ other.expressions
      )
  }

  private val emptyCheckedVectorFilterExpression: LazyVal[CheckedVectorFilterExpression] =
    LazyVal(CheckedVectorFilterExpression(SemanticCheck.success, Seq.empty))

  private def checkedVectorFilterExpression(
    rhs: Expression,
    filterExpression: VectorFilterExpression
  ): CheckedVectorFilterExpression = {
    filterExpression match {
      case InSet(_, _) => CheckedVectorFilterExpression(
          expectType(CTList(CTAny).covariant, rhs),
          Seq(filterExpression)
        )
      case _ => CheckedVectorFilterExpression(
          checkRhs(rhs),
          Seq(filterExpression)
        )
    }
  }

  private def checkedVectorFilterExpression(error: SemanticError): CheckedVectorFilterExpression =
    CheckedVectorFilterExpression(error, Seq.empty)

  private def asFilterExpressions(expression: Expression): CheckedVectorFilterExpression =
    expression match {
      case VectorFilterExpression(_: LogicalVariable, rhs: Expression, operator: VectorFilterExpression) =>
        checkedVectorFilterExpression(rhs, operator)
      case Conjunction(conjuncts*) =>
        conjuncts.map(asFilterExpressions).foldLeft(emptyCheckedVectorFilterExpression.value)(_ ++ _)
      case expr =>
        checkedVectorFilterExpression(SemanticError.singleStageWithInvalidPredicate(expr, expr.position))
    }

  private def checkRhs(rhs: Expression): SemanticCheck =
    checkRhsType(rhs)

  private def checkRhsType(rhs: Expression): SemanticCheck = {
    val validTypes = CTInteger
      .union(CTFloat)
      .union(CTBoolean)
      .union(CTString)
      .union(CTDate)
      .union(CTDateTime)
      .union(CTLocalDateTime)
      .union(CTLocalTime)
      .union(CTTime)
      .union(CTDuration)

    SemanticExpressionCheck.simple(rhs) chain expectTypeWithoutCoercion(validTypes, rhs)
  }

  private def checkWhenComplexPatternNotAllowed(check: => SemanticCheck): SemanticCheck = {
    whenState(!_.features.contains(SemanticFeature.VectorSearchWithComplexPattern)) {
      check
    }
  }

  /*
   * Semantic checks that verify the pattern from the MATCH statement.
   * Most of the limitations here are expected to be lifted in later iterations of the feature
   */
  def patternChecks(pattern: Pattern.ForMatch): SemanticCheck = {
    // We only want to do these checks once, because later rewriters can have made the pattern more complex
    whenState(!_.semanticCheckHasRunOnce) {
      val patternParts = pattern.patternParts
      val selectors = patternParts.collect(p => p.selector).filterNot(s => s.isInstanceOf[AllPaths])

      import PatternComplexity.Reason.*

      ifOkChainAll(
        checkWhenComplexPatternNotAllowed {
          patternComplexity match {
            case PatternComplexity.Simple => SemanticCheck.success
            case PatternComplexity.Complex(reason, position) =>
              reason match {
                case MultiplePatternParts   => SemanticError.searchWithTooComplexMatch(position)
                case MultipleBoundVariables => SemanticError.searchWithMultipleBoundVariables(position)
                case TooComplexMatch        => SemanticCheck.success
                case InvalidPredicate       => SemanticCheck.success
              }
          }
        },
        when(selectors.nonEmpty) {
          SemanticError.searchWithTooComplexMatch(selectors.head.position)
        },
        checkWhenComplexPatternNotAllowed {
          patternComplexity match {
            case PatternComplexity.Simple => SemanticCheck.success
            case PatternComplexity.Complex(reason, position) =>
              reason match {
                case MultiplePatternParts   => SemanticCheck.success
                case MultipleBoundVariables => SemanticCheck.success
                case TooComplexMatch        => SemanticError.searchWithTooComplexMatch(position)
                case InvalidPredicate       => SemanticError.searchWithInvalidPredicates(position)
              }
          }
        }
      )
    }
  }

  override def dup(children: Seq[AnyRef]): this.type = {
    if (children.iterator eqElements this.treeChildren)
      this
    else {
      children match {
        case Seq(
            bindingVariable: LogicalVariable,
            score: Option[LogicalVariable @unchecked],
            indexType: SearchIndexType,
            indexName: Expression,
            embedding: Expression,
            where: Option[Where @unchecked],
            analyzer: Option[Expression @unchecked],
            skip: Option[Skip @unchecked],
            limit: Limit
          ) =>
          copy(
            bindingVariable = bindingVariable,
            score = score,
            indexType = indexType,
            indexName = indexName,
            embedding = embedding,
            where = where,
            analyzer = analyzer,
            skip = skip,
            limit = limit
          )(
            position = position,
            patternComplexity = patternComplexity
          ).asInstanceOf[this.type]
        case _ =>
          throw new IllegalStateException(
            s"Failed rewriting $this\nTried using children: ${children.mkString(",")}"
          )
      }
    }
  }

  def withCheckedPatternComplexity(pattern: Pattern.ForMatch): Search & Search.CheckedPatternComplexity = {
    Search.withCheckedPatternComplexity(this, pattern)
  }
}
