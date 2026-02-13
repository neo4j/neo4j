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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.ASTAnnotationMap.PositionedNode
import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UpdateClause
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.DisableReworkedRewriters
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.ast.semantics.scoping.Result
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.containsAggregate
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Range
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.IsolateSubqueriesInMutatingPatterns.SubqueriesInMutatingPatternsIsolated
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoExpandableClauses
import org.neo4j.cypher.internal.rewriting.conditions.ProjectionClausesHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

import scala.annotation.tailrec

/**
 * ExpandClauses rewrites ASTs that does not have a representation that is supported by planner and runtime.
 * The AST, NEXT, WHEN, Top Level Braces and * are rewritten into known ASTs such as UNIONs and SingleQueries.
 *
 * The expansions follow a structures like this:
 *
 * Preface - collect and alias incoming
 * Nested [
 *   Ingress - at first row by row consumer start - Clauses: Use Unwind De-anonymizer
 *   Query - expanded
 * ]
 * Postface - deanonymize - Anonymizing
 *
 * A Next statement is expanded using primarily WITH and CALL subqueries. Observe that statements
 * that require wrapping in CALL have their return variables anonymized and moved to outside
 * the call to not cause scope issues. Anonymizing the returns also allows for returning unaliased
 * expressions in the last statement. To emulate the by-table semantics of NEXT the incoming rows are
 * collected and unwound where relevant.
 *
 * A When query is rewritten into a chain of UNION queries with a controlling WHERE ensuring that only one
 * branch of the UNION is excuted according to the predicate.
 *
 * QUERY
 *    WHEN $param < 1 THEN RETURN 1 AS res
 *    WHEN $param > 1 THEN RETURN 2 AS res
 *    ELSE RETURN 3 AS res
 *
 * REWRITE
 *    WITH *, CASE WHEN $param < 1 THEN 0 WHEN $param > 1 THEN 1 ELSE 2 END AS `  UNNAMED0`
 *    CALL (*) {
 *       // WHEN #1
 *       WITH * WHERE `  UNNAMED0` = 0
 *       // THEN #1
 *       RETURN 1 AS res
 *       UNION ALL
 *       // WHEN #2
 *       WITH * WHERE `  UNNAMED0` = 1
 *       // THEN #2
 *       RETURN 2 AS res
 *       UNION ALL
 *       WITH * WHERE `  UNNAMED0` = 2
 *       // ELSE
 *       RETURN 3 AS res
 *    }
 *    RETURN res
 *
 * Top Level Braces is in many cases removed and in some context replaced by nesting the inner query in a
 * call subquery. Any USE clause attached to the top level braces is propagated to the first SingleQuery.
 *
 */
case object ExpandClauses extends StatementRewriter with StepSequencer.Step with ParsePipelineTransformerFactory {

  override def instance(from: BaseState, context: BaseContext): Rewriter =
    if (context.semanticFeatures.contains(DisableReworkedRewriters)) Rewriter.noop
    else
      getRewriter(from.scopeState(), from.anonymousVariableNameGenerator, context.cypherVersion)

  override def preConditions: Set[Condition] =
    Set(
      UpToDateScopes,
      BaseContains[SemanticTable](),
      ProjectionClausesHaveSemanticInfo,
      SemanticTypeCheckCompleted,
      DeprecatedSemanticsReplaced,
      SubqueriesInMutatingPatternsIsolated
    )

  override def postConditions: Set[Condition] = { Set(ContainsNoExpandableClauses) }

  override def invalidatedConditions: Set[Condition] =
    Set(ProjectionClausesHaveSemanticInfo, UpToDateScopes)

  def getRewriter(
    scopeState: ScopeState,
    anonVarNameGen: AnonymousVariableNameGenerator,
    version: CypherVersion
  ): Rewriter = {

    sealed trait SemanticContext { val mappedReturns: Map[LogicalVariable, LogicalVariable] = Map.empty }

    sealed trait Default extends SemanticContext
    case class InSingleQuery(override val mappedReturns: Map[LogicalVariable, LogicalVariable] = Map.empty)
        extends Default
    case class InUnionDistinct(override val mappedReturns: Map[LogicalVariable, LogicalVariable] = Map.empty)
        extends Default
    case class InUnionAll(override val mappedReturns: Map[LogicalVariable, LogicalVariable] = Map.empty)
        extends Default

    case class InWhen(override val mappedReturns: Map[LogicalVariable, LogicalVariable] = Map.empty)
        extends SemanticContext

    sealed trait InNext extends SemanticContext
    case class FirstInNext(override val mappedReturns: Map[LogicalVariable, LogicalVariable] = Map.empty) extends InNext
    case class BodyInNext(override val mappedReturns: Map[LogicalVariable, LogicalVariable] = Map.empty) extends InNext
    case class LastInNext(override val mappedReturns: Map[LogicalVariable, LogicalVariable]) extends InNext

    case object Layout {
      def empty: Layout = Layout(None, Seq.empty, Map.empty, None, InSingleQuery(), Set.empty, None)
    }

    /**
     * An expansion modify the Layout of the AST and propagates information to children and sibling clauses.
     *
     * Pushes use clauses to the deepest nested child.
     * @param use
     * Transformation of incoming rows to the first by-row operation in the children.
     * @param ingress
     * Tracks the incoming variables mapped to anonymized variables.
     * @param incomingMapping
     * Tracks the count variable from NEXT and the switch variable from WHEN.
     * @param utilityVariable
     * Tracks outer context of query and expected result anonymization.
     * @param semanticContext
     */
    case class Layout(
      use: Option[UseGraph],
      ingress: Seq[Clause],
      incomingMapping: Map[LogicalVariable, AnonymizedVariable],
      utilityVariable: Option[LogicalVariable],
      semanticContext: SemanticContext,
      referencedByQuery: Set[LogicalVariable],
      importingWith: Option[PositionedNode[With]]
    ) {
      def ensureUniqueIds: Rewriter = bottomUp(Rewriter.lift { case v: LogicalVariable => v.copyId })

      def pushUse(maybeUse: Option[UseGraph]): Layout = copy(use = if (maybeUse.isDefined) maybeUse else use)
      def consumeLayout: Layout = copy(use = None, ingress = Seq.empty, utilityVariable = None)
      def withIngress(ingress: Seq[Clause]): Layout = copy(ingress = ingress)

      def getIngress: Seq[Clause] = ingress.endoRewrite(ensureUniqueIds)

      def resultMapping: Map[LogicalVariable, LogicalVariable] = semanticContext.mappedReturns
      def withMapping(resultMapping: Map[LogicalVariable, LogicalVariable]): Layout = {
        val ctx = semanticContext match {
          case InSingleQuery(_)   => InSingleQuery(resultMapping)
          case InUnionDistinct(_) => InUnionDistinct(resultMapping)
          case InUnionAll(_)      => InUnionAll(resultMapping)
          case InWhen(_)          => InWhen(resultMapping)
          case FirstInNext(_)     => FirstInNext(resultMapping)
          case BodyInNext(_)      => BodyInNext(resultMapping)
          case LastInNext(_)      => LastInNext(resultMapping)
        }
        copy(semanticContext = ctx)
      }

      def withIncomingMapping(incomingMapping: Map[LogicalVariable, AnonymizedVariable]): Layout =
        copy(incomingMapping = incomingMapping)

      def withUtilityVariable(utilityVariable: Option[LogicalVariable]): Layout =
        copy(utilityVariable = utilityVariable)

      def withIncomingMappingAndContext(
        incomingMapping: Map[LogicalVariable, AnonymizedVariable],
        semanticContext: SemanticContext
      ): Layout = copy(incomingMapping = incomingMapping, semanticContext = semanticContext)

      def anonymizeResultMapping: Map[LogicalVariable, AnonymizedVariable] =
        resultMapping.map { case (k, v) => k -> AnonymizedVariable(k, v, k) }

      def getLastInNextContext(layout: Layout): Layout =
        if (layout.semanticContext.isInstanceOf[InNext])
          withIncomingMappingAndContext(anonymizeResultMapping, BodyInNext(layout.resultMapping))
        else withIncomingMappingAndContext(anonymizeResultMapping, LastInNext(layout.resultMapping))

      def inContext(semanticContext: SemanticContext): Layout = copy(semanticContext = semanticContext)
      def inSingleQuery: Layout = copy(semanticContext = InSingleQuery(semanticContext.mappedReturns))
      def inUnionDistinct: Layout = copy(semanticContext = InUnionDistinct(semanticContext.mappedReturns))
      def inUnionAll: Layout = copy(semanticContext = InUnionAll(semanticContext.mappedReturns))
      def inWhen: Layout = copy(semanticContext = InWhen(semanticContext.mappedReturns))

      def refByQuery(ast: ASTNode): Layout =
        copy(referencedByQuery =
          scopeState.recordedScopes.get(ast).fold(Set.empty[LogicalVariable])(scope =>
            scope.referenced ++ scope.children.flatMap(_.referenced)
          )
        )

      def refBySingleQuery(sq: SingleQuery): Layout = {
        copy(
          referencedByQuery =
            scopeState.recordedScopes.get(sq) match {
              case Some(scope) =>
                scope.referenced ++ scope.children.flatMap(_.referenced)
              case None =>
                sq.clauses.flatMap(c => scopeState.recordedScopes.get(c)).flatMap(_.referenced).toSet
            },
          importingWith =
            if (
              scopeState.recordedScopes.get(sq).exists {
                case StatementScope(_, _, _, _, _, _, _, true) => true
                case _                                         => false
              }
            )
              sq.partitionedClauses.importingWith.map(PositionedNode(_))
            else None
        )
      }

      private def drivingTableReset(pos: InputPosition): Clause =
        With(
          ReturnItems(
            FreeProjection,
            Seq(AliasedReturnItem(
              Count(Null()(pos.zeroLength))(pos),
              Variable(anonVarNameGen.nextName)(pos, isIsolated = false)
            )(pos))
          )(pos),
          AddedInRewriteGeneral()
        )(pos)

      // Adapts a SingleQuery to the Layout
      def adapt(singleQuery: SingleQuery): SingleQuery = {
        val rewrittenClauses = singleQuery.clauses.map(_.endoRewrite(rewriter(consumeLayout)))

        val adaptedClauses = if (singleQuery.partitionedClauses.initialGraphSelection.isDefined) {
          Seq(rewrittenClauses.head) ++ getIngress ++ rewrittenClauses.tail
        } else {
          use.toSeq ++ getIngress ++ rewrittenClauses
        }

        val lastClause = (semanticContext, adaptedClauses.last) match {
          case (_: LastInNext, c)           => Seq(c)
          case (_: InNext, f: Finish)       => Seq(drivingTableReset(f.position))
          case (_: InNext, u: UpdateClause) => Seq(u, drivingTableReset(u.position))
          case (_: InNext, r: Return)       => Seq(r.convertToWith)
          case (_, c)                       => Seq(c)
        }

        singleQuery.copy(adaptedClauses.dropRight(1) ++ lastClause)(singleQuery.position)

      }

      def anonymize(returnItem: ReturnItem): ReturnItem =
        if (semanticContext.mappedReturns.contains(returnItem.alias.get)) {
          returnItem.withName(semanticContext.mappedReturns(returnItem.alias.get).copyId)(returnItem.position)
        } else returnItem

      def anonymizeSortKey(sortItem: SortItem): SortItem = {
        sortItem.mapExpression(exp =>
          semanticContext.mappedReturns.foldLeft(exp)((accExpr, pair) =>
            accExpr.replaceAllOccurrencesBy(pair._1.copyId, pair._2.copyId)
          )
        )
      }
    }

    case class AnonymizedVariable(original: LogicalVariable, incoming: LogicalVariable, outgoing: LogicalVariable) {
      def anonymizedIncoming: Boolean = original != incoming
    }

    object NextExpansion {

      /**
       * Emulate By-table semantics when needed.
       *
       * UNWIND range(0, cnt - 1) AS i
       * WITH list_a[i] AS a, ..., list_c[i] AS c
       */
      def getNextIngress(
        incomingVariables: Map[LogicalVariable, AnonymizedVariable],
        countVariable: Option[LogicalVariable],
        pos: InputPosition
      ): Seq[Clause] = {
        if (countVariable.isDefined) {
          val index = Variable(anonVarNameGen.nextName)(pos, isIsolated = false)

          val unwind = Unwind(
            FunctionInvocation(
              FunctionName(Range.name)(pos),
              distinct = false,
              IndexedSeq(
                SignedDecimalIntegerLiteral("0")(pos.zeroLength),
                Subtract(countVariable.get, SignedDecimalIntegerLiteral("1")(pos.zeroLength))(pos)
              )
            )(pos),
            index
          )(pos)

          val accessedItems =
            incomingVariables.toSeq.map { case (original, anonymized) =>
              AliasedReturnItem(
                ContainerIndex(
                  ContainerIndex(anonymized.outgoing.copyId, index.copyId)(pos),
                  SignedDecimalIntegerLiteral("0")(pos.zeroLength)
                )(pos),
                original
              )(pos)
            }

          val remappingWith: Option[With] =
            Option.when(accessedItems.nonEmpty)(
              With(ReturnItems(FreeProjection, accessedItems)(pos), AddedInRewriteGeneral())(pos)
            )
          Seq(unwind) ++ remappingWith
        } else Seq.empty
      }

      def getNextPreface(
        collectedVariables: Map[LogicalVariable, AnonymizedVariable],
        countVariable: Option[LogicalVariable],
        willExpand: Boolean,
        pos: InputPosition
      ): Seq[Clause] = {
        if (countVariable.isDefined) {
          val countStar = AliasedReturnItem(CountStar()(pos), countVariable.get.copyId)(pos)
          val collectItems = collectedVariables.map {
            case (_, v) =>
              AliasedReturnItem(
                FunctionInvocation(FunctionName("collect")(pos), ListLiteral(Seq(v.incoming.copyId))(pos))(pos),
                v.outgoing.copyId
              )(pos)
          }.toSeq

          val returnItems = ReturnItems(FreeProjection, Seq(countStar) ++ collectItems)(pos)
          Seq(With(returnItems, withType = AddedInRewriteGeneral())(pos))
        } else if (collectedVariables.exists(_._2.anonymizedIncoming) && !willExpand) {
          val deanonymized = collectedVariables.map(av =>
            AliasedReturnItem(av._2.incoming.copyId, av._2.original.copyId)(pos)
          )
          Seq(With(ReturnItems(FreeProjection, deanonymized.toSeq)(pos))(pos))
        } else Seq.empty
      }

      // Checks if the directly contained query will require nesting a call subquery.
      def needsWrapping(ast: Query): Boolean = ast match {
        case sq: SingleQuery         => sq.partitionedClauses.initialGraphSelection.isDefined
        case _: ConditionalQueryWhen => false
        case tlb: TopLevelBraces => tlb.query match {
            case _: Union => true
            case _        => false
          }
        case _ => true
      }

      // Check is the directly contained query will be expanded. If the query will not be expanded later any preface
      // need to be inserted.
      def willBeExpanded(ast: Query): Boolean = ast match {
        case _: SingleQuery          => false
        case _: ConditionalQueryWhen => true
        case tlb: TopLevelBraces => tlb.query match {
            case _: UnionDistinct => true
            case _                => false
          }
        case _ => true
      }

      sealed trait QuerySemantics
      case object ByRow extends QuerySemantics
      case object ByTable extends QuerySemantics
      case object RequiresCollecting extends QuerySemantics

      // Logic deciding whether a query queries ByTable semantics.
      def checkQuerySemantics(query: Query, init: QuerySemantics = ByRow): QuerySemantics =
        query.folder.treeFold[QuerySemantics](init) {
          case _: SubqueryCall         => _ => SkipChildren(ByRow)
          case _: ConditionalQueryWhen => _ => SkipChildren(ByRow)
          case _: UnionDistinct        => _ => SkipChildren(RequiresCollecting)
          case _: UnionAll             => _ => TraverseChildren(ByTable)
          case ns: NextStatement       => _ => SkipChildren(checkQuerySemantics(ns.queries.head, ByRow))
          case _: UseGraph             => _ => TraverseChildren(ByTable)
          case p: ProjectionClause => {
            case ByTable if p.distinct =>
              SkipChildren(RequiresCollecting)
            case ByTable if p.limit.isDefined || p.skip.isDefined =>
              SkipChildren(RequiresCollecting)
            case acc => TraverseChildren(acc)
          }
          case _: UpdateClause => {
            case ByTable =>
              SkipChildren(RequiresCollecting)
            case acc => TraverseChildren(acc)
          }
          case _: FullSubqueryExpression => acc => SkipChildren(acc)
          case exp: Expression => {
            case ByTable if containsAggregate(exp) =>
              SkipChildren(RequiresCollecting)
            case acc => TraverseChildren(acc)
          }
        }

      /**
       * Identifies if a query uses By-table semantics when in a NEXT statement
       * If an aggregation, pagination or an update occurs in a ByTable context, then the rewriter
       * must collect and unwind the incoming rows to emulate by-table semantics.
       */
      def requiresCollecting(query: Query): Boolean =
        checkQuerySemantics(query) match {
          case RequiresCollecting => true
          case _                  => false
        }

      def expand(ast: Query, incomingLayout: Layout): (Layout, Seq[Clause]) = {

        val incomingConstants: Seq[LogicalVariable] = scopeState.getIncomingConstants(ast)
        val incomingVariables: Seq[LogicalVariable] = scopeState.getIncomingVariables(ast)
        val incomingSymbols: Set[LogicalVariable] = (incomingConstants ++ incomingVariables).toSet

        val referenced: Set[LogicalVariable] = scopeState.getReferenced(ast)
        val variableRefs: Set[LogicalVariable] = referenced.filterNot(incomingConstants.contains(_))

        val result: Result = scopeState.getResult(ast)
        val resultColumns: Seq[LogicalVariable] = result.getColumns

        val needsCollecting = requiresCollecting(ast) && incomingVariables.nonEmpty

        val willBeWrapped = needsWrapping(ast)

        val collectedColumns =
          if (incomingLayout.utilityVariable.isDefined) incomingLayout.incomingMapping
          else
            variableRefs.map(vr =>
              vr -> AnonymizedVariable(
                vr,
                incomingLayout.incomingMapping.get(vr).fold(vr)(_.incoming),
                if (needsCollecting) Variable(anonVarNameGen.nextName, ast.position) else vr
              )
            ).toMap

        val countVariable =
          if (incomingLayout.utilityVariable.isDefined) incomingLayout.utilityVariable
          else Option.when(needsCollecting)(Variable(anonVarNameGen.nextName, ast.position))

        val imports =
          referenced.filterNot(collectedColumns.keys.toSet) ++ collectedColumns.values.map(_.outgoing) ++ countVariable

        val preface: Seq[Clause] =
          if (incomingLayout.ingress.nonEmpty) Seq.empty
          else getNextPreface(collectedColumns, countVariable, willBeExpanded(ast), ast.position)

        val ingress: Seq[Clause] =
          if (incomingLayout.ingress.nonEmpty) incomingLayout.getIngress
          else getNextIngress(collectedColumns, countVariable, ast.position)

        // If returned columns exists in incoming symbols we need to anonymize the result columns
        val returnsMapped: Map[LogicalVariable, LogicalVariable] = {
          incomingLayout.semanticContext match {
            case ctx if ctx.mappedReturns.nonEmpty => ctx.mappedReturns
            case _ =>
              resultColumns.map(vr =>
                (
                  vr,
                  if (resultColumns.exists(incomingSymbols) && willBeWrapped)
                    Variable(anonVarNameGen.nextName, ast.position)
                  else vr.copyId
                )
              ).toMap
          }
        }

        val updatedCtx = if (returnsMapped.nonEmpty) {
          incomingLayout.semanticContext match {
            case _: FirstInNext => FirstInNext(returnsMapped)
            case _: BodyInNext  => BodyInNext(returnsMapped)
            case _: LastInNext  => LastInNext(returnsMapped)
            case x              => x
          }
        } else incomingLayout.semanticContext

        val incomingMapping: Map[LogicalVariable, AnonymizedVariable] =
          if (preface.isEmpty || needsCollecting) collectedColumns else Map.empty

        val updatedLayout: Layout =
          incomingLayout
            .withIngress(ingress)
            .withMapping(returnsMapped)
            .inContext(updatedCtx)
            .withIncomingMapping(incomingMapping)
            .withUtilityVariable(countVariable)

        val rewrittenQuery: Query = flattenQuery(ast, updatedLayout)

        val postface: Seq[Clause] =
          if (incomingLayout.semanticContext.isInstanceOf[LastInNext]) {
            (rewrittenQuery, result, willBeWrapped) match {
              case (SingleQuery(_ :+ Finish()), _, false)                          => Seq.empty[Clause]
              case (SingleQuery(_ :+ x), _, false) if x.isInstanceOf[UpdateClause] => Seq.empty[Clause]
              case (SingleQuery(_ :+ Return(_, _, _, _, _, _, _, _)), _, false)    => Seq.empty[Clause]
              case (_, TableResult(_), _) =>
                val items =
                  if (incomingLayout.semanticContext.mappedReturns.nonEmpty)
                    incomingLayout.semanticContext.mappedReturns.map { case (_, v) => AliasedReturnItem(v) }.toSeq
                  else returnsMapped.map { case (k, v) => AliasedReturnItem(v.copyId, k.copyId)(k.position) }.toSeq
                Seq(Return(ReturnItems(FreeProjection, items)(ast.position))(ast.position))
              case _ => Seq(Finish()(ast.position))
            }
          } else Seq.empty

        val getExpandedAST: Seq[Clause] = rewrittenQuery match {
          case sq: SingleQuery if !willBeWrapped => preface ++ sq.clauses ++ postface
          case q: Query =>
            preface ++ Seq(ScopeClauseSubqueryCall(q, imports.toSeq)(q.position)) ++ postface
        }

        val resultingLayout =
          incomingLayout
            .withIngress(Seq.empty)
            .withUtilityVariable(None)
            .withMapping(returnsMapped)
            .withIncomingMapping(Map.empty)

        (resultingLayout, getExpandedAST.endoRewrite(incomingLayout.ensureUniqueIds))
      }
    }

    object BranchExpansion {
      def expand(ast: PartQuery, incomingLayout: Layout, index: Int, listName: String): SingleQuery = {
        val position = ast.position
        val incomingVariables: Seq[LogicalVariable] = scopeState.getIncomingVariables(ast)
        val references: Set[LogicalVariable] = scopeState.getReferenced(ast)
        val isReturning: Boolean = scopeState.getResult(ast).isTableResult
        val returns: Seq[LogicalVariable] = scopeState.getResultCols(ast)

        /**
         * THEN ... QUERY ... --> WITH * WHERE condition = i ... QUERY ...
         */
        val ingress: Seq[Clause] =
          Seq(With(
            ReturnItems(
              FreeProjection,
              incomingVariables.map(AliasedReturnItem(_)) :+ AliasedReturnItem(Variable(listName, position))
            )(position),
            Where(Equals(
              Variable(listName, position),
              SignedDecimalIntegerLiteral(index.toString)(position.zeroLength)
            )(position))(position),
            AddedInRewriteGeneral()
          )(position))

        val nestedQuery =
          ScopeClauseSubqueryCall(
            ensureNoTopLevelBracesSingleQuery(ast, incomingLayout.consumeLayout),
            references.map(_.copyId).toSeq
          )(ast.position)

        val deanonymizeReturns = returns.map(r =>
          incomingLayout.resultMapping.getOrElse(r, r).copyId
        )

        val returningClause = if (isReturning)
          Return(ReturnItems(FreeProjection, deanonymizeReturns.map(AliasedReturnItem(_)))(ast.position))(ast.position)
        else Finish()(ast.position)

        SingleQuery(ingress ++ Seq(nestedQuery, returningClause))(ast.position)
      }
    }

    object WhenExpansion {
      def expand(ast: ConditionalQueryWhen, incomingLayout: Layout): SingleQuery = {

        val incomingConstants: Seq[LogicalVariable] = scopeState.getIncomingConstants(ast)
        val incomingVariables: Seq[LogicalVariable] = scopeState.getIncomingVariables(ast)
        val incomingSymbols: Set[LogicalVariable] = (incomingConstants ++ incomingVariables).toSet

        val referenced: Set[LogicalVariable] = scopeState.getReferenced(ast)

        val result: Result = scopeState.getResult(ast)
        val resultColumns: Seq[LogicalVariable] = result.getColumns

        val ConditionalQueryWhen(branches, default) = ast
        val pos = ast.position

        val listName = anonVarNameGen.nextName

        val imports = referenced.toSeq ++ Seq(Variable(listName, ast.position))

        // If returned columns exists in incoming symbols we need to anonymize the result columns
        val returnsMapped: Map[LogicalVariable, LogicalVariable] =
          if (
            incomingLayout.resultMapping.nonEmpty &&
            incomingLayout.resultMapping.forall { case (k, v) => k != v }
          ) incomingLayout.resultMapping
          else
            resultColumns.map(vr =>
              (vr, if (resultColumns.exists(incomingSymbols)) Variable(anonVarNameGen.nextName, ast.position) else vr)
            ).toMap

        val updatedLayout =
          incomingLayout
            .inWhen
            .withIngress(Seq.empty)
            .withUtilityVariable(Some(Variable(listName, ast.position)))
            .withMapping(returnsMapped)

        val defaultQuery = default.map(d => BranchExpansion.expand(d.query, updatedLayout, branches.size, listName))

        val branchQueries = branches.zipWithIndex.map { case (branch, idx) =>
          BranchExpansion.expand(branch.query, updatedLayout, idx, listName)
        } ++ defaultQuery

        val unionOfBranches =
          branchQueries.tail.foldLeft[Query](branchQueries.head) { case (acc, query) => UnionAll(acc, query)(pos) }

        // If the incoming variables need to be deanonymized we can't reference them in the CASE expression
        // if it is in the same WITH clause. In this case we split the deanonymization to a preceding clause.
        val incomingItems =
          if (
            incomingLayout.ingress.nonEmpty ||
            incomingLayout.incomingMapping.isEmpty ||
            incomingLayout.incomingMapping.forall(!_._2.anonymizedIncoming)
          )
            (None, referenced.toSeq.map(lv => AliasedReturnItem(lv)))
          else
            (
              Some(
                With(
                  ReturnItems(
                    FreeProjection,
                    referenced.toSeq.map(lv =>
                      AliasedReturnItem(
                        incomingLayout.incomingMapping.get(lv).fold(lv)(_.incoming).copyId,
                        lv.copyId
                      )(lv.position)
                    )
                  )(ast.position),
                  AddedInRewriteGeneral(Some("WHEN"))
                )(ast.position)
              ),
              referenced.toSeq.map(lv => AliasedReturnItem(lv))
            )

        /**
         *  WITH ..., CASE
         *          WHEN $param < 1 THEN 0
         *          WHEN $param > 1 THEN 1
         *          ELSE 2
         *  END AS condition
         */
        val preface: Seq[Clause] = incomingItems._1.toSeq ++ Seq(With(
          ReturnItems(
            FreeProjection,
            incomingItems._2 :+ AliasedReturnItem(
              CaseExpression(
                expression = None,
                alternatives = branches.zipWithIndex.map { case (branch, index) =>
                  (branch.predicate.get, SignedDecimalIntegerLiteral(index.toString)(pos.zeroLength))
                }.toList,
                default = Some(SignedDecimalIntegerLiteral(branches.size.toString)(pos.zeroLength))
              )(pos),
              Variable(listName, pos)
            )(pos)
          )(pos),
          withType = AddedInRewriteGeneral()
        )(pos))

        val subquery = ScopeClauseSubqueryCall(unionOfBranches, imports)(pos)

        def inNextPostface: Option[Clause] =
          Option.when(resultColumns.exists(incomingSymbols.contains)) {
            val items = returnsMapped.map { case (k, v) =>
              AliasedReturnItem(v.copyId, incomingLayout.resultMapping.getOrElse(k, k).copyId)(v.position)
            }.toSeq
            With(ReturnItems(FreeProjection, items)(ast.position), withType = AddedInRewriteGeneral())(ast.position)
          }

        def defaultPostface: Clause = {
          val items = scopeState.getResultCols(ast).map { v =>
            (updatedLayout.resultMapping.getOrElse(v, v), incomingLayout.resultMapping.getOrElse(v, v))
          }.map { case (v1, v2) => AliasedReturnItem(v1.copyId, v2.copyId)(v1.position) }

          Return(ReturnItems(FreeProjection, items)(pos))(pos)
        }

        val postface: Option[Clause] = (incomingLayout.semanticContext, ast.isReturning) match {
          case (_: FirstInNext | _: BodyInNext, _) => inNextPostface
          case (_, false)                          => Some(Finish()(pos))
          case (_, true)                           => Some(defaultPostface)
        }

        SingleQuery(incomingLayout.getIngress ++ preface ++ Seq(subquery) ++ postface)(ast.position)
          .endoRewrite(incomingLayout.ensureUniqueIds)
      }
    }

    object TopLevelBracesExpansion {
      @tailrec
      def expand(ast: TopLevelBraces, incomingLayout: Layout): SingleQuery = {

        val layoutWithUse = incomingLayout.pushUse(ast.use)

        def wrap(inner: Union, rewritten: Boolean): SingleQuery = {
          val innerRewritten =
            if (rewritten) inner
            else inner.endoRewrite(
              rewriter(layoutWithUse.withIngress(Seq.empty).withUtilityVariable(None).withIncomingMapping(Map.empty))
            )

          val postface: Clause =
            if (innerRewritten.isReturning) {
              val items =
                if (layoutWithUse.resultMapping.nonEmpty)
                  layoutWithUse.resultMapping.map { case (_, v) =>
                    AliasedReturnItem(v.copyId, v.copyId)(v.position)
                  }
                else innerRewritten.returnVariables.explicitVariables.map(AliasedReturnItem(_))
              val returnItems = ReturnItems(FreeProjection, items.toSeq)(ast.position)
              Return(returnItems)(ast.position)
            } else Finish()(ast.position)

          val imports: Seq[LogicalVariable] = scopeState.getReferenced(ast).toSeq

          val expandedQuery =
            layoutWithUse.getIngress ++ Seq(ScopeClauseSubqueryCall(
              innerRewritten,
              imports.map(_.copyId)
            )(ast.position)) :+ postface
          SingleQuery(expandedQuery)(ast.position)
        }

        ast.query match {
          case inner: Union        => wrap(inner, rewritten = false)
          case tlb: TopLevelBraces => TopLevelBracesExpansion.expand(tlb, layoutWithUse)
          case query: Query => query.endoRewrite(rewriter(layoutWithUse)) match {
              case union: Union    => wrap(union, rewritten = true)
              case sq: SingleQuery => sq
              case _ => throw new IllegalStateException("All other queries should have been rewritten away")
            }
        }
      }
    }

    /**
     * Checks if the current query needs to be collected and then unwrapped in the following query.
     * This is needed when the following query is aggregating or updating and
     * if the current query returns more than one row.
     *
     * This check could be further refined to limit needing to materialize the full working table
     * by expanding this check. Avenues of optimization is exploring SKIP/LIMIT and aggregations.
     */
    def rewriteNextQueries(queries: Seq[Query], incomingLayout: Layout): Seq[Clause] = {

      (queries.map(Some(_)) :+ None).sliding(2).foldLeft((incomingLayout, Seq.empty[Clause])) {
        case ((lay, clauses), Seq(Some(q1), Some(_))) if queries.head == q1 =>
          // The first query in a NextStatement takes the incoming mapping of the enclosing context
          val res =
            NextExpansion.expand(q1, lay.withIncomingMappingAndContext(incomingLayout.incomingMapping, FirstInNext()))
          (res._1, clauses ++ res._2)
        case ((lay, clauses), Seq(Some(q1), Some(_))) =>
          // The queries in the body of the NextStatement takes the result mapping of the previous query as incoming
          val res =
            NextExpansion.expand(q1, lay.withIncomingMappingAndContext(lay.anonymizeResultMapping, BodyInNext()))
          (res._1, clauses ++ res._2)
        case ((lay, clauses), Seq(Some(q1), None)) =>
          // The last query in the NextStatement takes the previous query as incoming and the outer context
          // as result mapping. If the outer context is a NextStatement it is treated as a BodyInNext and not LastInNext
          val res = NextExpansion.expand(q1, lay.getLastInNextContext(incomingLayout))
          (res._1, clauses ++ res._2)
        case (acc, _) => acc
      }._2
    }

    def expandReturnItems(clause: ProjectionClause, returnItems: ReturnItems, layout: Layout): ProjectionClause = {

      clause match {
        case w @ With(false, ReturnItems(_, Seq(), _), None, None, None, None, withType)
          if version != CypherVersion.Cypher5 &&
            withType != ParsedAsYield &&
            !layout.importingWith.contains(PositionedNode(w)) =>
          w.copyProjection(returnItems = ReturnItems(FreeProjection, Seq.empty, None)(returnItems.position))
            .withRewrittenType
        case _ =>
          val expandedItems = if (returnItems.includeExisting) {
            clause match {
              case w: With
                if version != CypherVersion.Cypher5 &&
                  !clause.isAggregating && w.withType != ParsedAsYield =>
                scopeState.getOutgoingVariablesAndConstantsReturnItemSeq(clause)
                  .filterNot(i =>
                    returnItems.items.exists(_.name == i.name) || !layout.referencedByQuery.exists(_.name == i.name)
                  )
              case _ =>
                scopeState.getOutgoingVariableReturnItemSeq(clause)
                  .filterNot(i => returnItems.items.exists(_.name == i.name))
            }
          } else Seq.empty

          val sortedItems =
            returnItems.defaultOrderOnColumns
              .map(order => expandedItems.sortBy(ri => order.indexOf(ri.name)))
              .getOrElse(expandedItems.sortBy(_.name))

          val allItems = sortedItems ++ returnItems.items
          val anonymizedItems =
            if (clause.isInstanceOf[Return]) allItems.map(layout.anonymize)
            else allItems

          val anonymizedSortKeys =
            if (clause.isInstanceOf[Return])
              clause.orderBy.map(ob => ob.copy(sortItems = ob.sortItems.map(layout.anonymizeSortKey))(ob.position))
            else clause.orderBy

          clause.copyProjection(
            returnItems =
              returnItems.copy(FreeProjection, anonymizedItems, defaultOrderOnColumns = None)(returnItems.position),
            orderBy = anonymizedSortKeys
          ).withRewrittenType
      }

    }

    def getScopeImports(call: ScopeClauseSubqueryCall): Seq[LogicalVariable] =
      scopeState.getReferenced(call).map(_.copyId).toSeq

    def removeTopLevelBraces(tlb: TopLevelBraces, layout: Layout): Query =
      tlb.query.endoRewrite(rewriter(layout.pushUse(tlb.use)))

    def flattenQuery(query: Query, layout: Layout): Query = (query, layout.semanticContext) match {
      case (tlb: TopLevelBraces, _: InNext) => tlb.query.endoRewrite(rewriter(layout.pushUse(tlb.use)))
      case (tlb: TopLevelBraces, _)         => TopLevelBracesExpansion.expand(tlb, layout.pushUse(tlb.use))
      case (q: Query, _)                    => q.endoRewrite(rewriter(layout))
    }

    def ensureNoTopLevelBracesSingleQuery(query: PartQuery, layout: Layout): SingleQuery = query match {
      case tlb: TopLevelBraces => TopLevelBracesExpansion.expand(tlb, layout.pushUse(tlb.use))
      case sq: SingleQuery     => sq.endoRewrite(rewriter(layout))
    }

    def rewriter(layout: Layout) = Rewriter.lift {
      case clause @ With(_, returnItems, _, _, _, _, _) =>
        expandReturnItems(clause, returnItems, layout)
      case clause @ Return(_, returnItems, _, _, _, _, _, _) =>
        expandReturnItems(clause, returnItems, layout)
      case clause @ Yield(returnItems, _, _, _, _, _) =>
        expandReturnItems(clause, returnItems, layout)
      case clause: ScopeClauseSubqueryCall if clause.isImportingAll =>
        clause.copy(isImportingAll = false, importedVariables = getScopeImports(clause))(clause.position)
      case u @ UnionDistinct(lhs, rhs) =>
        val updatedLayout = layout.inUnionDistinct.refByQuery(u)
        u.copy(
          flattenQuery(lhs, updatedLayout),
          ensureNoTopLevelBracesSingleQuery(rhs, updatedLayout)
        )(u.position)
      case u @ UnionAll(lhs, rhs) =>
        val updatedLayout = layout.inUnionAll.refByQuery(u)
        u.copy(
          flattenQuery(lhs, updatedLayout),
          ensureNoTopLevelBracesSingleQuery(rhs, updatedLayout)
        )(u.position)
      case tlb: TopLevelBraces =>
        removeTopLevelBraces(tlb, layout.refByQuery(tlb))
      case sq: SingleQuery if sq.partitionedClauses.initialGraphSelection.isDefined =>
        layout.inSingleQuery.refBySingleQuery(sq).adapt(sq)
      case sq: SingleQuery =>
        layout.refBySingleQuery(sq).adapt(sq)
      case wh: ConditionalQueryWhen =>
        WhenExpansion.expand(wh, layout.refByQuery(wh))
      case ns @ NextStatement(queries) =>
        SingleQuery(rewriteNextQueries(queries, layout.refByQuery(ns)))(ns.position)
    }

    def clauseCleanup(clauses: Seq[Clause]): Seq[Clause] = clauses.filter {
      case With(false, ReturnItems(_, Seq(), _), None, None, None, None, _) => false
      case _                                                                => true
    }

    def subqueryExpressionCleanup(query: Query): Query =
      query.mapEachSingleQuery(sq => {
        val clauses = clauseCleanup(sq.clauses)
        if (clauses.nonEmpty)
          sq.copy(clauses)(sq.position)
        else sq.copy(Seq(Return(
          ReturnItems(
            FreeProjection,
            Seq(AliasedReturnItem(
              SignedDecimalIntegerLiteral("1")(sq.position.zeroLength),
              Variable(anonVarNameGen.nextName, sq.position)
            )(sq.position))
          )(sq.position)
        )(sq.position)))(sq.position)
      })

    // Cypher 5 requires a WITH clause between clauses in some cases so the query is not possible to fully cleanup.
    def cleanupCypher5: Rewriter = Rewriter.noop

    def cleanup: Rewriter = Rewriter.lift({
      case ex: ExistsExpression =>
        ex.copy(query =
          subqueryExpressionCleanup(ex.query)
        )(ex.position, ex.computedIntroducedVariables, ex.computedScopeDependencies)

      case cnt: CountExpression =>
        cnt.copy(query =
          subqueryExpressionCleanup(cnt.query)
        )(cnt.position, cnt.computedIntroducedVariables, cnt.computedScopeDependencies)
      case sq: SingleQuery => sq.copy(clauseCleanup(sq.clauses))(sq.position)
    })

    version match {
      case CypherVersion.Cypher5 =>
        topDown(rewriter(Layout.empty) andThen cleanupCypher5)
      case CypherVersion.Cypher25 =>
        topDown(rewriter(Layout.empty)) andThen topDown(cleanup)
    }
  }

  override def getTransformer(
    literalExtractionStrategy: LiteralExtractionStrategy,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    obfuscateLiterals: Boolean
  ): Transformer[BaseContext, BaseState, BaseState] = ScopeSurveyor andThen ExpandClauses

}
