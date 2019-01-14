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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.ir.v3_5.VarPatternLength
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ProcedureCallMode
import org.neo4j.cypher.internal.runtime.interpreted.commands.KeyTokenResolver
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.PatternConverters._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverters, InterpretedCommandProjection}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{AggregationExpression, Literal, ShortestPathExpression}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{Predicate, True}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans.{ColumnOrder, Limit => LimitPlan, LoadCSV => LoadCSVPlan, Skip => SkipPlan, _}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.{Equals => ASTEquals, Expression => ASTExpression, _}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.{Eagerly, InternalException}

/**
 * Responsible for turning a logical plan with argument pipes into a new pipe.
 * When adding new Pipes and LogicalPlans, this is where you should be looking.
 */
case class InterpretedPipeBuilder(recurse: LogicalPlan => Pipe,
                                  readOnly: Boolean,
                                  expressionConverters: ExpressionConverters,
                                  rewriteAstExpression: ASTExpression => ASTExpression,
                                  tokenContext: TokenContext)
                                 (implicit semanticTable: SemanticTable) extends PipeBuilder {

  private def getBuildExpression(id: Id) = rewriteAstExpression andThen
    ((e: ASTExpression) => expressionConverters.toCommandExpression(id, e)) andThen
    (expression => expression.rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext)))

  def onLeaf(plan: LogicalPlan): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case Argument(_) =>
        ArgumentPipe()(id)

      case AllNodesScan(ident, _) =>
        AllNodesScanPipe(ident)(id = id)

      case NodeCountFromCountStore(ident, labels, _) =>
        NodeCountFromCountStorePipe(ident, labels.map(l => l.map(LazyLabel.apply)))(id = id)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        RelationshipCountFromCountStorePipe(ident, startLabel.map(LazyLabel.apply),
                                            new LazyTypes(typeNames.map(_.name).toArray), endLabel.map(LazyLabel.apply))(id = id)

      case NodeByLabelScan(ident, label, _) =>
        NodeByLabelScanPipe(ident, LazyLabel(label))(id = id)

      case NodeByIdSeek(ident, nodeIdExpr, _) =>
        NodeByIdSeekPipe(ident, expressionConverters.toCommandSeekArgs(id, nodeIdExpr))(id = id)

      case DirectedRelationshipByIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        DirectedRelationshipByIdSeekPipe(ident, expressionConverters.toCommandSeekArgs(id, relIdExpr), toNode, fromNode)(id = id)

      case UndirectedRelationshipByIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        UndirectedRelationshipByIdSeekPipe(ident, expressionConverters.toCommandSeekArgs(id, relIdExpr), toNode, fromNode)(id = id)

      case NodeIndexSeek(ident, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(ident, label, properties.toArray, valueExpr.map(buildExpression), indexSeekMode, indexOrder)(id = id)

      case NodeUniqueIndexSeek(ident, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(ident, label, properties.toArray, valueExpr.map(buildExpression), indexSeekMode, indexOrder)(id = id)

      case NodeIndexScan(ident, label, property, _, indexOrder) =>
        NodeIndexScanPipe(ident, label, property, indexOrder)(id = id)

      case NodeIndexContainsScan(ident, label, property, valueExpr, _, indexOrder) =>
        NodeIndexContainsScanPipe(ident, label,property, buildExpression(valueExpr), indexOrder)(id = id)

      case NodeIndexEndsWithScan(ident, label, property, valueExpr, _, indexOrder) =>
        NodeIndexEndsWithScanPipe(ident, label,property, buildExpression(valueExpr), indexOrder)(id = id)
    }
  }

  def onOneChildPlan(plan: LogicalPlan, source: Pipe): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case Projection(_, expressions) =>
        ProjectionPipe(source,  InterpretedCommandProjection(Eagerly.immutableMapValues(expressions, buildExpression)))(id = id)

      case ProjectEndpoints(_, rel, start, startInScope, end, endInScope, types, directed, length) =>
        ProjectEndpointsPipe(source, rel,
          start, startInScope,
          end, endInScope,
          types.map(_.toArray).map(LazyTypes.apply), directed, length.isSimple)()

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id = id)

      case DropResult(_) =>
        DropResultPipe(source)(id = id)

      case Selection(predicate, _) =>
        val predicateExpression =
          if (predicate.exprs.size == 1) buildExpression(predicate.exprs.head) else buildExpression(predicate)
        FilterPipe(source, predicateExpression)(id = id)

      case Expand(_, fromName, dir, types: Seq[RelTypeName], toName, relName, ExpandAll) =>
        ExpandAllPipe(source, fromName, relName, toName, dir, LazyTypes(types.toArray))(id = id)

      case Expand(_, fromName, dir, types: Seq[RelTypeName], toName, relName, ExpandInto) =>
        ExpandIntoPipe(source, fromName, relName, toName, dir, LazyTypes(types.toArray))(id = id)

      case LockNodes(_, nodesToLock) =>
        LockNodesPipe(source, nodesToLock)()

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandAll, predicates) =>
        val predicate: Predicate = predicates.map(buildPredicate(id, _)).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandAllPipe(source, fromName, relName, toName, dir, LazyTypes(types.toArray), predicate)(id = id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandInto, predicates) =>
        val predicate = predicates.map(buildPredicate(id, _)).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandIntoPipe(source, fromName, relName, toName, dir, LazyTypes(types.toArray), predicate)(id = id)

      case VarExpand(_,
                     fromName,
                     dir,
                     projectedDir,
                     types,
                     toName,
                     relName,
                     VarPatternLength(min, max),
                     expansionMode,
                     _, _, _, _, predicates) =>
        val predicate = varLengthPredicate(id, predicates)

        val nodeInScope = expansionMode match {
          case ExpandAll => false
          case ExpandInto => true
        }

        VarLengthExpandPipe(source, fromName, relName, toName, dir, projectedDir,
          LazyTypes(types.toArray), min, max, nodeInScope, predicate)(id = id)

      case ActiveRead(_) =>
        ActiveReadPipe(source)(id = id)

      case Optional(inner, protectedSymbols) =>
        OptionalPipe(inner.availableSymbols -- protectedSymbols, source)(id = id)

      case PruningVarExpand(_, from, dir, types, toName, minLength, maxLength, predicates) =>
        val predicate = varLengthPredicate(id, predicates)
        PruningVarLengthExpandPipe(source, from, toName, LazyTypes(types.toArray), dir, minLength, maxLength, predicate)(id = id)

      case Sort(_, sortItems) =>
        SortPipe(source, sortItems.map(translateColumnOrder))(id = id)

      case SkipPlan(_, count) =>
        SkipPipe(source, buildExpression(count))(id = id)

      case Top(_, sortItems, _) if sortItems.isEmpty => source

      case Top(_, sortItems, SignedDecimalIntegerLiteral("1")) =>
        Top1Pipe(source, ExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder).toList))(id = id)

      case Top(_, sortItems, limit) =>
        TopNPipe(source, buildExpression(limit),
                 ExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder).toList))(id = id)

      case LimitPlan(_, count, DoNotIncludeTies) =>
        LimitPipe(source, buildExpression(count))(id = id)

      case LimitPlan(_, count, IncludeTies) =>
        (source, count) match {
          case (SortPipe(inner, sortDescription), SignedDecimalIntegerLiteral("1")) =>
            Top1WithTiesPipe(inner, ExecutionContextOrdering.asComparator(sortDescription))(id = id)

          case _ => throw new InternalException("Including ties is only supported for very specific plans")
        }

      case Aggregation(_, groupingExpressions, aggregatingExpressions) if aggregatingExpressions.isEmpty =>
        val commandExpressions = Eagerly.immutableMapValues(groupingExpressions, buildExpression)
        val projection = InterpretedCommandProjection(commandExpressions)
        source match {
          case ProjectionPipe(inner, p) if p == projection =>
            DistinctPipe(inner, commandExpressions)(id = id)
          case _ =>
            DistinctPipe(source, commandExpressions)(id = id)
        }

      case Distinct(_, groupingExpressions) =>
        val commandExpressions = Eagerly.immutableMapValues(groupingExpressions, buildExpression)
        source match {
          case ProjectionPipe(inner, es) if es == commandExpressions =>
            DistinctPipe(inner, commandExpressions)(id = id)
          case _ =>
            DistinctPipe(source, commandExpressions)(id = id)
        }

      case Aggregation(_, groupingExpressions, aggregatingExpressions) =>
        EagerAggregationPipe(
          source,
          Eagerly.immutableMapValues(groupingExpressions, buildExpression),
          Eagerly.immutableMapValues[String, ASTExpression, AggregationExpression](aggregatingExpressions, buildExpression(_).asInstanceOf[AggregationExpression])
        )(id = id)

      case FindShortestPaths(_, shortestPathPattern, predicates, withFallBack, disallowSameNode) =>
        val legacyShortestPath = shortestPathPattern.expr.asLegacyPatterns(id, shortestPathPattern.name, expressionConverters).head
        val pathVariables = Set(legacyShortestPath.pathName, legacyShortestPath.relIterator.getOrElse(""))

        def noDependency(expression: ASTExpression) =
          (expression.dependencies.map(_.name) intersect pathVariables).isEmpty

        val (perStepPredicates, fullPathPredicates) = predicates.partition {
          case p: IterablePredicateExpression =>
            noDependency(
              p.innerPredicate.getOrElse(throw new InternalException("This should have been handled in planning")))
          case e => noDependency(e)
        }
        val commandPerStepPredicates = perStepPredicates.map(p => buildPredicate(id, p))
        val commandFullPathPredicates = fullPathPredicates.map(p => buildPredicate(id, p))

        val commandExpression = ShortestPathExpression(legacyShortestPath, commandPerStepPredicates,
                                                       commandFullPathPredicates, withFallBack, disallowSameNode)
        ShortestPathPipe(source, commandExpression, withFallBack, disallowSameNode)(id = id)

      case UnwindCollection(_, variable, collection) =>
        UnwindPipe(source, buildExpression(collection), variable)(id = id)

      case ProcedureCall(_, call@ResolvedCall(signature, callArguments, _, _, _)) =>
        val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
        val callArgumentCommands = callArguments.map(Some(_)).zipAll(signature.inputSignature.map(_.default.map(_.value)), None, None).map {
          case (given, default) => given.map(buildExpression).getOrElse(Literal(default.get))
        }
        val rowProcessing = ProcedureCallRowProcessing(signature)
        ProcedureCallPipe(source, signature, callMode, callArgumentCommands, rowProcessing, call.callResultTypes, call.callResultIndices)(id = id)

      case LoadCSVPlan(_, url, variableName, format, fieldTerminator, legacyCsvQuoteEscaping, bufferSize) =>
        LoadCSVPipe(source, format, buildExpression(url), variableName, fieldTerminator, legacyCsvQuoteEscaping, bufferSize)(id = id)

      case ProduceResult(_, columns) =>
        ProduceResultsPipe(source, columns)(id = id)

      case Create(_, nodes, relationships) =>
        CreatePipe(
          source,
          nodes.map(n =>
            CreateNodeCommand(n.idName, n.labels.map(LazyLabel.apply), n.properties.map(buildExpression))
          ).toArray,
          relationships.map(r =>
            CreateRelationshipCommand(r.idName, r.startNode, LazyType(r.relType.name), r.endNode, r.properties.map(buildExpression))
          ).toArray
        )(id = id)

      case MergeCreateNode(_, idName, labels, props) =>
        MergeCreateNodePipe(source,
          CreateNodeCommand(idName, labels.map(LazyLabel.apply), props.map(buildExpression))
        )(id = id)

      case MergeCreateRelationship(_, idName, startNode, typ, endNode, props) =>
        MergeCreateRelationshipPipe(source,
          CreateRelationshipCommand(idName, startNode, LazyType(typ)(semanticTable), endNode, props.map(buildExpression))
        )(id = id)

      case SetLabels(_, name, labels) =>
        SetPipe(source, SetLabelsOperation(name, labels.map(LazyLabel.apply)))(id = id)

      case SetNodeProperty(_, name, propertyKey, expression) =>
        val needsExclusiveLock = ASTExpression.hasPropertyReadDependency(name, expression, propertyKey)
        SetPipe(source, SetNodePropertyOperation(name, LazyPropertyKey(propertyKey),
          buildExpression(expression), needsExclusiveLock))(id = id)

      case SetNodePropertiesFromMap(_, name, expression, removeOtherProps) =>
        val needsExclusiveLock = ASTExpression.mapExpressionHasPropertyReadDependency(name, expression)
        SetPipe(source,
          SetNodePropertyFromMapOperation(name, buildExpression(expression), removeOtherProps, needsExclusiveLock))(id = id)

      case SetRelationshipProperty(_, name, propertyKey, expression) =>
        val needsExclusiveLock = ASTExpression.hasPropertyReadDependency(name, expression, propertyKey)
        SetPipe(source,
          SetRelationshipPropertyOperation(name, LazyPropertyKey(propertyKey), buildExpression(expression), needsExclusiveLock))(id = id)

      case SetRelationshipPropertiesFromMap(_, name, expression, removeOtherProps) =>
        val needsExclusiveLock = ASTExpression.mapExpressionHasPropertyReadDependency(name, expression)
        SetPipe(source,
          SetRelationshipPropertyFromMapOperation(name, buildExpression(expression), removeOtherProps, needsExclusiveLock))(id = id)

      case SetProperty(_, entityExpr, propertyKey, expression) =>
        SetPipe(source, SetPropertyOperation(
          buildExpression(entityExpr), LazyPropertyKey(propertyKey), buildExpression(expression)))(id = id)

      case RemoveLabels(_, name, labels) =>
        RemoveLabelsPipe(source, name, labels.map(LazyLabel.apply))(id = id)

      case DeleteNode(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DetachDeleteNode(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = true)(id = id)

      case DeleteRelationship(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DeletePath(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DetachDeletePath(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = true)(id = id)

      case DeleteExpression(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DetachDeleteExpression(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = true)(id = id)

      case Eager(_) =>
        EagerPipe(source)(id = id)

      case ErrorPlan(_, ex) =>
        ErrorPipe(source, ex)(id = id)

      case x =>
        throw new InternalException(s"Received a logical plan that has no physical operator $x")
    }
  }

  private def varLengthPredicate(id: Id, predicates: Seq[(LogicalVariable, ASTExpression)]): VarLengthPredicate  = {
    //Creates commands out of the predicates
    def asCommand(predicates: Seq[(LogicalVariable, ASTExpression)]) = {
      val (keys: Seq[LogicalVariable], exprs) = predicates.unzip

      val commands = exprs.map(buildPredicate(id, _))
      (context: ExecutionContext, state: QueryState, entity: AnyValue) => {
        keys.zip(commands).forall { case (variable: LogicalVariable, expr: Predicate) =>
          context(variable.name) = entity
          val result = expr.isTrue(context, state)
          context.remove(variable.name)
          result
        }
      }
    }

    //partition predicates on whether they deal with nodes or rels
    val (nodePreds, relPreds) = predicates.partition(e => semanticTable.seen(e._1) && semanticTable.isNode(e._1))
    val nodeCommand = asCommand(nodePreds)
    val relCommand = asCommand(relPreds)

    new VarLengthPredicate {
      override def filterNode(row: ExecutionContext, state: QueryState)(node: NodeValue): Boolean = nodeCommand(row, state, node)

      override def filterRelationship(row: ExecutionContext, state: QueryState)(rel: RelationshipValue): Boolean = relCommand(row, state, rel)
    }
  }

  def onTwoChildPlan(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case CartesianProduct(_, _) =>
        CartesianProductPipe(lhs, rhs)(id = id)

      case NodeHashJoin(nodes, _, _) =>
        NodeHashJoinPipe(nodes, lhs, rhs)(id = id)

      case LeftOuterHashJoin(nodes, l, r) =>
        val nullableVariables = r.availableSymbols -- l.availableSymbols
        val nullableCachedProperties = r.availableCachedNodeProperties.values.toSet -- l.availableCachedNodeProperties.values
        NodeLeftOuterHashJoinPipe(nodes, lhs, rhs, nullableVariables, nullableCachedProperties)(id = id)

      case RightOuterHashJoin(nodes, l, r) =>
        val nullableVariables = l.availableSymbols -- r.availableSymbols
        val nullableCachedProperties = l.availableCachedNodeProperties.values.toSet -- r.availableCachedNodeProperties.values
        NodeRightOuterHashJoinPipe(nodes, lhs, rhs, nullableVariables, nullableCachedProperties)(id = id)

      case Apply(_, _) => ApplyPipe(lhs, rhs)(id = id)

      case AssertSameNode(node, _, _) =>
        AssertSameNodePipe(lhs, rhs, node)(id = id)

      case SemiApply(_, _) =>
        SemiApplyPipe(lhs, rhs, negated = false)(id = id)

      case AntiSemiApply(_, _) =>
        SemiApplyPipe(lhs, rhs, negated = true)(id = id)

      case LetSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName, negated = false)(id = id)

      case LetAntiSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName, negated = true)(id = id)

      case SelectOrSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(id, predicate), negated = false)(id = id)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(id, predicate), negated = true)(id = id)

      case LetSelectOrSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName, buildPredicate(id, predicate), negated = false)(id = id)

      case LetSelectOrAntiSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName, buildPredicate(id, predicate), negated = true)(id = id)

      case ConditionalApply(_, _, ids) =>
        ConditionalApplyPipe(lhs, rhs, ids, negated = false)(id = id)

      case AntiConditionalApply(_, _, ids) =>
        ConditionalApplyPipe(lhs, rhs, ids, negated = true)(id = id)

      case Union(_, _) =>
        UnionPipe(lhs, rhs)(id = id)

      case TriadicSelection(_, _, positivePredicate, sourceId, seenId, targetId) =>
        TriadicSelectionPipe(positivePredicate, lhs, sourceId, seenId, targetId, rhs)(id = id)

      case ValueHashJoin(_, _, ASTEquals(lhsExpression, rhsExpression)) =>
        ValueHashJoinPipe(buildExpression(lhsExpression), buildExpression(rhsExpression), lhs, rhs)(id = id)

      case ForeachApply(_, _, variable, expression) =>
        ForeachPipe(lhs, rhs, variable, buildExpression(expression))(id = id)

      case RollUpApply(_, _, collectionName, identifierToCollection, nullables) =>
        RollUpApplyPipe(lhs, rhs, collectionName, identifierToCollection, nullables)(id = id)

      case x =>
        throw new InternalException(s"Received a logical plan that has no physical operator $x")
    }
  }

  private def buildPredicate(id: Id, expr: ASTExpression): Predicate = {
    val rewrittenExpr: ASTExpression = rewriteAstExpression(expr)

    expressionConverters.toCommandPredicate(id, rewrittenExpr).rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext)).asInstanceOf[Predicate]
  }

  private def translateColumnOrder(s: ColumnOrder): org.neo4j.cypher.internal.runtime.interpreted.pipes.ColumnOrder = s match {
    case plans.Ascending(name) => org.neo4j.cypher.internal.runtime.interpreted.pipes.Ascending(name)
    case plans.Descending(name) => org.neo4j.cypher.internal.runtime.interpreted.pipes.Descending(name)
  }
}
