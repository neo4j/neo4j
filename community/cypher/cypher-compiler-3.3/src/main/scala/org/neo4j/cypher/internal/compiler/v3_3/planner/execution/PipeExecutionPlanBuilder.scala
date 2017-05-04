/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.execution

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_3.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_3.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v3_3.ast.convert.commands.StatementConverters
import org.neo4j.cypher.internal.compiler.v3_3.commands.EntityProducerFactory
import org.neo4j.cypher.internal.compiler.v3_3.commands.expressions.{AggregationExpression, Literal, Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v3_3.commands.predicates.{True, _}
import org.neo4j.cypher.internal.compiler.v3_3.executionplan._
import org.neo4j.cypher.internal.compiler.v3_3.executionplan.builders.prepare.KeyTokenResolver
import org.neo4j.cypher.internal.compiler.v3_3.pipes._
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{Limit => LimitPlan, LoadCSV => LoadCSVPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.compiler.v3_3.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v3_3.{ExecutionContext, pipes, ast => compilerAst}
import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.ir.v3_2.exception.CantHandleQueryException
import org.neo4j.cypher.internal.ir.v3_2.{IdName, PeriodicCommit, VarPatternLength}
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.mutable

class PipeExecutionPlanBuilder(clock: Clock,
                               monitors: Monitors,
                               pipeBuilderFactory: PipeBuilderFactory = PipeBuilderFactory()) {
  def build(periodicCommit: Option[PeriodicCommit], plan: LogicalPlan, idMap: Map[LogicalPlan, Id])
           (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeInfo = {

    val topLevelPipe = buildPipe(plan, idMap)

    val fingerprint = planContext.statistics match {
      case igs: InstrumentedGraphStatistics =>
        Some(PlanFingerprint(clock.millis(), planContext.txIdProvider(), igs.snapshot.freeze))
      case _ =>
        None
    }

    val periodicCommitInfo = periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    PipeInfo(topLevelPipe, !plan.solved.readOnly,
             periodicCommitInfo, fingerprint, context.plannerName)
  }

  /*
  Traverses the logical plan tree structure and builds up the corresponding pipe structure. Given a logical plan such as:

          a
         / \
        b   c
       /   / \
      d   e   f

   populate(a) starts the session, and eagerly adds [a, c, f] to the plan stack. We then immediately pop 'f' from the
   plan stack, we build a pipe for it add it to the pipe stack, and pop 'c' from the plan stack. Since we are coming from
   'f', we add [c, e] to the stack and then pop 'e' out again. This is a leaf, so we build a pipe for it and add it to the
   pipe stack. We now pop 'c' from the plan stack again. This time we are coming from 'e', and so we know we can use
   two pipes from the pipe stack to use when building 'c'. We add this pipe to the pipe stack and pop 'a' from the plan
   stack. Since we are coming from 'a's RHS, we add [a,b,d] to the stack. Next step is to pop 'd' out, and build a pipe
   for it, storing it in the pipe stack. Pop ut 'b' from the plan stack, one pipe from the pipe stack, and build a pipe for 'b'.
   Next we pop out 'a', and this time we are coming from the LHS, and we can now pop two pipes from the pipe stack to
   build the pipe for 'a'. Thanks for reading this far - I didn't think we would make it!
   */
  private def buildPipe(plan: LogicalPlan, idMap: Map[LogicalPlan, Id])(implicit context: PipeExecutionBuilderContext, planContext: PlanContext): Pipe = {
    val pipeBuilder = pipeBuilderFactory(
      monitors = monitors,
      recurse = p => buildPipe(p, idMap),
      readOnly = plan.solved.all(_.queryGraph.readOnly),
      idMap)

    val planStack = new mutable.Stack[LogicalPlan]()
    val pipeStack = new mutable.Stack[Pipe]()
    var comingFrom = plan
    def populate(plan: LogicalPlan) = {
      var current = plan
      while (!current.isLeaf) {
        planStack.push(current)
        (current.lhs, current.rhs) match {
          case (Some(_), Some(right)) =>
            current = right

          case (Some(left), None) =>
            current = left
          case _ => throw new InternalException("This must not be!")
        }
      }
      comingFrom = current
      planStack.push(current)
    }

    populate(plan)

    while (planStack.nonEmpty) {
      val current = planStack.pop()

      (current.lhs, current.rhs) match {
        case (None, None) =>
          val newPipe = pipeBuilder
            .build(current)

          pipeStack.push(newPipe)

        case (Some(_), None) =>
          val source = pipeStack.pop()
          val newPipe = pipeBuilder
            .build(current, source)

          pipeStack.push(newPipe)

        case (Some(left), Some(_)) if comingFrom == left =>
          val arg1 = pipeStack.pop()
          val arg2 = pipeStack.pop()
          val newPipe = pipeBuilder
            .build(current, arg1, arg2)

          pipeStack.push(newPipe)

        case (Some(left), Some(right)) if comingFrom == right =>
          planStack.push(current)
          populate(left)
      }

      comingFrom = current

    }

    val result = pipeStack.pop()
    assert(pipeStack.isEmpty, "Should have emptied the stack of pipes by now!")

    result
  }
}

case class PipeBuilderFactory() {
  def apply(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean, idMap: Map[LogicalPlan, Id])
           (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder =
    ActualPipeBuilder(monitors, recurse, readOnly, idMap)
}

trait PipeBuilder {
  def build(plan: LogicalPlan): Pipe
  def build(plan: LogicalPlan, source: Pipe): Pipe
  def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe
}

/**
 * Responsible for turning a logical plan with argument pipes into a new pipe.
 * When adding new Pipes and LogicalPlans, this is where you should be looking.
 */
case class ActualPipeBuilder(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean, idMap: Map[LogicalPlan, Id])
                            (implicit context: PipeExecutionBuilderContext, planContext: PlanContext) extends PipeBuilder {

  def build(plan: LogicalPlan): Pipe = {
    val id = idMap.getOrElse(plan, new Id)
    plan match {
      case SingleRow() =>
        SingleRowPipe()(id)

      case arg@Argument(_) =>
        ArgumentPipe()(id = id)

      case AllNodesScan(IdName(ident), _) =>
        AllNodesScanPipe(ident)(id = id)

      case NodeCountFromCountStore(IdName(ident), label, _) =>
        NodeCountFromCountStorePipe(ident, label.map(LazyLabel.apply))(id = id)

      case RelationshipCountFromCountStore(IdName(ident), startLabel, typeNames, endLabel, _) =>
        RelationshipCountFromCountStorePipe(ident, startLabel.map(LazyLabel.apply), LazyTypes(typeNames.map(_.name)), endLabel.map(LazyLabel.apply))(id = id)

      case NodeByLabelScan(IdName(ident), label, _) =>
        NodeByLabelScanPipe(ident, LazyLabel(label))(id = id)

      case NodeByIdSeek(IdName(ident), nodeIdExpr, _) =>
        NodeByIdSeekPipe(ident, nodeIdExpr.asCommandSeekArgs)(id = id)

      case DirectedRelationshipByIdSeek(IdName(ident), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
        DirectedRelationshipByIdSeekPipe(ident, relIdExpr.asCommandSeekArgs, toNode, fromNode)(id = id)

      case UndirectedRelationshipByIdSeek(IdName(ident), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
        UndirectedRelationshipByIdSeekPipe(ident, relIdExpr.asCommandSeekArgs, toNode, fromNode)(id = id)

      case NodeIndexSeek(IdName(ident), label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(ident, label, propertyKeys, valueExpr.map(buildExpression), indexSeekMode)(id = id)

      case NodeUniqueIndexSeek(IdName(ident), label, propertyKeys, valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(ident, label, propertyKeys, valueExpr.map(buildExpression), indexSeekMode)(id = id)

      case NodeIndexScan(IdName(ident), label, propertyKey, _) =>
        NodeIndexScanPipe(ident, label, propertyKey)(id = id)

      case LegacyNodeIndexSeek(ident, hint: NodeStartItem, _) =>
        val source = SingleRowPipe()(id = id)
        val startItem = StatementConverters.StartItemConverter(hint).asCommandStartItem
        val ep = entityProducerFactory.readNodeStartItems((planContext, startItem))
        NodeStartPipe(source, ident.name, ep, Effects(ReadsAllNodes))(id = id)

      case LegacyRelationshipIndexSeek(ident, hint: RelationshipStartItem, _) =>
        val source = SingleRowPipe()(id = id)
        val startItem = StatementConverters.StartItemConverter(hint).asCommandStartItem
        val ep = entityProducerFactory.readRelationshipLegacy((planContext, startItem))
        RelationshipStartPipe(source, ident.name, ep)(id = id)

      case NodeIndexContainsScan(IdName(ident), label, propertyKey, valueExpr, _) =>
        NodeIndexContainsScanPipe(ident, label, propertyKey, buildExpression(valueExpr))(id = id)

      case NodeIndexEndsWithScan(IdName(ident), label, propertyKey, valueExpr, _) =>
        NodeIndexEndsWithScanPipe(ident, label, propertyKey, buildExpression(valueExpr))(id = id)
    }
  }

  def build(plan: LogicalPlan, source: Pipe): Pipe = {
    val id = idMap.getOrElse(plan, new Id)
    plan match {
      case Projection(_, expressions) =>
        ProjectionPipe(source, Eagerly.immutableMapValues(expressions, buildExpression))(id = id)

      case ProjectEndpoints(_, rel, start, startInScope, end, endInScope, types, directed, length) =>
        ProjectEndpointsPipe(source, rel.name,
          start.name, startInScope,
          end.name, endInScope,
          types.map(LazyTypes.apply), directed, length.isSimple)()

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id = id)

      case Selection(predicates, _) =>
        FilterPipe(source, predicates.map(buildPredicate).reduce(_ andWith _))(id = id)

      case Expand(_, IdName(fromName), dir, types: Seq[RelTypeName], IdName(toName), IdName(relName), ExpandAll) =>
        ExpandAllPipe(source, fromName, relName, toName, dir, LazyTypes(types))(id = id)

      case Expand(_, IdName(fromName), dir, types: Seq[RelTypeName], IdName(toName), IdName(relName), ExpandInto) =>
        ExpandIntoPipe(source, fromName, relName, toName, dir, LazyTypes(types))(id = id)

      case LockNodes(_, nodesToLock) =>
        LockNodesPipe(source, nodesToLock.map(_.name))()

      case OptionalExpand(_, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandAll, predicates) =>
        val predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandAllPipe(source, fromName, relName, toName, dir, LazyTypes(types), predicate)(id = id)

      case OptionalExpand(_, IdName(fromName), dir, types, IdName(toName), IdName(relName), ExpandInto, predicates) =>
        val predicate = predicates.map(buildPredicate).reduceOption(_ andWith _).getOrElse(True())
        OptionalExpandIntoPipe(source, fromName, relName, toName, dir, LazyTypes(types), predicate)(id = id)

      case VarExpand(_, IdName(fromName), dir, projectedDir, types, IdName(toName), IdName(relName), VarPatternLength(min, max), expansionMode, predicates) =>
        val predicate = varLengthPredicate(predicates)

        val nodeInScope = expansionMode match {
          case ExpandAll => false
          case ExpandInto => true
        }

        VarLengthExpandPipe(source, fromName, relName, toName, dir, projectedDir,
          LazyTypes(types), min, max, nodeInScope, predicate)(id = id)

      case Optional(inner, protectedSymbols) =>
        OptionalPipe((inner.availableSymbols -- protectedSymbols).map(_.name), source)(id = id)

      case PruningVarExpand(_, IdName(from), dir, types, IdName(toName), minLength, maxLength, predicates) =>
        val predicate = varLengthPredicate(predicates)
        PruningVarLengthExpandPipe(source, from, toName, LazyTypes(types), dir, minLength, maxLength, predicate)()

      case FullPruningVarExpand(_, IdName(from), dir, types, IdName(toName), minLength, maxLength, predicates) =>
        val predicate = varLengthPredicate(predicates)
        FullPruningVarLengthExpandPipe(source, from, toName, LazyTypes(types), dir, minLength, maxLength, predicate)()

      case Sort(_, sortItems) =>
        SortPipe(source, sortItems.map(translateSortDescription))(id = id)

      case SkipPlan(_, count) =>
        SkipPipe(source, buildExpression(count))(id = id)

      case Top(_, sortItems, SignedDecimalIntegerLiteral("1")) =>
        Top1Pipe(source, sortItems.map(translateSortDescription).toList)(id = id)

      case Top(_, sortItems, limit) =>
        TopNPipe(source, sortItems.map(translateSortDescription).toList, buildExpression(limit))(id = id)

      case LimitPlan(_, count, DoNotIncludeTies) =>
        LimitPipe(source, buildExpression(count))(id = id)

      case LimitPlan(_, count, IncludeTies) =>
        (source, count) match {
          case (SortPipe(inner, sortDescription), SignedDecimalIntegerLiteral("1")) =>
            Top1WithTiesPipe(inner, sortDescription.toList)(id = id)

          case _ => throw new InternalException("Including ties is only supported for very specific plans")
        }

      case Aggregation(_, groupingExpressions, aggregatingExpressions) if aggregatingExpressions.isEmpty =>
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
          groupingExpressions.keySet,
          Eagerly.immutableMapValues[String, ast.Expression, AggregationExpression](aggregatingExpressions, buildExpression(_).asInstanceOf[AggregationExpression])
        )(id = id)

      case FindShortestPaths(_, shortestPathPattern, predicates, withFallBack, disallowSameNode) =>
        val legacyShortestPath = shortestPathPattern.expr.asLegacyPatterns(shortestPathPattern.name.map(_.name)).head
        ShortestPathPipe(source, legacyShortestPath, predicates.map(toCommandPredicate), withFallBack, disallowSameNode)(id = id)

      case UnwindCollection(_, variable, collection) =>
        UnwindPipe(source, toCommandExpression(collection), variable.name)(id = id)

      case ProcedureCall(_, call@ResolvedCall(signature, callArguments, _, _, _)) =>
        val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
        val callArgumentCommands = callArguments.map(Some(_)).zipAll(signature.inputSignature.map(_.default.map(_.value)), None, None).map {
          case (given, default) => given.map(toCommandExpression).getOrElse(Literal(default.get))
        }
        val rowProcessing = ProcedureCallRowProcessing(signature)
        ProcedureCallPipe(source, signature.name, callMode, callArgumentCommands, rowProcessing, call.callResultTypes, call.callResultIndices)(id = id)

      case LoadCSVPlan(_, url, variableName, format, fieldTerminator, legacyCsvQuoteEscaping) =>
        LoadCSVPipe(source, format, toCommandExpression(url), variableName.name, fieldTerminator, legacyCsvQuoteEscaping)(id = id)

      case ProduceResult(columns, _) =>
        ProduceResultsPipe(source, columns)(id = id)

      case CreateNode(_, idName, labels, props) =>
        CreateNodePipe(source, idName.name, labels.map(LazyLabel.apply), props.map(toCommandExpression))(id = id)

      case MergeCreateNode(_, idName, labels, props) =>
        MergeCreateNodePipe(source, idName.name, labels.map(LazyLabel.apply), props.map(toCommandExpression))(id = id)

      case CreateRelationship(_, idName, startNode, typ, endNode, props) =>
        CreateRelationshipPipe(source, idName.name, startNode.name, LazyType(typ)(context.semanticTable), endNode.name, props.map(toCommandExpression))(id = id)

      case MergeCreateRelationship(_, idName, startNode, typ, endNode, props) =>
        MergeCreateRelationshipPipe(source, idName.name, startNode.name, typ, endNode.name, props.map(toCommandExpression))(id = id)

      case SetLabels(_, IdName(name), labels) =>
        SetPipe(source, SetLabelsOperation(name, labels.map(LazyLabel.apply)))(id = id)

      case SetNodeProperty(_, IdName(name), propertyKey, expression) =>
        SetPipe(source, SetNodePropertyOperation(name, LazyPropertyKey(propertyKey),
          toCommandExpression(expression)))(id = id)

      case SetNodePropertiesFromMap(_, IdName(name), expression, removeOtherProps) =>
        SetPipe(source,
          SetNodePropertyFromMapOperation(name, toCommandExpression(expression), removeOtherProps))(id = id)

      case SetRelationshipPropery(_, IdName(name), propertyKey, expression) =>
        SetPipe(source,
          SetRelationshipPropertyOperation(name, LazyPropertyKey(propertyKey), toCommandExpression(expression)))(id = id)

      case SetRelationshipPropertiesFromMap(_, IdName(name), expression, removeOtherProps) =>
        SetPipe(source,
          SetRelationshipPropertyFromMapOperation(name, toCommandExpression(expression), removeOtherProps))(id = id)

      case SetProperty(_, entityExpr, propertyKey, expression) =>
        SetPipe(source, SetPropertyOperation(
          toCommandExpression(entityExpr), LazyPropertyKey(propertyKey), toCommandExpression(expression)))(id = id)

      case RemoveLabels(_, IdName(name), labels) =>
        RemoveLabelsPipe(source, name, labels.map(LazyLabel.apply))(id = id)

      case DeleteNode(_, expression) =>
        DeletePipe(source, toCommandExpression(expression), forced = false)(id = id)

      case DetachDeleteNode(_, expression) =>
        DeletePipe(source, toCommandExpression(expression), forced = true)(id = id)

      case DeleteRelationship(_, expression) =>
        DeletePipe(source, toCommandExpression(expression), forced = false)(id = id)

      case DeletePath(_, expression) =>
        DeletePipe(source, toCommandExpression(expression), forced = false)(id = id)

      case DetachDeletePath(_, expression) =>
        DeletePipe(source, toCommandExpression(expression), forced = true)(id = id)

      case DeleteExpression(_, expression) =>
        DeletePipe(source, toCommandExpression(expression), forced = false)(id = id)

      case DetachDeleteExpression(_, expression) =>
        DeletePipe(source, toCommandExpression(expression), forced = true)(id = id)

      case Eager(_) =>
        EagerPipe(source)(id = id)

      case ErrorPlan(_, ex) =>
        ErrorPipe(source, ex)(id = id)

      case x =>
        throw new CantHandleQueryException(x.toString)
    }
  }

  private def varLengthPredicate(predicates: Seq[(Variable, Expression)]) = {
    //Creates commands out of the predicates
    def asCommand(predicates: Seq[(Variable, Expression)]) = {
      val (keys: Seq[Variable], exprs) = predicates.unzip
      val commands = exprs.map(buildPredicate)
      (context: ExecutionContext, state: QueryState, entity: PropertyContainer) => {
        keys.zip(commands).forall { case (variable: Variable, expr: Predicate) =>
          context(variable.name) = entity
          val result = expr.isTrue(context)(state)
          context.remove(variable.name)
          result
        }
      }
    }

    //partition particate on whether they deal with nodes or rels
    val (nodePreds, relPreds) = predicates.partition(e => table.seen(e._1) && table.isNode(e._1))
    val nodeCommand = asCommand(nodePreds)
    val relCommand = asCommand(relPreds)

    new VarLengthPredicate {
      override def filterNode(row: ExecutionContext, state: QueryState)(node: Node): Boolean = nodeCommand(row, state, node)

      override def filterRelationship(row: ExecutionContext, state: QueryState)(rel: Relationship): Boolean = relCommand(row, state, rel)
    }
  }

  def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {
    val id = idMap.getOrElse(plan, new Id)
    plan match {
      case CartesianProduct(_, _) =>
        CartesianProductPipe(lhs, rhs)(id = id)

      case NodeHashJoin(nodes, _, _) =>
        NodeHashJoinPipe(nodes.map(_.name), lhs, rhs)(id = id)

      case OuterHashJoin(nodes, l, r) =>
        NodeOuterHashJoinPipe(nodes.map(_.name), lhs, rhs, (r.availableSymbols -- l.availableSymbols).map(_.name))(id = id)

      case Apply(_, _) => ApplyPipe(lhs, rhs)(id = id)

      case AssertSameNode(node, _, _) =>
        AssertSameNodePipe(lhs, rhs, node.name)(id = id)

      case SemiApply(_, _) =>
        SemiApplyPipe(lhs, rhs, negated = false)(id = id)

      case AntiSemiApply(_, _) =>
        SemiApplyPipe(lhs, rhs, negated = true)(id = id)

      case LetSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName.name, negated = false)(id = id)

      case LetAntiSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName.name, negated = true)(id = id)

      case SelectOrSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(predicate), negated = false)(id = id)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(predicate), negated = true)(id = id)

      case LetSelectOrSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName.name, buildPredicate(predicate), negated = false)(id = id)

      case LetSelectOrAntiSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName.name, buildPredicate(predicate), negated = true)(id = id)

      case ConditionalApply(_, _, ids) =>
        ConditionalApplyPipe(lhs, rhs, ids.map(_.name), negated = false)(id = id)

      case AntiConditionalApply(_, _, ids) =>
        ConditionalApplyPipe(lhs, rhs, ids.map(_.name), negated = true)(id = id)

      case Union(_, _) =>
        UnionPipe(lhs, rhs)(id = id)

      case TriadicSelection(positivePredicate, _, sourceId, seenId, targetId, _) =>
        TriadicSelectionPipe(positivePredicate, lhs, sourceId.name, seenId.name, targetId.name, rhs)(id = id)

      case ValueHashJoin(_, _, ast.Equals(lhsExpression, rhsExpression)) =>
        ValueHashJoinPipe(buildExpression(lhsExpression), buildExpression(rhsExpression), lhs, rhs)(id = id)

      case ForeachApply(_, _, variable, expression) =>
        ForeachPipe(lhs, rhs, variable, toCommandExpression(expression))(id = id)

      case RollUpApply(_, _, collectionName, identifierToCollection, nullables) =>
        RollUpApplyPipe(lhs, rhs, collectionName.name, identifierToCollection.name, nullables.map(_.name))(id = id)

      case x =>
        throw new CantHandleQueryException(x.toString)
    }
  }

  private val resolver = new KeyTokenResolver
  private val entityProducerFactory = new EntityProducerFactory
  implicit private val monitor = monitors.newMonitor[PipeMonitor]()
  implicit val table: SemanticTable = context.semanticTable

  private object buildPipeExpressions extends Rewriter {
    private val instance = bottomUp(Rewriter.lift {
      case expr@compilerAst.NestedPlanExpression(patternPlan, expression) =>
        val pipe = recurse(patternPlan)
        val result = compilerAst.NestedPipeExpression(pipe, expression)(expr.position)
        result
    })

    override def apply(that: AnyRef): AnyRef = instance.apply(that)
  }

  private def buildExpression(expr: ast.Expression)(implicit planContext: PlanContext): CommandExpression = {
    val rewrittenExpr = expr.endoRewrite(buildPipeExpressions) // TODO

    toCommandExpression(rewrittenExpr).rewrite(resolver.resolveExpressions(_, planContext))
  }

  private def buildPredicate(expr: ast.Expression)(implicit context: PipeExecutionBuilderContext, planContext: PlanContext): Predicate = {
    val rewrittenExpr: Expression = expr.endoRewrite(buildPipeExpressions)

    toCommandPredicate(rewrittenExpr).rewrite(resolver.resolveExpressions(_, planContext)).asInstanceOf[Predicate]
  }

  private def translateSortDescription(s: logical.SortDescription): pipes.SortDescription = s match {
    case logical.Ascending(IdName(name)) => pipes.Ascending(name)
    case logical.Descending(IdName(name)) => pipes.Descending(name)
  }
}
