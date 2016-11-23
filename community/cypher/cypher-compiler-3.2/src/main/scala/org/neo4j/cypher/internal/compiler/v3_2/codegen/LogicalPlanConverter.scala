/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.codegen

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.ExpressionConverter._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v3_2.commands.{ManyQueryExpression, QueryExpression, RangeQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{One, ZeroOneOrMany}
import org.neo4j.cypher.internal.compiler.v3_2.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_2.helpers.Eagerly.immutableMapValues
import org.neo4j.cypher.internal.frontend.v3_2.{InternalException, ast, symbols}

object LogicalPlanConverter {

  def asCodeGenPlan(logicalPlan: LogicalPlan): CodeGenPlan = logicalPlan match {
    case p: SingleRow => singleRowAsCodeGenPlan(p)
    case p: AllNodesScan => allNodesScanAsCodeGenPlan(p)
    case p: NodeByLabelScan => nodeByLabelScanAsCodeGenPlan(p)
    case p: NodeIndexSeek => nodeIndexSeekAsCodeGenPlan(p)
    case p: NodeByIdSeek => nodeByIdSeekAsCodeGenPlan(p)
    case p: NodeUniqueIndexSeek => nodeUniqueIndexSeekAsCodeGen(p)
    case p: Expand => expandAsCodeGenPlan(p)
    case p: OptionalExpand => optExpandAsCodeGenPlan(p)
    case p: NodeHashJoin => nodeHashJoinAsCodeGenPlan(p)
    case p: CartesianProduct => cartesianProductAsCodeGenPlan(p)
    case p: Selection => selectionAsCodeGenPlan(p)
    case p: plans.Limit => limitAsCodeGenPlan(p)
    case p: plans.Skip => skipAsCodeGenPlan(p)
    case p: ProduceResult => produceResultsAsCodeGenPlan(p)
    case p: plans.Projection => projectionAsCodeGenPlan(p)
    case p: plans.Aggregation => aggregationAsCodeGenPlan(p)

    case _ =>
      throw new CantCompileQueryException(s"$logicalPlan is not yet supported")
  }

  private def singleRowAsCodeGenPlan(singleRow: SingleRow) = new CodeGenPlan with LeafCodeGenPlan {
    override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      val (methodHandle, actions) = context.popParent().consume(context, this)
      (methodHandle, actions)
    }

    override val logicalPlan: LogicalPlan = singleRow
  }

  private def projectionAsCodeGenPlan(projection: plans.Projection) = new CodeGenPlan {

    override val logicalPlan = projection

    override def produce(context: CodeGenContext) = {
      context.pushParent(this)
      asCodeGenPlan(projection.lhs.get).produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan) = {
      val projectionOpName = context.registerOperator(projection)
      val columns = immutableMapValues(projection.expressions,
                                       (e: ast.Expression) => ExpressionConverter.createProjection(e)(context))
      val vars = columns.map {
        case (name, expr) =>
          val variable = Variable(context.namer.newVarName(), CodeGenType(expr.codeGenType(context).ct, ReferenceType),
                                  expr.nullable(context))
          context.addProjection(name, variable)
          variable -> expr
      }
      val (methodHandle, action :: tl) = context.popParent().consume(context, this)

      (methodHandle, ir.Projection(projectionOpName, vars, action) :: tl)
    }
  }

  private def produceResultsAsCodeGenPlan(produceResults: ProduceResult) = new CodeGenPlan {

    override val logicalPlan = produceResults

    override def produce(context: CodeGenContext) = {
      context.pushParent(this)
      asCodeGenPlan(produceResults.lhs.get).produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan) = {
      val produceResultOpName = context.registerOperator(produceResults)
      val projections = (produceResults.lhs.get match {
        // if lhs is projection than we can simply load things that it projected
        case _: plans.Projection => produceResults.columns.map(c => c -> LoadVariable(context.getProjection(c)))
        // else we have to evaluate all expressions ourselves
        case _ => produceResults.columns.map(c => c -> ExpressionConverter.createExpressionForVariable(c)(context))
      }).toMap

      (None, List(AcceptVisitor(produceResultOpName, projections)))
    }
  }

  private def allNodesScanAsCodeGenPlan(allNodesScan: AllNodesScan) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: LogicalPlan = allNodesScan

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      context.addVariable(allNodesScan.idName.name, variable)
      val (methodHandle, actions :: tl) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, WhileLoop(variable, ScanAllNodes(opName), actions) :: tl)
    }
  }

  private def nodeByLabelScanAsCodeGenPlan(nodeByLabelScan: NodeByLabelScan) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: LogicalPlan = nodeByLabelScan

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      val nodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      val labelVar = context.namer.newVarName()
      context.addVariable(nodeByLabelScan.idName.name, nodeVar)
      val (methodHandle, actions :: tl) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, WhileLoop(nodeVar, ScanForLabel(opName, nodeByLabelScan.label.name, labelVar), actions) :: tl)
    }
  }

  private type IndexSeekFun = (String, String, CodeGenExpression, Variable, Instruction) => Instruction

  // Used by both nodeIndexSeekAsCodeGenPlan and nodeUniqueIndexSeekAsCodeGenPlan
  private def sharedIndexSeekAsCodeGenPlan(indexSeekFun: IndexSeekFun)
                                          (idName: String, valueExpr: QueryExpression[Expression],
                                           indexSeek: LogicalPlan) =
    new CodeGenPlan with LeafCodeGenPlan {
      override val logicalPlan: LogicalPlan = indexSeek

      override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
        val nodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
        context.addVariable(idName, nodeVar)

        val (methodHandle, actions :: tl) = context.popParent().consume(context, this)
        val opName = context.registerOperator(logicalPlan)
        val indexSeekInstruction = valueExpr match {
          //single expression, do a index lookup for that value
          case SingleQueryExpression(e) =>
            val expression = createExpression(e)(context)
            indexSeekFun(opName, context.namer.newVarName(), expression, nodeVar, actions)
          //collection, create set and for each element of the set do an index lookup
          case ManyQueryExpression(e: ast.ListLiteral) =>
            val expression = ToSet(createExpression(e)(context))
            val expressionVar = Variable(context.namer.newVarName(), CodeGenType(symbols.CTAny, ReferenceType),
                                         nullable = false)

            ForEachExpression(expressionVar, expression,
                              indexSeekFun(opName, context.namer.newVarName(), LoadVariable(expressionVar), nodeVar,
                                           actions))
          //Unknown, try to cast to collection and then same as above
          case ManyQueryExpression(e) =>
            val expression = ToSet(CastToCollection(createExpression(e)(context)))
            val expressionVar = Variable(context.namer.newVarName(), CodeGenType(symbols.CTAny, ReferenceType),
                                         nullable = false)
            ForEachExpression(expressionVar, expression,
                              indexSeekFun(opName, context.namer.newVarName(), LoadVariable(expressionVar), nodeVar,
                                           actions))

          case e: RangeQueryExpression[_] =>
            throw new CantCompileQueryException(s"To be done")

          case e => throw new InternalException(s"$e is not a valid QueryExpression")
        }

        (methodHandle, indexSeekInstruction :: tl)
      }
    }

  private def nodeByIdSeekAsCodeGenPlan(seek: NodeByIdSeek) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: LogicalPlan = seek

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      val nodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      context.addVariable(seek.idName.name, nodeVar)
      val (methodHandle, actions :: tl) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      val seekOperation = seek.nodeIds match {
        case SingleSeekableArg(e) => SeekNodeById(opName, nodeVar,
                                                  createExpression(e)(context), actions)
        case ManySeekableArgs(e) => e match {
          case coll: ast.ListLiteral =>
            ZeroOneOrMany(coll.expressions) match {
              case One(value) => SeekNodeById(opName, nodeVar,
                                              createExpression(value)(context), actions)
              case _ =>
                val expression = createExpression(e)(context)
                val expressionVar = Variable(context.namer.newVarName(), CodeGenType(symbols.CTAny, ReferenceType),
                                             nullable = false)
                ForEachExpression(expressionVar, expression,
                                  SeekNodeById(opName, nodeVar, LoadVariable(expressionVar), actions))
            }

          case exp =>
            val expression = ToSet(CastToCollection(createExpression(exp)(context)))
            val expressionVar = Variable(context.namer.newVarName(), CodeGenType(symbols.CTAny, ReferenceType),
                                         nullable = false)
            ForEachExpression(expressionVar, expression,
                              SeekNodeById(opName, nodeVar, LoadVariable(expressionVar), actions))
        }
      }
      (methodHandle, seekOperation :: tl)
    }
  }

  private def nodeIndexSeekAsCodeGenPlan(indexSeek: NodeIndexSeek) = {
    def indexSeekFun(opName: String, descriptorVar: String, expression: CodeGenExpression,
                     nodeVar: Variable, actions: Instruction) =
      WhileLoop(nodeVar, IndexSeek(opName, indexSeek.label.name, indexSeek.propertyKey.name,
                                   descriptorVar, expression), actions)

    sharedIndexSeekAsCodeGenPlan(indexSeekFun)(indexSeek.idName.name, indexSeek.valueExpr, indexSeek)
  }

  private def nodeUniqueIndexSeekAsCodeGen(indexSeek: NodeUniqueIndexSeek) = {
    def indexSeekFun(opName: String, descriptorVar: String, expression: CodeGenExpression,
                     nodeVar: Variable, actions: Instruction) =
      IndexUniqueSeek(opName, indexSeek.label.name, indexSeek.propertyKey.name,
                      descriptorVar, expression, nodeVar, actions)

    sharedIndexSeekAsCodeGenPlan(indexSeekFun)(indexSeek.idName.name, indexSeek.valueExpr, indexSeek)
  }

  private def nodeHashJoinAsCodeGenPlan(nodeHashJoin: NodeHashJoin) = new CodeGenPlan {

    override val logicalPlan: LogicalPlan = nodeHashJoin

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      val (Some(symbol), leftInstructions) = asCodeGenPlan(logicalPlan.lhs.get).produce(context)
      val opName = context.registerOperator(logicalPlan)
      val lhsMethod = MethodInvocation(Set(opName), symbol, context.namer.newMethodName(), leftInstructions)

      context.pushParent(this)
      val (otherSymbol, rightInstructions) = asCodeGenPlan(logicalPlan.rhs.get).produce(context)
      (otherSymbol, lhsMethod :: rightInstructions)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan) = {
      if (child.logicalPlan eq logicalPlan.lhs.get) {
        val joinNodes = nodeHashJoin.nodes.map(n => context.getVariable(n.name))
        val probeTableName = context.namer.newVarName()

        val lhsSymbols = nodeHashJoin.left.availableSymbols.map(_.name)
        val nodeNames = nodeHashJoin.nodes.map(_.name)
        val notNodeSymbols = lhsSymbols intersect context.variableQueryVariables() diff nodeNames
        val symbols = notNodeSymbols.map(s => s -> context.getVariable(s)).toMap

        val opName = context.registerOperator(nodeHashJoin)
        val probeTable = BuildProbeTable(opName, probeTableName, joinNodes, symbols)(context)
        val probeTableSymbol = JoinTableMethod(probeTableName, probeTable.tableType)

        context.addProbeTable(this, probeTable.joinData)


        (Some(probeTableSymbol), List(probeTable))

      }
      else if (child.logicalPlan eq logicalPlan.rhs.get) {

        val joinNodes = nodeHashJoin.nodes.map(n => context.getVariable(n.name))
        val joinData = context.getProbeTable(this)
        joinData.vars foreach { case (_, symbol) => context.addVariable(symbol.variable, symbol.outgoing) }

        val (methodHandle, actions :: tl) = context.popParent().consume(context, this)

        (methodHandle, GetMatchesFromProbeTable(joinNodes, joinData, actions) :: tl)
      }
      else {
        throw new InternalException(s"Unexpected consume call by $child")
      }
    }
  }

  private def expandAsCodeGenPlan(expand: Expand) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: LogicalPlan = expand

    override def consume(context: CodeGenContext,
                         child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = expand
      .mode match {
      case ExpandAll => expandAllConsume(context, child)
      case ExpandInto => expandIntoConsume(context, child)
    }

    private def expandAllConsume(context: CodeGenContext,
                                 child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      val relVar = Variable(context.namer.newVarName(), CodeGenType.primitiveRel)
      val toNodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      context.addVariable(expand.relName.name, relVar)
      context.addVariable(expand.to.name, toNodeVar)

      val (methodHandle, action :: tl) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(expand.from.name)
      val typeVar2TypeName = expand.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(expand)
      val expandGenerator = ExpandAllLoopDataGenerator(opName, fromNodeVar, expand.dir, typeVar2TypeName, toNodeVar,
                                                       relVar)

      (methodHandle, WhileLoop(relVar, expandGenerator, action) :: tl)
    }

    private def expandIntoConsume(context: CodeGenContext,
                                  child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      val relVar = Variable(context.namer.newVarName(), CodeGenType.primitiveRel)
      context.addVariable(expand.relName.name, relVar)

      val (methodHandle, action :: tl) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(expand.from.name)
      val toNodeVar = context.getVariable(expand.to.name)
      val typeVar2TypeName = expand.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(expand)
      val expandGenerator = ExpandIntoLoopDataGenerator(opName, fromNodeVar, expand.dir, typeVar2TypeName, toNodeVar,
                                                        relVar)

      (methodHandle, WhileLoop(relVar, expandGenerator, action) :: tl)
    }
  }

  private def optExpandAsCodeGenPlan(optionalExpand: OptionalExpand) = new CodeGenPlan {

    override val logicalPlan: LogicalPlan = optionalExpand

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      asCodeGenPlan(optionalExpand.lhs.get).produce(context)
    }

    override def consume(context: CodeGenContext,
                         child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = optionalExpand.mode match {
      case ExpandAll => expandAllConsume(context, child)
      case ExpandInto => expandIntoConsume(context, child)
    }

    private def expandAllConsume(context: CodeGenContext,
                                 child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      //mark relationship and node to visit as nullable
      val relVar = Variable(context.namer.newVarName(), CodeGenType.primitiveRel, nullable = true)
      val toNodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode, nullable = true)
      context.addVariable(optionalExpand.relName.name, relVar)
      context.addVariable(optionalExpand.to.name, toNodeVar)

      val (methodHandle, action :: tl) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(optionalExpand.from.name)
      val typeVar2TypeName = optionalExpand.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(optionalExpand)

      //name of flag to check if results were yielded
      val yieldFlag = context.namer.newVarName()

      val predicatesAsCodeGenExpressions =
        optionalExpand.
          predicates.
          // We reverse the order of the predicates so the least selective comes first in line
          reverseMap(ExpressionConverter.createPredicate(_)(context))

      //wrap inner instructions with predicates -
      // the least selective predicate gets wrapped in an If, which is then wrapped in an If, until we reach the action
      val instructionWithPredicates = predicatesAsCodeGenExpressions.foldLeft[Instruction](
        CheckingInstruction(action, yieldFlag)) {
        case (acc, predicate) => If(predicate, acc)
      }

      val expand = ExpandAllLoopDataGenerator(opName, fromNodeVar, optionalExpand.dir, typeVar2TypeName, toNodeVar,
                                              relVar)

      val loop = WhileLoop(relVar, expand, instructionWithPredicates)

      (methodHandle, NullingInstruction(loop, yieldFlag, action, relVar, toNodeVar) :: tl)
    }

    private def expandIntoConsume(context: CodeGenContext,
                                  child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      //mark relationship  to visit as nullable
      val relVar = Variable(context.namer.newVarName(), CodeGenType.primitiveRel, nullable = true)
      context.addVariable(optionalExpand.relName.name, relVar)

      val (methodHandle, action :: tl) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(optionalExpand.from.name)
      val toNodeVar = context.getVariable(optionalExpand.to.name)
      val typeVar2TypeName = optionalExpand.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(optionalExpand)

      //name of flag to check if results were yielded
      val yieldFlag = context.namer.newVarName()

      val predicatesAsCodeGenExpressions =
        optionalExpand.
          predicates.
          // We reverse the order of the predicates so the least selective comes first in line
          reverseMap(ExpressionConverter.createPredicate(_)(context))

      //wrap inner instructions with predicates -
      // the least selective predicate gets wrapped in an If, which is then wrapped in an If, until we reach the action
      val instructionWithPredicates = predicatesAsCodeGenExpressions.foldLeft[Instruction](
        CheckingInstruction(action, yieldFlag)) {
        case (acc, predicate) => If(predicate, acc)
      }

      val expand = ExpandIntoLoopDataGenerator(opName, fromNodeVar, optionalExpand.dir, typeVar2TypeName, toNodeVar,
                                               relVar)

      val loop = WhileLoop(relVar, expand, instructionWithPredicates)

      (methodHandle, NullingInstruction(loop, yieldFlag, action, relVar) :: tl)
    }
  }

  private def cartesianProductAsCodeGenPlan(cartesianProduct: CartesianProduct) = new CodeGenPlan {

    override val logicalPlan: LogicalPlan = cartesianProduct

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      asCodeGenPlan(cartesianProduct.lhs.get).produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      if (child.logicalPlan eq cartesianProduct.lhs.get) {
        context.pushParent(this)
        val (m, actions) = asCodeGenPlan(cartesianProduct.rhs.get).produce(context)
        if (actions.isEmpty) throw new InternalException("Illegal call chain")
        (m, actions)
      } else if (child.logicalPlan eq cartesianProduct.rhs.get) {
        val opName = context.registerOperator(cartesianProduct)
        val (m, instruction :: tl) = context.popParent().consume(context, this)
        (m, CartesianProductInstruction(opName, instruction) :: tl)
      }
      else {
        throw new InternalException(s"Unexpected consume call by $child")
      }
    }
  }

  private def selectionAsCodeGenPlan(selection: Selection) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: LogicalPlan = selection

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      val opName = context.registerOperator(selection)
      val predicates = selection.predicates.map(
        ExpressionConverter.createPredicate(_)(context)
      )

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this)

      val instruction = predicates.reverse.foldLeft[Instruction](innerBlock) {
        case (acc, predicate) => If(predicate, acc)
      }

      (methodHandle, SelectionInstruction(opName, instruction) :: tl)
    }
  }

  private def limitAsCodeGenPlan(limit: plans.Limit) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: LogicalPlan = limit

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      val opName = context.registerOperator(limit)
      val count = createExpression(limit.count)(context)
      val counterName = context.namer.newVarName()

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this)
      val instruction = DecreaseAndReturnWhenZero(opName, counterName, innerBlock, count)

      (methodHandle, instruction :: tl)
    }
  }

  private def skipAsCodeGenPlan(skip: plans.Skip) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: LogicalPlan = skip

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      val opName = context.registerOperator(skip)
      val numberToSkip = createExpression(skip.count)(context)
      val counterName = context.namer.newVarName()

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this)
      val instruction = SkipInstruction(opName, counterName, innerBlock, numberToSkip)

      (methodHandle, instruction :: tl)
    }
  }

  private def aggregationAsCodeGenPlan(aggregation: Aggregation) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: LogicalPlan = aggregation

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) = {
      if (aggregation.groupingExpressions.size > 1) throw new CantCompileQueryException("Not yet able to compile")
      implicit val codeGenContext = context

      val opName = context.registerOperator(aggregation)

      val groupingVariables = aggregation.groupingExpressions.keys.map(context.getProjection)

      def aggregateExpressionConverter(name: String, e: ast.Expression) = {
        val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveInt)
        context.addVariable(name, variable)
        e match {
          case func: ast.FunctionInvocation => func.function match {
            case ast.functions.Count if groupingVariables.isEmpty =>
              SimpleCount(variable, createExpression(func.args(0)), func.distinct)
            case ast.functions.Count if groupingVariables.size == 1  =>
              val arg = createExpression(func.args(0))
              new DynamicCount(opName, variable, arg, groupingVariables, func.distinct)

            case f => throw new CantCompileQueryException(s"$f is not supported")
          }
          case _ => throw new CantCompileQueryException(s"$e is not supported")
        }
      }

      val aggregationExpression =
        if (aggregation.aggregationExpression.isEmpty) throw new CantCompileQueryException("not yet")
        else aggregation.aggregationExpression.map {
          case (name, e) => aggregateExpressionConverter(name, e)
        }

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this)
      val instruction = AggregationInstruction(opName, aggregationExpression)
      val continuation = aggregationExpression.foldLeft(innerBlock) {
        case (acc, curr) => curr.continuation(acc)
      }

      (methodHandle, instruction :: continuation :: tl)
    }
  }

  trait SingleChildPlan extends CodeGenPlan {

    final override def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      asCodeGenPlan(logicalPlan.lhs.get).produce(context)
    }
  }
}
