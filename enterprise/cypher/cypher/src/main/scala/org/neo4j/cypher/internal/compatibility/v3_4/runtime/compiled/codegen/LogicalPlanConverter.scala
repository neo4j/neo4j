/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.aggregation.AggregationConverter.aggregateExpressionConverter
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.aggregation.Distinct
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.ExpressionConverter.createExpression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.SortItem
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.v3_4.Eagerly.immutableMapValues
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.{InternalException, One, ZeroOneOrMany, symbols}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, FunctionInvocation}
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.v3_4.{expressions => ast, functions => ast_functions}

object LogicalPlanConverter {

  def asCodeGenPlan(logicalPlan: plans.LogicalPlan): CodeGenPlan = logicalPlan match {
    case p: plans.Argument => argumentAsCodeGenPlan(p)
    case p: plans.AllNodesScan => allNodesScanAsCodeGenPlan(p)
    case p: plans.NodeByLabelScan => nodeByLabelScanAsCodeGenPlan(p)
    case p: plans.NodeIndexSeek => nodeIndexSeekAsCodeGenPlan(p)
    case p: plans.NodeByIdSeek => nodeByIdSeekAsCodeGenPlan(p)
    case p: plans.NodeUniqueIndexSeek => nodeUniqueIndexSeekAsCodeGen(p)
    case p: plans.Expand => expandAsCodeGenPlan(p)
    case p: plans.NodeHashJoin => nodeHashJoinAsCodeGenPlan(p)
    case p: plans.CartesianProduct if p.findByAllClass[plans.NodeHashJoin].nonEmpty =>
      throw new CantCompileQueryException(s"This logicalPlan is not yet supported: $logicalPlan")
    case p: plans.CartesianProduct => cartesianProductAsCodeGenPlan(p)
    case p: plans.Selection => selectionAsCodeGenPlan(p)
    case p: plans.Top if hasStandaloneLimit(p) => throw new CantCompileQueryException(s"Not able to combine LIMIT and $p")
    case p: plans.Top => topAsCodeGenPlan(p)
    case p: plans.Limit => limitAsCodeGenPlan(p)
    case p: plans.Skip => skipAsCodeGenPlan(p)
    case p: plans.ProduceResult => produceResultsAsCodeGenPlan(p)
    case p: plans.Projection => projectionAsCodeGenPlan(p)
    case p: plans.Aggregation if hasLimit(p) || hasMultipleAggregations(p) =>
      throw new CantCompileQueryException(s"Not able to combine aggregation with $p")
    case p: plans.Aggregation => aggregationAsCodeGenPlan(p)
    case p: plans.Distinct if hasStandaloneLimit(p) => throw new CantCompileQueryException(s"Not able to combine LIMIT and $p")
    case p: plans.Distinct => distinctAsCodeGenPlan(p)
    case p: plans.NodeCountFromCountStore => nodeCountFromCountStore(p)
    case p: plans.RelationshipCountFromCountStore => relCountFromCountStore(p)
    case p: plans.UnwindCollection => unwindAsCodeGenPlan(p)
    case p: plans.Sort if hasStandaloneLimit(p) => throw new CantCompileQueryException(s"Not able to combine LIMIT and $p")
    case p: plans.Sort => sortAsCodeGenPlan(p)
    case p: plans.Apply => applyAsCodeGenPlan(p)

    case _ =>
      throw new CantCompileQueryException(s"This logicalPlan is not yet supported: ${logicalPlan.getClass.getSimpleName}")
  }

  private def argumentAsCodeGenPlan(argument: plans.Argument) = new CodeGenPlan with LeafCodeGenPlan {
    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val (methodHandle, actions) = context.popParent().consume(context, this, cardinalities)
      (methodHandle, actions)
    }

    override val logicalPlan: plans.LogicalPlan = argument
  }

  private def hasLimit(p: plans.LogicalPlan) = p.treeExists {
    case _: plans.Limit => true
    //top is limit + sort
    case _: plans.Top => true
  }

  private def hasStandaloneLimit(p: plans.LogicalPlan)= p.treeExists {
    case _: plans.Limit => true
  }

  private def hasMultipleAggregations(p: plans.Aggregation) = p.aggregationExpression.values.toList.treeCount {
    case f: FunctionInvocation if f.function == ast_functions.Count => true
    case _ => false
  } > 1

  private def projectionAsCodeGenPlan(projection: plans.Projection) = new CodeGenPlan {

    override val logicalPlan: plans.Projection = projection

    override def produce(context: CodeGenContext, cardinalities: Cardinalities) = {
      context.pushParent(this)
      asCodeGenPlan(projection.lhs.get).produce(context, cardinalities)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities) = {
      val projectionOpName = context.registerOperator(projection)
      val columns = immutableMapValues(projection.expressions,
                                       (e: ast.Expression) => ExpressionConverter.createExpression(e)(context))
      context.retainProjectedVariables(projection.expressions.keySet)
      val vars = columns.collect {
        case (name, expr) if !context.hasVariable(name) =>
          val variable = Variable(context.namer.newVarName(), expr.codeGenType(context), expr.nullable(context))
          context.addVariable(name, variable)
          context.addProjectedVariable(name, variable)
          variable -> expr
      }
      // Variables that pre-existed do not need new names, but need to be added to the collection of projected variables
      columns.foreach {
        case (name, _) if !context.isProjectedVariable(name) =>
          // The variable exists but has not yet been projected
          val existingVariable = context.getVariable(name)
          context.addProjectedVariable(name, existingVariable)
        case _ => ()
      }
      val (methodHandle, action :: tl) = context.popParent().consume(context, this, cardinalities)

      (methodHandle, ir.Projection(projectionOpName, vars, action) :: tl)
    }
  }

  private def produceResultsAsCodeGenPlan(produceResults: plans.ProduceResult) = new CodeGenPlan {

    override val logicalPlan = produceResults

    override def produce(context: CodeGenContext, cardinalities: Cardinalities) = {
      context.pushParent(this)
      asCodeGenPlan(produceResults.lhs.get).produce(context, cardinalities)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities) = {
      val produceResultOpName = context.registerOperator(produceResults)
      val projections = produceResults.columns.map(c =>
                                                     c -> ExpressionConverter
                                                       .createMaterializeExpressionForVariable(c)(context)).toMap

      (None, List(AcceptVisitor(produceResultOpName, projections)))
    }
  }

  private def allNodesScanAsCodeGenPlan(allNodesScan: plans.AllNodesScan) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: plans.LogicalPlan = allNodesScan

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      context.addVariable(allNodesScan.idName, variable)
      val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, WhileLoop(variable, ScanAllNodes(opName), actions) :: tl)
    }
  }

  private def nodeByLabelScanAsCodeGenPlan(nodeByLabelScan: plans.NodeByLabelScan) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: plans.LogicalPlan = nodeByLabelScan

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val nodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      val labelVar = context.namer.newVarName()
      context.addVariable(nodeByLabelScan.idName, nodeVar)
      val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, WhileLoop(nodeVar, ScanForLabel(opName, nodeByLabelScan.label.name, labelVar), actions) :: tl)
    }
  }

  private type IndexSeekFun = (String, String, CodeGenExpression, Variable, Instruction) => Instruction

  // Used by both nodeIndexSeekAsCodeGenPlan and nodeUniqueIndexSeekAsCodeGenPlan
  private def sharedIndexSeekAsCodeGenPlan(indexSeekFun: IndexSeekFun)
                                          (idName: String, valueExpr: plans.QueryExpression[Expression],
                                           indexSeek: plans.LogicalPlan) =
    new CodeGenPlan with LeafCodeGenPlan {
      override val logicalPlan: plans.LogicalPlan = indexSeek

      override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
        val nodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
        context.addVariable(idName, nodeVar)

        val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)
        val opName = context.registerOperator(logicalPlan)
        val indexSeekInstruction = valueExpr match {

          //single expression, do a index lookup for that value
          case plans.SingleQueryExpression(e) =>
            val expression = createExpression(e)(context)
            indexSeekFun(opName, context.namer.newVarName(), expression, nodeVar, actions)

          //collection, create set and for each element of the set do an index lookup
          case plans.ManyQueryExpression(e) =>
            val expression = ToSet(createExpression(e)(context))
            val expressionVar = Variable(context.namer.newVarName(), CodeGenType.Any, nullable = false)
            ForEachExpression(expressionVar, expression,
                              indexSeekFun(opName, context.namer.newVarName(), LoadVariable(expressionVar), nodeVar,
                                           actions))

          //collection used in composite index search, pass entire collection to index seek
          case plans.CompositeQueryExpression(e: ast.ListLiteral) =>
            throw new CantCompileQueryException(s"To be done")

          case e: plans.RangeQueryExpression[_] =>
            throw new CantCompileQueryException(s"To be done")

          case e => throw new CantCompileQueryException(s"$e is not a valid QueryExpression")
        }

        (methodHandle, indexSeekInstruction :: tl)
      }
    }

  private def nodeByIdSeekAsCodeGenPlan(seek: plans.NodeByIdSeek) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: plans.LogicalPlan = seek

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val nodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      context.addVariable(seek.idName, nodeVar)
      val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)
      val opName = context.registerOperator(logicalPlan)
      val seekOperation = seek.nodeIds match {
        case plans.SingleSeekableArg(e) => SeekNodeById(opName, nodeVar,
                                                        createExpression(e)(context), actions)
        case plans.ManySeekableArgs(e) => e match {
          case coll: ast.ListLiteral =>
            ZeroOneOrMany(coll.expressions) match {
              case One(value) => SeekNodeById(opName, nodeVar,
                                              createExpression(value)(context), actions)
              case _ =>
                val expression = createExpression(e)(context)
                val expressionVar = Variable(context.namer.newVarName(), CodeGenType.Any, nullable = false)
                ForEachExpression(expressionVar, expression,
                                  SeekNodeById(opName, nodeVar, LoadVariable(expressionVar), actions))
            }

          case exp =>
            val expression = ToSet(createExpression(exp)(context))
            val expressionVar = Variable(context.namer.newVarName(), CodeGenType.Any, nullable = false)
            ForEachExpression(expressionVar, expression,
                              SeekNodeById(opName, nodeVar, LoadVariable(expressionVar), actions))
        }
      }
      (methodHandle, seekOperation :: tl)
    }
  }

  private def nodeIndexSeekAsCodeGenPlan(indexSeek: plans.NodeIndexSeek) = {
    def indexSeekFun(opName: String, descriptorVar: String, expression: CodeGenExpression,
                     nodeVar: Variable, actions: Instruction) =
      WhileLoop(nodeVar, IndexSeek(opName, indexSeek.label.name, indexSeek.propertyKeys.map(_.name),
                                   descriptorVar, expression), actions)

    sharedIndexSeekAsCodeGenPlan(indexSeekFun)(indexSeek.idName, indexSeek.valueExpr, indexSeek)
  }

  private def nodeUniqueIndexSeekAsCodeGen(indexSeek: plans.NodeUniqueIndexSeek) = {
    def indexSeekFun(opName: String, descriptorVar: String, expression: CodeGenExpression,
                     nodeVar: Variable, actions: Instruction) =
      WhileLoop(nodeVar, IndexSeek(opName, indexSeek.label.name, indexSeek.propertyKeys.map(_.name),
                                   descriptorVar, expression), actions)

    sharedIndexSeekAsCodeGenPlan(indexSeekFun)(indexSeek.idName, indexSeek.valueExpr, indexSeek)
  }

  private def nodeHashJoinAsCodeGenPlan(nodeHashJoin: plans.NodeHashJoin) = new CodeGenPlan {

    override val logicalPlan: plans.LogicalPlan = nodeHashJoin

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      val (Some(symbol), leftInstructions) = asCodeGenPlan(logicalPlan.lhs.get).produce(context, cardinalities)
      val opName = context.registerOperator(logicalPlan)
      val lhsMethod = MethodInvocation(Set(opName), symbol, context.namer.newMethodName(), leftInstructions)

      context.pushParent(this)
      val (otherSymbol, rightInstructions) = asCodeGenPlan(logicalPlan.rhs.get).produce(context, cardinalities)
      (otherSymbol, lhsMethod :: rightInstructions)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities) = {
      if (child.logicalPlan eq logicalPlan.lhs.get) {
        val joinNodes = nodeHashJoin.nodes.map(n => context.getVariable(n))
        val probeTableName = context.namer.newVarName()

        val lhsSymbols = nodeHashJoin.left.availableSymbols
        val nodeNames = nodeHashJoin.nodes
        val notNodeSymbols = lhsSymbols intersect context.variableQueryVariables() diff nodeNames
        val symbols = notNodeSymbols.map(s => s -> context.getVariable(s)).toMap

        val opName = context.registerOperator(nodeHashJoin)
        val probeTable = BuildProbeTable(opName, probeTableName, joinNodes, symbols)(context)
        val probeTableSymbol = JoinTableMethod(probeTableName, probeTable.tableType)

        context.addProbeTable(this, probeTable.joinData)


        (Some(probeTableSymbol), List(probeTable))

      }
      else if (child.logicalPlan eq logicalPlan.rhs.get) {

        val joinNodes = nodeHashJoin.nodes.map(n => context.getVariable(n))
        val joinData = context.getProbeTable(this)
        joinData.vars foreach { case (_, symbol) => context.addVariable(symbol.variable, symbol.outgoing) }

        val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)

        (methodHandle, GetMatchesFromProbeTable(joinNodes, joinData, actions) :: tl)
      }
      else {
        throw new InternalException(s"Unexpected consume call by $child")
      }
    }
  }

  private def expandAsCodeGenPlan(expand: plans.Expand) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: plans.LogicalPlan = expand

    override def consume(context: CodeGenContext,
                         child: CodeGenPlan,
                         cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = expand
      .mode match {
      case plans.ExpandAll => expandAllConsume(context, child, cardinalities)
      case plans.ExpandInto => expandIntoConsume(context, child, cardinalities)
    }

    private def expandAllConsume(context: CodeGenContext,
                                 child: CodeGenPlan,
                                 cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val relVar = Variable(context.namer.newVarName(), CodeGenType.primitiveRel)
      val fromNodeVar = context.getVariable(expand.from)
      val toNodeVar = Variable(context.namer.newVarName(), CodeGenType.primitiveNode)
      context.addVariable(expand.relName, relVar)
      context.addVariable(expand.to, toNodeVar)

      val (methodHandle, action :: tl) = context.popParent().consume(context, this, cardinalities)
      val typeVar2TypeName = expand.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(expand)
      val expandGenerator = ExpandAllLoopDataGenerator(opName, fromNodeVar, expand.dir, typeVar2TypeName, toNodeVar,
                                                       relVar)

      (methodHandle, WhileLoop(relVar, expandGenerator, action) :: tl)
    }

    private def expandIntoConsume(context: CodeGenContext,
                                  child: CodeGenPlan,
                                  cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val relVar = Variable(context.namer.newVarName(), CodeGenType.primitiveRel)
      context.addVariable(expand.relName, relVar)
      val fromNodeVar = context.getVariable(expand.from)
      val toNodeVar = context.getVariable(expand.to)

      val (methodHandle, action :: tl) = context.popParent().consume(context, this, cardinalities)
      val typeVar2TypeName = expand.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(expand)
      val expandGenerator = ExpandIntoLoopDataGenerator(opName, fromNodeVar, expand.dir, typeVar2TypeName, toNodeVar,
                                                        relVar)

      (methodHandle, WhileLoop(relVar, expandGenerator, action) :: tl)
    }
  }

  private def cartesianProductAsCodeGenPlan(cartesianProduct: plans.CartesianProduct) = new CodeGenPlan {

    override val logicalPlan: plans.LogicalPlan = cartesianProduct

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      asCodeGenPlan(cartesianProduct.lhs.get).produce(context, cardinalities)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      if (child.logicalPlan eq cartesianProduct.lhs.get) {
        context.pushParent(this)
        val (m, actions) = asCodeGenPlan(cartesianProduct.rhs.get).produce(context, cardinalities)
        if (actions.isEmpty) throw new InternalException("Illegal call chain")
        (m, actions)
      } else if (child.logicalPlan eq cartesianProduct.rhs.get) {
        val opName = context.registerOperator(cartesianProduct)
        val (m, instruction :: tl) = context.popParent().consume(context, this, cardinalities)
        (m, CartesianProductInstruction(opName, instruction) :: tl)
      }
      else {
        throw new InternalException(s"Unexpected consume call by $child")
      }
    }
  }

  private def selectionAsCodeGenPlan(selection: plans.Selection) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: plans.LogicalPlan = selection

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val opName = context.registerOperator(selection)
      val predicates = selection.predicates.map(
        ExpressionConverter.createPredicate(_)(context)
      )

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this, cardinalities)

      val instruction = predicates.reverse.foldLeft[Instruction](innerBlock) {
        case (acc, predicate) => If(predicate, acc)
      }

      (methodHandle, SelectionInstruction(opName, instruction) :: tl)
    }
  }

  private def limitAsCodeGenPlan(limit: plans.Limit) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: plans.LogicalPlan = limit

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val opName = context.registerOperator(limit)
      val count = createExpression(limit.count)(context)
      val counterName = context.namer.newVarName()

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this, cardinalities)
      val instruction = DecreaseAndReturnWhenZero(opName, counterName, innerBlock, count)

      (methodHandle, instruction :: tl)
    }
  }

  private def skipAsCodeGenPlan(skip: plans.Skip) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: plans.LogicalPlan = skip

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val opName = context.registerOperator(skip)
      val numberToSkip = createExpression(skip.count)(context)
      val counterName = context.namer.newVarName()

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this, cardinalities)
      val instruction = SkipInstruction(opName, counterName, innerBlock, numberToSkip)

      (methodHandle, instruction :: tl)
    }
  }

  private def aggregationAsCodeGenPlan(aggregation: plans.Aggregation) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: plans.LogicalPlan = aggregation

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      implicit val codeGenContext = context
      val opName = context.registerOperator(aggregation)
      val groupingVariables = aggregation.groupingExpressions.map {
        case (name, e) =>
          val expr = createExpression(e)(context)
          val variable = Variable(context.namer.newVarName(), expr.codeGenType, expr.nullable(context))
          context.addVariable(name, variable)
          context.addProjectedVariable(name, variable)
          variable.name -> expr
      }

      // With aggregation we can only retain projected variables that are either grouping variables or aggregation variables.
      // The aggregation variables will be added to the context below by the aggregateExpressionConverter.
      context.retainProjectedVariables(aggregation.groupingExpressions.keySet)

      val aggregationExpression =
        if (aggregation.aggregationExpression.isEmpty) Seq(
          Distinct(opName, context.namer.newVarName(), groupingVariables))
        else aggregation.aggregationExpression.map {
          case (name, e) =>
            aggregateExpressionConverter(opName, groupingVariables, name, e)
        }

      val instruction = AggregationInstruction(opName, aggregationExpression)
      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this, cardinalities)
      val continuation = aggregationExpression.foldLeft(innerBlock) {
        case (acc, curr) => curr.continuation(acc)
      }

      (methodHandle, instruction :: continuation :: tl)
    }
  }

  private def distinctAsCodeGenPlan(distinct: plans.Distinct) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: plans.LogicalPlan = distinct

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      implicit val codeGenContext = context
      val opName = context.registerOperator(distinct)
      val groupingVariables = distinct.groupingExpressions.map {
        case (name, e) =>
          val expr = createExpression(e)(context)
          val variable = Variable(context.namer.newVarName(), expr.codeGenType, expr.nullable(context))
          context.addVariable(name, variable)
          context.addProjectedVariable(name, variable)
          variable.name -> expr
      }
      // With DISTINCT we can only retain projected variables that are grouping variables.
      // The aggregation variables will be added to the context below by the aggregateExpressionConverter.
      context.retainProjectedVariables(distinct.groupingExpressions.keySet)

      val distinctExpression = Seq(Distinct(opName, context.namer.newVarName(), groupingVariables))

      val instruction = AggregationInstruction(opName, distinctExpression)
      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this, cardinalities)
      val continuation = distinctExpression.foldLeft(innerBlock) {
        case (acc, curr) => curr.continuation(acc)
      }

      (methodHandle, instruction :: continuation :: tl)
    }
  }

  private def nodeCountFromCountStore(nodeCount: plans.NodeCountFromCountStore) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: plans.LogicalPlan = nodeCount

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveInt)
      context.addVariable(nodeCount.idName, variable)

      // Only the node count variable is projected from now on
      context.retainProjectedVariables(Set.empty)
      context.addProjectedVariable(nodeCount.idName, variable)

      val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)
      val opName = context.registerOperator(logicalPlan)

      val label = nodeCount.labelNames.map(ll => ll.map(l => context.semanticTable.id(l).map(_.id) -> l.name))
      (methodHandle, NodeCountFromCountStoreInstruction(opName, variable, label, actions) :: tl)
    }
  }

  private def relCountFromCountStore(relCount: plans.RelationshipCountFromCountStore) = new CodeGenPlan with LeafCodeGenPlan {
    override val logicalPlan: plans.LogicalPlan = relCount

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveInt)
      context.addVariable(relCount.idName, variable)

      // Only the relationship count variable is projected from now on
      context.retainProjectedVariables(Set.empty)
      context.addProjectedVariable(relCount.idName, variable)

      val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)
      val opName = context.registerOperator(logicalPlan)

      val startLabel = relCount.startLabel.map(l => context.semanticTable.id(l).map(_.id) -> l.name)
      val endLabel = relCount.endLabel.map(l => context.semanticTable.id(l).map(_.id) -> l.name)
      val types = relCount.typeNames.map(t => context.semanticTable.id(t).map(_.id) -> t.name)
      (methodHandle, RelationshipCountFromCountStoreInstruction(opName, variable, startLabel, types, endLabel,
                                                                actions) :: tl)
    }
  }

  private def unwindAsCodeGenPlan(unwind: plans.UnwindCollection) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan: plans.UnwindCollection = unwind

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities) = {
      val collection: CodeGenExpression = ExpressionConverter.createExpression(unwind.expression)(context)

      // TODO: Handle range
      val collectionCodeGenType = collection.codeGenType(context)

      val opName = context.registerOperator(logicalPlan)

      val elementCodeGenType = collectionCodeGenType match {
        case CypherCodeGenType(symbols.ListType(innerType), ListReferenceType(innerReprType)) =>
          CypherCodeGenType(innerType, innerReprType)
        case CypherCodeGenType(symbols.ListType(innerType), _) =>
          CypherCodeGenType(innerType, ReferenceType)
        case CypherCodeGenType(symbols.CTAny, _) =>
          CypherCodeGenType(symbols.CTAny, ReferenceType)
        case t =>
          throw new CantCompileQueryException(s"Unwind collection type $t not supported")
      }

      val loopDataGenerator =
        if (elementCodeGenType.isPrimitive)
          UnwindPrimitiveCollection(opName, collection)
        else
          UnwindCollection(opName, collection, elementCodeGenType)

      val variableName = context.namer.newVarName()
      val variable = Variable(variableName, elementCodeGenType, nullable = elementCodeGenType.canBeNullable)
      context.addVariable(unwind.variable, variable)
      // Unwind is a kind of projection that only adds one exposed variable, and keeps everything exposed that was already projected
      context.addProjectedVariable(unwind.variable, variable)

      val (methodHandle, actions :: tl) = context.popParent().consume(context, this, cardinalities)

      (methodHandle, WhileLoop(variable, loopDataGenerator, actions) :: tl)
    }
  }

  private def applyAsCodeGenPlan(apply: plans.Apply) = new CodeGenPlan {
    override val logicalPlan: plans.LogicalPlan = apply

    override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      asCodeGenPlan(apply.lhs.get).produce(context, cardinalities)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      if (child.logicalPlan eq apply.lhs.get) {
        context.pushParent(this)
        val (m, actions) = asCodeGenPlan(apply.rhs.get).produce(context, cardinalities)
        if (actions.isEmpty) throw new InternalException("Illegal call chain")
        (m, actions)
      } else if (child.logicalPlan eq apply.rhs.get) {
        val opName = context.registerOperator(apply)
        val (m, instruction :: tl) = context.popParent().consume(context, this, cardinalities)
        (m, ApplyInstruction(opName, instruction) :: tl)
      }
      else {
        throw new InternalException(s"Unexpected consume call by $child")
      }
    }
  }

  private def sortAsCodeGenPlan(sort: plans.Sort) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan = sort

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities) = {
      val opName = context.registerOperator(logicalPlan)

      val (variablesToKeep: Map[String, Variable],
      sortItems: Seq[SortItem],
      tupleVariables: Map[String, Variable],
      sortTableName: String) = prepareSortTableInfo(context, sort.sortItems)

      val estimatedCardinality = cardinalities.get(sort.id).amount

      val buildSortTableInstruction =
        BuildSortTable(opName, sortTableName, tupleVariables, sortItems, estimatedCardinality)(context)

      // Update the context for parent consumers to use the new outgoing variable names
      updateContextWithSortTableInfo(context, buildSortTableInstruction.sortTableInfo)

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this, cardinalities)

      val sortInstruction = SortInstruction(opName, buildSortTableInstruction.sortTableInfo)
      val continuation = GetSortedResult(opName, variablesToKeep, buildSortTableInstruction.sortTableInfo, innerBlock)

      (methodHandle, buildSortTableInstruction :: sortInstruction :: continuation :: tl)
    }
  }

  private def topAsCodeGenPlan(top: plans.Top) = new CodeGenPlan with SingleChildPlan {

    override val logicalPlan = top

    override def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities) = {
      val opName = context.registerOperator(logicalPlan)

      val (variablesToKeep: Map[String, Variable],
      sortItems: Seq[SortItem],
      tupleVariables: Map[String, Variable],
      sortTableName: String) = prepareSortTableInfo(context, top.sortItems)

      val countExpression = createExpression(top.limit)(context)

      val buildTopTableInstruction = BuildTopTable(opName, sortTableName, countExpression, tupleVariables, sortItems)(
        context)

      // Update the context for parent consumers to use the new outgoing variable names
      updateContextWithSortTableInfo(context, buildTopTableInstruction.sortTableInfo)

      val (methodHandle, innerBlock :: tl) = context.popParent().consume(context, this, cardinalities)

      val sortInstruction = SortInstruction(opName, buildTopTableInstruction.sortTableInfo)
      val continuation = GetSortedResult(opName, variablesToKeep, buildTopTableInstruction.sortTableInfo, innerBlock)

      (methodHandle, buildTopTableInstruction :: sortInstruction :: continuation :: tl)
    }
  }

  // Helper shared by sortAsCodeGenPlan and topAsCodeGenPlan
  private def prepareSortTableInfo(context: CodeGenContext, inputSortItems: Seq[ColumnOrder]):
  (Map[String, Variable], Seq[SortItem], Map[String, Variable], String) = {

    val variablesToKeep = context.getProjectedVariables // TODO: Intersect/replace with usedVariables(innerBlock)

    val sortItems = inputSortItems.map {
      case plans.Ascending(name) => spi.SortItem(name, spi.Ascending)
      case plans.Descending(name) => spi.SortItem(name, spi.Descending)
    }
    val additionalSortVariables = sortItems.collect {
      case spi.SortItem(name, _) if !variablesToKeep.isDefinedAt(name) => (name, context.getVariable(name))
    }
    val tupleVariables = variablesToKeep ++ additionalSortVariables

    val sortTableName = context.namer.newVarName()

    (variablesToKeep, sortItems, tupleVariables, sortTableName)
  }

  // Helper shared by sortAsCodeGenPlan and topAsCodeGenPlan
  private def updateContextWithSortTableInfo(context: CodeGenContext, sortTableInfo: SortTableInfo) = {
    sortTableInfo.fieldToVariableInfo.foreach {
      case (_, info) =>
        context.updateVariable(info.queryVariableName, info.outgoingVariable)
    }
  }

  trait SingleChildPlan extends CodeGenPlan {

    final override def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) = {
      context.pushParent(this)
      asCodeGenPlan(logicalPlan.lhs.get).produce(context, cardinalities)
    }
  }

}
