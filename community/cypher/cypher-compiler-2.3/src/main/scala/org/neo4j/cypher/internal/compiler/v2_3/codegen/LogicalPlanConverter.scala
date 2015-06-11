/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen

import org.neo4j.cypher.internal.compiler.v2_3.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.{InternalException, ast, symbols}

object LogicalPlanConverter {

  implicit class LogicalPlan2CodeGenPlan(val logicalPlan: LogicalPlan) {

    def asCodeGenPlan: CodeGenPlan = logicalPlan match {
      case p: SingleRow => p.asCodeGenPlan
      case p: AllNodesScan => p.asCodeGenPlan
      case p: NodeByLabelScan => p.asCodeGenPlan
      case p: NodeIndexSeek => p.asCodeGenPlan
      case p: Expand => p.asCodeGenPlan
      case p: OptionalExpand => p.asCodeGenPlan
      case p: NodeHashJoin => p.asCodeGenPlan
      case p: CartesianProduct => p.asCodeGenPlan
      case p: Selection => p.asCodeGenPlan
      case p: plans.Limit => p.asCodeGenPlan
      case produceResult@ProduceResult(_, _, _, projection: Projection) =>
        ProduceProjectionResults(produceResult, projection)

      case _ =>
        throw new CantCompileQueryException(s"$logicalPlan is not yet supported")
    }
  }

  private implicit class SingleRowCodeGen(singleRow: SingleRow) {
    def asCodeGenPlan = new CodeGenPlan with LeafCodeGenPlan {
      override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
        val (methodHandle, actions) = context.popParent().consume(context, this)
        (methodHandle, Seq(actions))
      }

      override val logicalPlan: LogicalPlan = singleRow
    }
  }

  private case class ProduceProjectionResults(produceResults: ProduceResult, projection: Projection)
    extends CodeGenPlan {

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val projectionOpName = context.registerOperator(projection)
      val produceResultOpName = context.registerOperator(produceResults)
      val projections = Eagerly.immutableMapValues(projection.expressions,
        (e: Expression) => ExpressionConverter.createExpression(e)(context))

      (None, AcceptVisitor(produceResultOpName, projectionOpName, projections))
    }

    override def produce(context: CodeGenContext) = {
      context.pushParent(this)
      projection.lhs.get.asCodeGenPlan.produce(context)
    }

    override val logicalPlan: LogicalPlan = produceResults
  }

  private implicit class AllNodesScanCodeGen(allNodesScan: AllNodesScan) {
    def asCodeGenPlan = new CodeGenPlan with LeafCodeGenPlan {
      override val logicalPlan: LogicalPlan = allNodesScan

      override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
        val variable = Variable(context.namer.newVarName(), symbols.CTNode)
        context.addVariable(allNodesScan.idName.name, variable)
        val (methodHandle, actions) = context.popParent().consume(context, this)
        val opName = context.registerOperator(logicalPlan)
        (methodHandle, Seq(WhileLoop(variable, ScanAllNodes(opName), actions)))
      }
    }
  }

  private implicit class NodeByLabelScanCodeGen(nodeByLabelScan: NodeByLabelScan) {
    def asCodeGenPlan = new CodeGenPlan with LeafCodeGenPlan {
      override val logicalPlan: LogicalPlan = nodeByLabelScan

      override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
        val nodeVar = Variable(context.namer.newVarName(), symbols.CTNode)
        val labelVar = context.namer.newVarName()
        context.addVariable(nodeByLabelScan.idName.name, nodeVar)
        val (methodHandle, actions) = context.popParent().consume(context, this)
        val opName = context.registerOperator(logicalPlan)
        (methodHandle, Seq(WhileLoop(nodeVar, ScanForLabel(opName, nodeByLabelScan.label.name, labelVar), actions)))
      }
    }
  }

  private implicit class NodeIndexSeekCodeGen(indexSeek: NodeIndexSeek) {
    def asCodeGenPlan = new CodeGenPlan with LeafCodeGenPlan {

      override val logicalPlan: LogicalPlan = indexSeek

      override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
        val nodeVar = Variable(context.namer.newVarName(), symbols.CTNode)
        context.addVariable(indexSeek.idName.name, nodeVar)

        val (methodHandle, actions) = context.popParent().consume(context, this)
        val opName = context.registerOperator(logicalPlan)
        val indexSeekInstruction = indexSeek.valueExpr match {
          //single expression, do a index lookup for that value
          case SingleQueryExpression(e) =>
            val expression = ExpressionConverter.createExpression(e)(context)
            WhileLoop(nodeVar,
              IndexSeek(opName, indexSeek.label.name, indexSeek.propertyKey.name,
                context.namer.newVarName(), expression), actions)
          //collection, create set and for each element of the set do an index lookup
          case ManyQueryExpression(e: ast.Collection) =>
            val expression = ToSet(ExpressionConverter.createExpression(e)(context))
            val expressionVar = context.namer.newVarName()
            ForEachExpression(expressionVar, expression,
              WhileLoop(nodeVar,
                IndexSeek(opName, indexSeek.label.name, indexSeek.propertyKey.name,
                  context.namer.newVarName(), LoadVariable(expressionVar)), actions))
          //Unknown, try to cast to collection and then same as above
          case ManyQueryExpression(e) =>
            val expression = ToSet(CastToCollection(ExpressionConverter.createExpression(e)(context)))
            val expressionVar = context.namer.newVarName()
            ForEachExpression(expressionVar, expression,
              WhileLoop(nodeVar,
                IndexSeek(opName, indexSeek.label.name, indexSeek.propertyKey.name,
                  context.namer.newVarName(), LoadVariable(expressionVar)), actions))

          case e => throw new InternalException(s"$e is not a valid QueryExpression")
        }

        (methodHandle, Seq(indexSeekInstruction))
      }
    }
  }

  private implicit class NodeHashJoinCodeGen(nodeHashJoin: NodeHashJoin) {
    def asCodeGenPlan = new CodeGenPlan {

      if (nodeHashJoin.nodes.size > 1)
        throw new CantCompileQueryException("Joining on multiple nodes is not yet supported in compiled runtim")

      override val logicalPlan: LogicalPlan = nodeHashJoin

      override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
        context.pushParent(this)
        val (Some(symbol), leftInstructions) = logicalPlan.lhs.get.asCodeGenPlan.produce(context)
        val opName = context.registerOperator(logicalPlan)
        val lhsMethod = MethodInvocation(Set(opName), symbol, context.namer.newMethodName(), leftInstructions)

        context.pushParent(this)
        val (otherSymbol, rightInstructions) = logicalPlan.rhs.get.asCodeGenPlan.produce(context)
        (otherSymbol, lhsMethod +: rightInstructions)
      }

      override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        if (child.logicalPlan eq logicalPlan.lhs.get) {

          val nodeId = context.getVariable(nodeHashJoin.nodes.head.name)
          val probeTableName = context.namer.newVarName()

          val lhsSymbols = nodeHashJoin.left.availableSymbols.map(_.name)
          val nodeNames = nodeHashJoin.nodes.map(_.name)
          val notNodeSymbols = lhsSymbols intersect context.variableNames() diff nodeNames
          val symbols = notNodeSymbols.map(s => s -> context.getVariable(s)).toMap

          val opName = context.registerOperator(nodeHashJoin)
          val probeTable = BuildProbeTable(opName, probeTableName, nodeId, symbols)(context)
          val probeTableSymbol = JoinTableMethod(probeTableName, probeTable.tableType)

          context.addProbeTable(this, probeTable.joinData)

          (Some(probeTableSymbol), probeTable)

        }
        else if (child.logicalPlan eq logicalPlan.rhs.get) {

          val nodeId = context.getVariable(nodeHashJoin.nodes.head.name)
          val joinData = context.getProbeTable(this)
          joinData.vars foreach { case (_, symbol) => context.addVariable(symbol.identifier, symbol.outgoing) }

          val (methodHandle, actions) = context.popParent().consume(context, this)

          (methodHandle, GetMatchesFromProbeTable(nodeId, joinData, actions))
        }
        else {
          throw new InternalException(s"Unexpected consume call by $child")
        }
      }
    }
  }

  private implicit class ExpandCodeGen(expand: Expand) {
    def asCodeGenPlan = new CodeGenPlan with SingleChildPlan {

      override val logicalPlan: LogicalPlan = expand

      override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        if (expand.mode == ExpandAll)
          expandAllConsume(context, child)
        else
          expandIntoConsume(context, child)
      }

      private def expandAllConsume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        val relVar = Variable(context.namer.newVarName(), symbols.CTRelationship)
        val toNodeVar = Variable(context.namer.newVarName(), symbols.CTNode)
        context.addVariable(expand.relName.name, relVar)
        context.addVariable(expand.to.name, toNodeVar)

        val (methodHandle, action) = context.popParent().consume(context, this)
        val fromNodeVar = context.getVariable(expand.from.name)
        val typeVar2TypeName = expand.types.map(t => context.namer.newVarName() -> t.name).toMap
        val opName = context.registerOperator(expand)
        val expandGenerator = ExpandAllLoopDataGenerator(opName, fromNodeVar, expand.dir, typeVar2TypeName, toNodeVar)

        (methodHandle, WhileLoop(relVar, expandGenerator, action))
      }

      private def expandIntoConsume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        val relVar = Variable(context.namer.newVarName(), symbols.CTRelationship)
        context.addVariable(expand.relName.name, relVar)

        val (methodHandle, action) = context.popParent().consume(context, this)
        val fromNodeVar = context.getVariable(expand.from.name)
        val toNodeVar = context.getVariable(expand.to.name)
        val typeVar2TypeName = expand.types.map(t => context.namer.newVarName() -> t.name).toMap
        val opName = context.registerOperator(expand)
        val expandGenerator = ExpandIntoLoopDataGenerator(opName, fromNodeVar, expand.dir, typeVar2TypeName, toNodeVar)

        (methodHandle, WhileLoop(relVar, expandGenerator, action))
      }
    }
  }

  private implicit class OptionalExpandCodeGen(optionalExpand: OptionalExpand) {
    def asCodeGenPlan = new CodeGenPlan {

      if (optionalExpand.mode != ExpandAll) {
        throw new CantCompileQueryException(s"OptionalExpand ${optionalExpand.mode} not yet supported")
      }

      override val logicalPlan: LogicalPlan = optionalExpand

      override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
        context.pushParent(this)
        optionalExpand.lhs.get.asCodeGenPlan.produce(context)
      }

      override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        //mark relationship and node to visit as nullable
        val relVar = Variable(context.namer.newVarName(), symbols.CTRelationship, nullable = true)
        val toNodeVar = Variable(context.namer.newVarName(), symbols.CTNode, nullable = true)
        context.addVariable(optionalExpand.relName.name, relVar)
        context.addVariable(optionalExpand.to.name, toNodeVar)

        val (methodHandle, action) = context.popParent().consume(context, this)
        val fromNodeVar = context.getVariable(optionalExpand.from.name)
        val typeVar2TypeName = optionalExpand.types.map(t => context.namer.newVarName() -> t.name).toMap
        val opName = context.registerOperator(optionalExpand)

        //name of flag to check if results were yielded
        val yieldFlag = context.namer.newVarName()

        //wrap inner instructions with predicates
        val instructionWithPredicates = optionalExpand.predicates
          .reverseMap(ExpressionConverter.createPredicate(_)(context)).foldLeft[Instruction](CheckingInstruction(action, yieldFlag)) {
          case (acc, predicate) => If(predicate, acc)
        }

        val expand = ExpandAllLoopDataGenerator(opName, fromNodeVar, optionalExpand.dir, typeVar2TypeName, toNodeVar)

        val loop = WhileLoop(relVar, expand, instructionWithPredicates)

        (methodHandle, NullingInstruction(loop, yieldFlag, action, relVar, toNodeVar))
      }
    }
  }

  private implicit class CartesianProductGen(cartesianProduct: CartesianProduct) {
    def asCodeGenPlan = new CodeGenPlan {

      override val logicalPlan: LogicalPlan = cartesianProduct

      override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
        context.pushParent(this)
        cartesianProduct.lhs.get.asCodeGenPlan.produce(context)
      }

      override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        if (child.logicalPlan eq cartesianProduct.lhs.get) {
          context.pushParent(this)
          val (m, actions) = cartesianProduct.rhs.get.asCodeGenPlan.produce(context)
          (m, actions.headOption.getOrElse(throw new InternalException("Illegal call chain")))
        } else if (child.logicalPlan eq cartesianProduct.rhs.get) {
          val opName = context.registerOperator(cartesianProduct)
          val (m, instruction) = context.popParent().consume(context, this)
          (m, TracingInstruction(opName, instruction))
        }
        else {
          throw new InternalException(s"Unexpected consume call by $child")
        }
      }
    }
  }

  private implicit class SelectionCodeGen(selection: Selection) {
    def asCodeGenPlan = new CodeGenPlan with SingleChildPlan {

      override val logicalPlan: LogicalPlan = selection

      override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        val opName = context.registerOperator(selection)
        val predicates = selection.predicates.map(
          ExpressionConverter.createPredicate(_)(context)
        )

        val (methodHandle, innerBlock) = context.popParent().consume(context, this)

        val instruction = predicates.reverse.foldLeft[Instruction](innerBlock) {
          case (acc, predicate) => If(predicate, acc)
        }

        (methodHandle, TracingInstruction(opName, instruction))
      }
    }
  }

  private implicit class Limit(limit: plans.Limit) {
    def asCodeGenPlan = new CodeGenPlan with SingleChildPlan {

      override val logicalPlan: LogicalPlan = limit

      override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
        val opName = context.registerOperator(limit)
        val count = ExpressionConverter.createExpression(limit.count)(context)
        val counterName = context.namer.newVarName()

        val (methodHandle, innerBlock) = context.popParent().consume(context, this)
        val instruction = DecreaseAndReturnWhenZero(opName, counterName, innerBlock, count)

        (methodHandle, instruction)
      }
    }
  }

  trait SingleChildPlan extends CodeGenPlan {

    final override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }
  }

}
