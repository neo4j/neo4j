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

import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.{ast, InternalException, symbols}

object LogicalPlanConverter {

  implicit class LogicalPlan2CodeGenPlan(val logicalPlan: LogicalPlan) {

    def asCodeGenPlan: CodeGenPlan = logicalPlan match {
      case p: SingleRow => p
      case p: AllNodesScan => p
      case p: NodeByLabelScan => p
      case p: NodeIndexSeek => p
      case p: ProduceResult => p
      case p: Expand => p
      case p: OptionalExpand => p
      case p: NodeHashJoin => p
      case p: CartesianProduct => p
      case p: Projection => p
      case p: Selection => p
      case p: plans.Limit => p

      case _ =>
        throw new CantCompileQueryException(s"$logicalPlan is not yet supported")
    }
  }

  private implicit class SingleRowCodeGen(val logicalPlan: SingleRow) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      val (methodHandle, actions) = context.popParent().consume(context, this)
      (methodHandle, Seq(actions))
    }
  }

  private implicit class AllNodesScanCodeGen(val logicalPlan: AllNodesScan) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      val variable = Variable(context.namer.newVarName(), symbols.CTNode)
      context.addVariable(logicalPlan.idName.name, variable)
      val (methodHandle, actions) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, Seq(WhileLoop(variable, ScanAllNodes(opName), actions)))
    }
  }

  private implicit class NodeByLabelScanCodeGen(val logicalPlan: NodeByLabelScan) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      val nodeVar = Variable(context.namer.newVarName(), symbols.CTNode)
      val labelVar = context.namer.newVarName()
      context.addVariable(logicalPlan.idName.name, nodeVar)
      val (methodHandle, actions) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, Seq(WhileLoop(nodeVar, ScanForLabel(opName, logicalPlan.label.name, labelVar), actions)))
    }
  }

  private implicit class NodeIndexSeekCodeGen(val logicalPlan: NodeIndexSeek) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      val nodeVar = Variable(context.namer.newVarName(), symbols.CTNode)
      context.addVariable(logicalPlan.idName.name, nodeVar)

      val (methodHandle, actions) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      val indexSeekInstruction = logicalPlan.valueExpr match {
        //single expression, do a index lookup for that value
        case SingleQueryExpression(e) =>
          val expression = ExpressionConverter.createExpression(e)(opName, context)
          WhileLoop(nodeVar,
                    IndexSeek(opName, logicalPlan.label.name, logicalPlan.propertyKey.name,
                              context.namer.newVarName(), expression), actions)
        //collection, create set and for each element of the set do an index lookup
        case ManyQueryExpression(e: ast.Collection) =>
          val expression = ToSet(ExpressionConverter.createExpression(e)(opName, context))
          val expressionVar = context.namer.newVarName()
          ForEachExpression(expressionVar, expression,
                            WhileLoop(nodeVar,
                                      IndexSeek(opName, logicalPlan.label.name, logicalPlan.propertyKey.name,
                                                context.namer.newVarName(), LoadVariable(expressionVar)), actions))
        //Unknown, try to cast to collection and then same as above
        case ManyQueryExpression(e) =>
          val expression = ToSet(CastToCollection(ExpressionConverter.createExpression(e)(opName, context)))
          val expressionVar = context.namer.newVarName()
          ForEachExpression(expressionVar, expression,
                            WhileLoop(nodeVar,
                                      IndexSeek(opName, logicalPlan.label.name, logicalPlan.propertyKey.name,
                                                context.namer.newVarName(), LoadVariable(expressionVar)), actions))

         case e => throw new InternalException(s"$e is not a valid QueryExpression")
        }

      (methodHandle, Seq(indexSeekInstruction))
    }
  }

  private implicit class NodeHashJoinCodeGen(val logicalPlan: NodeHashJoin) extends CodeGenPlan {

    if (logicalPlan.nodes.size > 1)
      throw new CantCompileQueryException("Joining on multiple nodes is not yet supported in compiled runtim")

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      val (Some(symbol), leftInstructions) = logicalPlan.lhs.get.asCodeGenPlan.produce(context)
      val opName = context.registerOperator(logicalPlan)
      val lhsMethod = MethodInvocation(Some(opName), symbol, context.namer.newMethodName(), leftInstructions)

      context.pushParent(this)
      val (otherSymbol, rightInstructions) = logicalPlan.rhs.get.asCodeGenPlan.produce(context)
      (otherSymbol, lhsMethod +: rightInstructions)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      if (child.logicalPlan eq logicalPlan.lhs.get) {

        val nodeId = context.getVariable(logicalPlan.nodes.head.name)
        val probeTableName = context.namer.newVarName()

        val lhsSymbols = logicalPlan.lhs.get.availableSymbols.map(_.name)
        val nodeNames = logicalPlan.nodes.map(_.name)
        val notNodeSymbols = lhsSymbols intersect context.variableNames() diff nodeNames
        val symbols = notNodeSymbols.map(s => s -> context.getVariable(s)).toMap

        val opName = context.registerOperator(logicalPlan)
        val probeTable = BuildProbeTable(opName, probeTableName, nodeId, symbols)(context)
        val probeTableSymbol = JoinTableMethod(probeTableName, probeTable.tableType)

        context.addProbeTable(this, probeTable.joinData)

        (Some(probeTableSymbol), probeTable)

      }
      else if (child.logicalPlan eq logicalPlan.rhs.get) {

        val nodeId = context.getVariable(logicalPlan.nodes.head.name)
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

  trait SingleChildPlan extends CodeGenPlan {

    final override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }
  }

  private implicit class ProduceResultCodeGen(val logicalPlan: ProduceResult) extends SingleChildPlan {
    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val nodeVars = logicalPlan.nodes.map(n => n -> context.getProjection(n))
      val relVars = logicalPlan.relationships.map(r => r -> context.getProjection(r))
      val otherVars = logicalPlan.other.map(o => o -> context.getProjection(o))
      val opName = context.registerOperator(logicalPlan)
      (None, AcceptVisitor(opName, nodeVars.toMap ++ relVars.toMap ++ otherVars.toMap))
    }
  }

  private implicit class ExpandCodeGen(val logicalPlan: Expand) extends SingleChildPlan {

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      if (logicalPlan.mode == ExpandAll)
        expandAllConsume(context, child)
      else
        expandIntoConsume(context, child)
    }

    private def expandAllConsume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val relVar = Variable(context.namer.newVarName(), symbols.CTRelationship)
      val toNodeVar = Variable(context.namer.newVarName(), symbols.CTNode)
      context.addVariable(logicalPlan.relName.name, relVar)
      context.addVariable(logicalPlan.to.name, toNodeVar)

      val (methodHandle, action) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(logicalPlan.from.name)
      val typeVar2TypeName = logicalPlan.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(logicalPlan)
      val expand = ExpandAllLoopDataGenerator(opName, fromNodeVar, logicalPlan.dir, typeVar2TypeName, toNodeVar)

      (methodHandle, WhileLoop(relVar, expand, action))
    }

    private def expandIntoConsume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val relVar = Variable(context.namer.newVarName(), symbols.CTRelationship)
      context.addVariable(logicalPlan.relName.name, relVar)

      val (methodHandle, action) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(logicalPlan.from.name)
      val toNodeVar = context.getVariable(logicalPlan.to.name)
      val typeVar2TypeName = logicalPlan.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(logicalPlan)
      val expand = ExpandIntoLoopDataGenerator(opName, fromNodeVar, logicalPlan.dir, typeVar2TypeName, toNodeVar)

      (methodHandle, WhileLoop(relVar, expand, action))
    }
  }


  private implicit class OptionalExpandCodeGen(val logicalPlan: OptionalExpand) extends CodeGenPlan {

    if (logicalPlan.mode != ExpandAll) {
      throw new CantCompileQueryException(s"OptionalExpand ${logicalPlan.mode} not yet supported")
    }

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      //mark relationship and node to visit as nullable
      val relVar = Variable(context.namer.newVarName(), symbols.CTRelationship, nullable = true)
      val toNodeVar = Variable(context.namer.newVarName(), symbols.CTNode, nullable = true)
      context.addVariable(logicalPlan.relName.name, relVar)
      context.addVariable(logicalPlan.to.name, toNodeVar)

      val (methodHandle, action) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(logicalPlan.from.name)
      val typeVar2TypeName = logicalPlan.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(logicalPlan)

      //wrap inner instructions with predicates
      val instructionWithPredicates = logicalPlan.predicates
        .reverseMap(ExpressionConverter.createExpression(_)(opName, context)).foldLeft[Instruction](action) {
        case (acc, predicate) => If(predicate, acc)
      }
      //name of flag to check if results were yielded
      val yieldFlag = context.namer.newVarName()

      val expand = ExpandAllLoopDataGenerator(opName, fromNodeVar, logicalPlan.dir, typeVar2TypeName, toNodeVar)

      val dataGenerator = CheckingLoopDataGenerator(expand, yieldFlag)
      val loop = WhileLoop(relVar, dataGenerator, instructionWithPredicates)

      (methodHandle, NullingWhileLoop(loop, yieldFlag, relVar, toNodeVar))
    }
  }

  private implicit class CartesianProductGen(val logicalPlan: CartesianProduct) extends CodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      if (child.logicalPlan eq logicalPlan.lhs.get) {
        context.pushParent(this)
        val (m, actions) = logicalPlan.rhs.get.asCodeGenPlan.produce(context)
        (m, actions.headOption.getOrElse(throw new InternalException("Illegal call chain")))
      } else if (child.logicalPlan eq logicalPlan.rhs.get) {
        val opName = context.registerOperator(logicalPlan)
        val (m, instruction) = context.popParent().consume(context, this)
        (m, TracingInstruction(opName, instruction))
      }
      else {
        throw new InternalException(s"Unexpected consume call by $child")
      }
    }
  }

  private implicit class ProjectionCodeGen(val logicalPlan: Projection) extends SingleChildPlan {

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val opName = context.registerOperator(logicalPlan)
      val projectionInstructions = logicalPlan.expressions.map {
        case (identifier, expression) =>
          val instruction = ExpressionConverter.createExpression(expression)(opName, context)

          context.addProjection(identifier, instruction)
          instruction
      }.toSeq

      val (methodHandle, action) = context.popParent().consume(context, this)

      (methodHandle, Project(opName, projectionInstructions, action))
    }
  }

  private implicit class SelectionCodeGen(val logicalPlan: Selection) extends SingleChildPlan {

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val opName = context.registerOperator(logicalPlan)
      val predicates = logicalPlan.predicates.map(
        ExpressionConverter.createPredicate(_)(opName, context)
      )

      val (methodHandle, innerBlock) = context.popParent().consume(context, this)

      val instruction = predicates.reverse.foldLeft[Instruction](TracingInstruction(opName, innerBlock)) {
        case (acc, predicate) => If(predicate, acc)
      }

      (methodHandle, instruction)
    }
  }

  private implicit class Limit(val logicalPlan: plans.Limit) extends SingleChildPlan {

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val opName = context.registerOperator(logicalPlan)
      val count = ExpressionConverter.createExpression(logicalPlan.count)(opName, context)
      val counterName = context.namer.newVarName()

      val (methodHandle, innerBlock) = context.popParent().consume(context, this)
      val instruction = DecreaseAndReturnWhenZero(opName, counterName, innerBlock, count)

      (methodHandle, instruction)
    }
  }
}
