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
package org.neo4j.cypher.internal.compiler.v2_3.birk.codegen

import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator.JavaTypes._
import org.neo4j.cypher.internal.compiler.v2_3.birk.JavaSymbol
import org.neo4j.cypher.internal.compiler.v2_3.birk.il._
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.helpers.ThisShouldNotHappenError

object LogicalPlanConverter {

  implicit class LogicalPlan2CodeGenPlan(val logicalPlan: LogicalPlan) {

    def asCodeGenPlan: CodeGenPlan = logicalPlan match {
      case p: SingleRow => p
      case p: AllNodesScan => p
      case p: NodeByLabelScan => p
      case p: ProduceResult => p
      case p: Expand => p
      case p: NodeHashJoin => p
      case p: Projection => p

      case _ =>
        throw new CantCompileQueryException(s"$logicalPlan is not yet supported")
    }
  }

  private implicit class SingleRowCodeGen(val logicalPlan: SingleRow) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JavaSymbol], Seq[Instruction]) = {
      val (methodHandle, actions) = context.popParent().consume(context, this)
      (methodHandle, Seq(actions))
    }
  }

  private implicit class AllNodesScanCodeGen(val logicalPlan: AllNodesScan) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JavaSymbol], Seq[Instruction]) = {
      val variable = JavaSymbol(context.namer.newVarName(), LONG)
      context.addVariable(logicalPlan.idName.name, variable)
      val (methodHandle, actions) = context.popParent().consume(context, this)
      (methodHandle, Seq(WhileLoop(variable, ScanAllNodes(), actions)))
    }
  }

  private implicit class NodeByLabelScanCodeGen(val logicalPlan: NodeByLabelScan) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JavaSymbol], Seq[Instruction]) = {
      val nodeVar = JavaSymbol(context.namer.newVarName(), LONG)
      val labelVar = JavaSymbol(context.namer.newVarName(), INT)
      context.addVariable(logicalPlan.idName.name, nodeVar)
      val (methodHandle, actions) = context.popParent().consume(context, this)
      (methodHandle, Seq(WhileLoop(nodeVar, ScanForLabel(logicalPlan.label.name, labelVar), actions)))
    }
  }

  private implicit class NodeHashJoinCodeGen(val logicalPlan: NodeHashJoin) extends CodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JavaSymbol], Seq[Instruction]) = {
      context.pushParent(this)
      val (Some(symbol), leftInstructions) = logicalPlan.lhs.get.asCodeGenPlan.produce(context)
      val lhsMethod = MethodInvocation(symbol.name, symbol.javaType, context.namer.newMethodName(), leftInstructions)

      context.pushParent(this)
      val (otherSymbol, rightInstructions) = logicalPlan.rhs.get.asCodeGenPlan.produce(context)
      (otherSymbol, lhsMethod +: rightInstructions)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JavaSymbol], Instruction) = {
      if (child.logicalPlan eq logicalPlan.lhs.get) {

        val nodeId = context.getVariable(logicalPlan.nodes.head.name)
        val probeTableName = context.namer.newVarName()

        val lhsSymbols = logicalPlan.lhs.get.availableSymbols.map(_.name)
        val nodeNames = logicalPlan.nodes.map(_.name)
        val notNodeSymbols = lhsSymbols intersect context.variableNames() diff nodeNames
        val symbols = notNodeSymbols.map(s => s -> context.getVariable(s)).toMap

        val probeTable = BuildProbeTable(probeTableName, nodeId.name, symbols, context.namer)
        val probeTableSymbol = JavaSymbol(probeTableName, probeTable.producedType)

        context.addProbeTable(this, probeTable.generateFetchCode)

        (Some(probeTableSymbol), probeTable)

      }
      else if (child.logicalPlan eq logicalPlan.rhs.get) {

        val nodeId = context.getVariable(logicalPlan.nodes.head.name)
        val thunk = context.getProbeTable(this)
        thunk.vars foreach { case (name, symbol) => context.addVariable(name, symbol) }

        val (methodHandle, actions) = context.popParent().consume(context, this)

        (methodHandle, GetMatchesFromProbeTable(nodeId.name, thunk, actions))
      }
      else {

        throw new ThisShouldNotHappenError("lutovich", s"Unexpected consume call by $child")
      }
    }
  }

  private implicit class ProduceResultCodeGen(val logicalPlan: ProduceResult) extends CodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JavaSymbol], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JavaSymbol], Instruction) = {
      val nodeVars = logicalPlan.nodes.map(n => n -> context.getVariable(n).name)
      val relVars = logicalPlan.relationships.map(r => r -> context.getVariable(r).name)
      val otherVars = logicalPlan.other.map(o => o -> context.getVariable(o).name)
      (None, ProduceResults(nodeVars.toMap, relVars.toMap, otherVars.toMap))
    }
  }

  private implicit class ExpandCodeGen(val logicalPlan: Expand) extends CodeGenPlan {

    if (logicalPlan.mode != ExpandAll) {
      throw new CantCompileQueryException(s"Expand ${logicalPlan.mode} not yet supported")
    }

    override def produce(context: CodeGenContext): (Option[JavaSymbol], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JavaSymbol], Instruction) = {
      val relVar = context.namer.newVarName(LONG)
      val toNodeVar = context.namer.newVarName(LONG)
      context.addVariable(logicalPlan.relName.name, relVar)
      context.addVariable(logicalPlan.to.name, toNodeVar)

      val (methodHandle, action) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(logicalPlan.from.name)
      val typeVar2TypeName = logicalPlan.types.map(t => context.namer.newVarName() -> t.name).toMap
      val expand = ExpandC(fromNodeVar.name, relVar.name, logicalPlan.dir, typeVar2TypeName, toNodeVar.name, action)
      (methodHandle, WhileLoop(relVar, expand, Instruction.empty))
    }
  }

  private implicit class ProjectionCodeGen(val logicalPlan: Projection) extends CodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JavaSymbol], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JavaSymbol], Instruction) = {
      val projectionInstructions = logicalPlan.expressions.map {
        case (identifier, expression) =>
          val instruction = createProjectionInstruction(expression, context)
          context.addVariable(identifier, instruction.projectedVariable)
          instruction
      }.toSeq

      val (methodHandle, action) = context.popParent().consume(context, this)

      (methodHandle, ProjectProperties(projectionInstructions, action))
    }

    private def createProjectionInstruction(expression: Expression, context: CodeGenContext): ProjectionInstruction =
      expression match {
        case nodeOrRel@Identifier(name)
          if context.semanticTable.isNode(nodeOrRel) || context.semanticTable.isRelationship(nodeOrRel) =>
          ProjectNodeOrRelationship(context.getVariable(name))

        case Property(node@Identifier(name), propKey) if context.semanticTable.isNode(node) =>
          val token = propKey.id(context.semanticTable).map(_.id)
          ProjectNodeProperty(token, propKey.name, context.getVariable(name).name, context.namer)

        case Property(rel@Identifier(name), propKey) if context.semanticTable.isRelationship(rel) =>
          val token = propKey.id(context.semanticTable).map(_.id)
          ProjectRelProperty(token, propKey.name, context.getVariable(name).name, context.namer)

        case Parameter(name) => ProjectParameter(name)

        case lit: IntegerLiteral =>
          ProjectLiteral(JavaSymbol(s"${lit.value.toString}L", LONG))

        case lit: DoubleLiteral =>
          ProjectLiteral(JavaSymbol(lit.value.toString, DOUBLE))

        case lit: StringLiteral =>
          ProjectLiteral(JavaSymbol( s""""${lit.value}"""", STRING))

        case lit: Literal =>
          ProjectLiteral(JavaSymbol(lit.value.toString, OBJECT))

        case Collection(exprs) =>
          ProjectCollection(exprs.map(e => createProjectionInstruction(e, context)))

        case Add(lhs, rhs) =>
          val leftOp = createProjectionInstruction(lhs, context)
          val rightOp = createProjectionInstruction(rhs, context)
          ProjectAddition(leftOp, rightOp)

        case Subtract(lhs, rhs) =>
          val leftOp = createProjectionInstruction(lhs, context)
          val rightOp = createProjectionInstruction(rhs, context)
          ProjectSubtraction(leftOp, rightOp)

        case other => throw new CantCompileQueryException(s"Projection of $other not yet supported")
      }
  }

}
