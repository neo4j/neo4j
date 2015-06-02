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

import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.{InternalException, ast}

object LogicalPlanConverter {

  implicit class LogicalPlan2CodeGenPlan(val logicalPlan: LogicalPlan) {

    def asCodeGenPlan: CodeGenPlan = logicalPlan match {
      case p: SingleRow => p
      case p: AllNodesScan => p
      case p: NodeByLabelScan => p
      case p: ProduceResult => p
      case p: Expand => p
      case p: NodeHashJoin => p
      case p: CartesianProduct => p
      case p: Projection => p
      case p: Selection => p

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
      val variable = context.namer.newVarName()
      context.addVariable(logicalPlan.idName.name, variable)
      val (methodHandle, actions) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, Seq(WhileLoop(variable, ScanAllNodes(opName), actions)))
    }
  }

  private implicit class NodeByLabelScanCodeGen(val logicalPlan: NodeByLabelScan) extends LeafCodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      val nodeVar = context.namer.newVarName()
      val labelVar = context.namer.newVarName()
      context.addVariable(logicalPlan.idName.name, nodeVar)
      val (methodHandle, actions) = context.popParent().consume(context, this)
      val opName = context.registerOperator(logicalPlan)
      (methodHandle, Seq(WhileLoop(nodeVar, ScanForLabel(opName, logicalPlan.label.name, labelVar), actions)))
    }
  }

  private implicit class NodeHashJoinCodeGen(val logicalPlan: NodeHashJoin) extends CodeGenPlan {

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
        val thunk = context.getProbeTable(this)
        thunk.vars foreach { case (name, symbol) => context.addVariable(name, symbol) }

        val (methodHandle, actions) = context.popParent().consume(context, this)

        (methodHandle, GetMatchesFromProbeTable(nodeId, thunk, actions))
      }
      else {
        throw new InternalException(s"Unexpected consume call by $child")
      }
    }
  }

  private implicit class ProduceResultCodeGen(val logicalPlan: ProduceResult) extends CodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val nodeVars = logicalPlan.nodes.map(n => n -> context.getProjection(n))
      val relVars = logicalPlan.relationships.map(r => r -> context.getProjection(r))
      val otherVars = logicalPlan.other.map(o => o -> context.getProjection(o))
      val opName = context.registerOperator(logicalPlan)
      (None, AcceptVisitor(opName, nodeVars.toMap ++ relVars.toMap ++ otherVars.toMap))
    }
  }

  private implicit class ExpandCodeGen(val logicalPlan: Expand) extends CodeGenPlan {

    if (logicalPlan.mode != ExpandAll) {
      throw new CantCompileQueryException(s"Expand ${logicalPlan.mode} not yet supported")
    }

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val relVar = context.namer.newVarName()
      val toNodeVar = context.namer.newVarName()
      context.addVariable(logicalPlan.relName.name, relVar)
      context.addVariable(logicalPlan.to.name, toNodeVar)

      val (methodHandle, action) = context.popParent().consume(context, this)
      val fromNodeVar = context.getVariable(logicalPlan.from.name)
      val typeVar2TypeName = logicalPlan.types.map(t => context.namer.newVarName() -> t.name).toMap
      val opName = context.registerOperator(logicalPlan)
      val expand = ExpandC(opName, fromNodeVar, relVar, logicalPlan.dir, typeVar2TypeName, toNodeVar, Instruction.empty)
      (methodHandle, WhileLoop(relVar, expand, action))
    }
  }

  private implicit class CartestianProductGen(val logicalPlan: CartesianProduct) extends CodeGenPlan {

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

  private implicit class ProjectionCodeGen(val logicalPlan: Projection) extends CodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

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

  private implicit class SelectionCodeGen(val logicalPlan: Selection) extends CodeGenPlan {

    override def produce(context: CodeGenContext): (Option[JoinTableMethod], Seq[Instruction]) = {
      context.pushParent(this)
      logicalPlan.lhs.get.asCodeGenPlan.produce(context)
    }

    override def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], Instruction) = {
      val opName = context.registerOperator(logicalPlan)
      val predicates = logicalPlan.predicates.map {
        case expression =>
          ExpressionConverter.createPredicate(expression)(opName, context)

        case x => throw new CantCompileQueryException(x.toString)
      }

      val (methodHandle, innerBlock) = context.popParent().consume(context, this)

      val instruction = predicates.reverse.foldLeft[Instruction](TracingInstruction(opName, innerBlock)) {
        case (acc, predicate) => If(predicate, acc)
      }

      (methodHandle, instruction)
    }
  }
}

object ExpressionConverter {
  def createPredicate(expression: Expression)
                     (implicit opName: String, context: CodeGenContext): CodeGenExpression = expression match {
    case expression: HasLabels =>
      createExpression(expression)

    case exp@Property(node@Identifier(name), propKey) if context.semanticTable.isNode(node) =>
      PropertyAsPredicate(createExpression(exp))

    case exp@Property(node@Identifier(name), propKey) if context.semanticTable.isRelationship(node) =>
      PropertyAsPredicate(createExpression(exp))

    case other =>
      throw new CantCompileQueryException(s"Predicate of $other not yet supported")

  }

  def createExpression(expression: Expression)
                      (implicit opName: String, context: CodeGenContext): CodeGenExpression = {

    expression match {
      case node@Identifier(name) if context.semanticTable.isNode(node) =>
        Node(context.getVariable(name))

      case rel@Identifier(name) if context.semanticTable.isRelationship(rel) =>
        Relationship(context.getVariable(name))

      case Property(node@Identifier(name), propKey) if context.semanticTable.isNode(node) =>
        val token = propKey.id(context.semanticTable).map(_.id)
        NodeProperty(opName, token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case Property(rel@Identifier(name), propKey) if context.semanticTable.isRelationship(rel) =>
        val token = propKey.id(context.semanticTable).map(_.id)
        RelProperty(opName, token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case ast.Parameter(name) => expressions.Parameter(name)

      case lit: IntegerLiteral => Literal(lit.value)

      case lit: DoubleLiteral => Literal(lit.value)

      case lit: StringLiteral => Literal(lit.value)

      case lit: ast.Literal => Literal(lit.value)

      case ast.Collection(exprs) =>
        expressions.Collection(exprs.map(e => createExpression(e)))

      case Add(lhs, rhs) =>
        val leftOp = createExpression(lhs)
        val rightOp = createExpression(rhs)
        Addition(leftOp, rightOp)

      case Subtract(lhs, rhs) =>
        val leftOp = createExpression(lhs)
        val rightOp = createExpression(rhs)
        Subtraction(leftOp, rightOp)

      case MapExpression(items: Seq[(PropertyKeyName, Expression)]) =>
        val map = items.map {
          case (key, expr) => (key.name, createExpression(expr))
        }.toMap
        MyMap(map)

      case HasLabels(Identifier(name), label :: Nil) =>
        val labelIdVariable = context.namer.newVarName()
        val nodeVariable = context.getVariable(name)
        HasLabel(opName, nodeVariable, labelIdVariable, label.name)

      case other => throw new CantCompileQueryException(s"Expression of $other not yet supported")
    }
  }
}
