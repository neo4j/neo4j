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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.phases.{CompilationState, CompilerContext}
import org.neo4j.cypher.internal.frontend.v3_2.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, FunctionInvocation, _}
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.v3_2.phases.Phase
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, Rewriter, topDown}
import org.neo4j.cypher.internal.ir.v3_2.{RegularPlannerQuery, UnionQuery, _}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.collection.{TraversableOnce, mutable}

case object OptionalMatchRemover extends PlannerQueryRewriter {

  override def description: String = "remove optional match when possible"

  override def postConditions = Set.empty

  override def instance(ignored: CompilerContext): Rewriter = topDown(Rewriter.lift {
    case RegularPlannerQuery(graph, proj@AggregatingQueryProjection(distinctExpressions, aggregations, _), tail)
      if validAggregations(aggregations) =>

      // The variables that are needed by the return/with clause
      val expressionDeps: Set[IdName] =
        (distinctExpressions.values ++ aggregations.values)
          .flatMap(_.dependencies)
          .map(IdName.fromVariable)
          .toSet

      val optionalMatches = graph.optionalMatches.flatMapWithTail {
        (original: QueryGraph, tail: Seq[QueryGraph]) =>

          //The dependencies on an optional match are:
          val allDeps =
          // dependencies from optional matches listed later in the query
            tail.flatMap(g => g.argumentIds ++ g.selections.variableDependencies).toSet ++
              // any dependencies from the next horizon
              expressionDeps --
              // But we don't need to solve variables already present by the non-optional part of the QG
              graph.coveredIds

          val mustInclude = allDeps -- original.argumentIds
          val mustKeep = original.smallestGraphIncluding(mustInclude)

          if (mustKeep.isEmpty)
            // We did not find anything in this OPTIONAL MATCH. Since there are no variable deps from this clause,
          // and it can't change cardinality, it's safe to ignore it
            None
          else {
            val (predicatesForPatterns, remaining) = {
              val elementsToKeep1 = original.smallestGraphIncluding(mustInclude ++ original.argumentIds)
              partitionPredicates(original.selections.predicates, elementsToKeep1)
            }

            val elementsToKeep = {
              val variablesNeededForPredicates =
                remaining.flatMap(expression => expression.dependencies.map(IdName.fromVariable))
              original.smallestGraphIncluding(mustInclude ++ original.argumentIds ++ variablesNeededForPredicates)
            }

            val (patternsToKeep, patternsToFilter) = original.patternRelationships.partition(r => elementsToKeep(r.name))
            val patternNodes = original.patternNodes.filter(elementsToKeep.apply)
            val patternPredicates = patternsToFilter.map(toAst(elementsToKeep, predicatesForPatterns, _))

            val newOptionalGraph = original.
              withPatternRelationships(patternsToKeep).
              withPatternNodes(patternNodes).
              withSelections(Selections.from(remaining) ++ patternPredicates)

            Some(newOptionalGraph)
          }
      }

      val matches = graph.withOptionalMatches(optionalMatches)
      RegularPlannerQuery(matches, horizon = proj, tail = tail)
  })

  private object LabelsAndEquality {
    def empty = new LabelsAndEquality(Seq.empty, Seq.empty)
  }

  private case class LabelsAndEquality(labels: Seq[LabelName], equality: Seq[(PropertyKeyName, Expression)])

  /**
    * This method extracts predicates that need to be part of pattern expressions
    *
    * @param predicates All the original predicates of the QueryGraph
    * @param kept       Set of all variables that should not be moved to pattern expressions
    * @return Map of label and property equality comparisons to move to pattern expressions,
    *         and the set of remaining predicates
    */
  private def partitionPredicates(predicates: Set[Predicate], kept: Set[IdName]): (Map[IdName, LabelsAndEquality], Set[Expression]) = {

    val patternPredicates = mutable.Map.empty[IdName, LabelsAndEquality]
    val predicatesToKeep = mutable.Set.empty[Expression]

    def addLabel(idName: IdName, labelName: LabelName) = {
      val current = patternPredicates.getOrElse(idName, LabelsAndEquality.empty)
      patternPredicates += idName -> current.copy(labels = current.labels :+ labelName)
    }

    def addProperty(idName: IdName, prop: PropertyKeyName, rhs: Expression) = {
      val current = patternPredicates.getOrElse(idName, LabelsAndEquality.empty)
      patternPredicates += idName -> current.copy(equality = current.equality :+ prop -> rhs)
    }

    predicates.foreach {
      case Predicate(deps, HasLabels(Variable(v), labels)) if deps.size == 1 && !kept(deps.head) =>
        assert(labels.size == 1) // We know there is only a single label here because AST rewriting
        addLabel(deps.head, labels.head)

      case Predicate(deps, Equals(Property(Variable(v), prop), rhs)) if deps.size == 1 && !kept(deps.head) =>
        addProperty(deps.head, prop, rhs)

      case Predicate(_, expr) =>
        predicatesToKeep += expr
    }

    (patternPredicates.toMap, predicatesToKeep.toSet)
  }

  private def validAggregations(aggregations: Map[String, Expression]) =
    aggregations.isEmpty ||
      aggregations.values.forall {
        case func: FunctionInvocation => func.distinct
        case _ => false
      }

  private def toAst(elementsToKeep: Set[IdName], predicates: Map[IdName, LabelsAndEquality], pattern: PatternRelationship) = {
    val pos = InputPosition.NONE
    def createVariable(name: IdName): Option[Variable] =
      if (!elementsToKeep(name))
        None
      else {
        Some(Variable(name.name)(pos))
      }

    def createNode(name: IdName): NodePattern = {
      val labelsAndProps = predicates.getOrElse(name, LabelsAndEquality.empty)
      val props = if (labelsAndProps.equality.isEmpty) None else Some(MapExpression(labelsAndProps.equality)(pos))
      NodePattern(createVariable(name), labels = labelsAndProps.labels, properties = props)(pos)
    }

    val relName = createVariable(pattern.name)
    val leftNode = createNode(pattern.nodes._1)
    val rightNode = createNode(pattern.nodes._2)
    val relPattern = RelationshipPattern(relName, pattern.types, length = None, properties = None, pattern.dir)(pos)
    val chain = RelationshipChain(leftNode, relPattern, rightNode)(pos)
    PatternExpression(RelationshipsPattern(chain)(pos))
  }

  implicit class FlatMapWithTailable(in: IndexedSeq[QueryGraph]) {
    def flatMapWithTail(f: (QueryGraph, Seq[QueryGraph]) => TraversableOnce[QueryGraph]): IndexedSeq[QueryGraph] = {

      @tailrec
      def recurse(that: QueryGraph, rest: Seq[QueryGraph], builder: mutable.Builder[QueryGraph, ListBuffer[QueryGraph]]): Unit = {
        builder ++= f(that, rest)
        if (rest.nonEmpty)
          recurse(rest.head, rest.tail, builder)
      }
      if (in.isEmpty)
        IndexedSeq.empty
      else {
        val builder = ListBuffer.newBuilder[QueryGraph]
        recurse(in.head, in.tail, builder)
        builder.result().toIndexedSeq
      }
    }
  }

}

trait PlannerQueryRewriter extends Phase[CompilerContext, CompilationState, CompilationState] {
  override def phase: CompilationPhase = LOGICAL_PLANNING

  def instance(context: CompilerContext): Rewriter

  override def process(from: CompilationState, context: CompilerContext): CompilationState = {
    val query: UnionQuery = from.unionQuery
    val rewritten = query.endoRewrite(instance(context))
    from.copy(maybeUnionQuery = Some(rewritten))
  }
}
