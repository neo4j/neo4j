/**
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_1.{Rewriter, topDown}
import org.neo4j.cypher.internal.compiler.v2_1.helpers.UnNamedNameGenerator._
import org.neo4j.cypher.InternalException

object SimplePlannerQueryBuilder {

  object SubQueryExtraction {

    val normalizer = MatchPredicateNormalizerChain(PropertyPredicateNormalizer, LabelPredicateNormalizer)

    def extractQueryGraph(exp: PatternExpression): QueryGraph = {
      val relChain: RelationshipChain = exp.pattern.element
      val predicates: Vector[Expression] = relChain.fold(Vector.empty[Expression]) {
        case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
        case _ => identity
      }

      val rewrittenChain = relChain.endoRewrite(topDown(Rewriter.lift(normalizer.replace)))

      val (patternNodes, relationships) = PatternDestructuring.destruct(rewrittenChain)
      val qg = QueryGraph(
        patternRelationships = relationships.toSet,
        patternNodes = patternNodes.toSet
      ).addPredicates(predicates: _*)
      qg.addArgumentId(qg.coveredIds.filter(_.name.isNamed).toSeq)
    }
  }

  object PatternDestructuring {
    def destruct(pattern: Pattern): (Seq[IdName], Seq[PatternRelationship], Seq[ShortestPathPattern]) =
      pattern.patternParts.foldLeft((Seq.empty[IdName], Seq.empty[PatternRelationship], Seq.empty[ShortestPathPattern])) {
        case ((accIdNames, accRels, accShortest), NamedPatternPart(ident, sps @ ShortestPaths(element, single))) =>
          val (idNames, rels) = destruct(element)
          val pathName = IdName(ident.name)
          val newShortest = ShortestPathPattern(Some(pathName), rels.head, single)(sps)
          (accIdNames ++ idNames , accRels, accShortest ++ Seq(newShortest))

        case ((accIdNames, accRels, accShortest), sps @ ShortestPaths(element, single)) =>
          val (idNames, rels) = destruct(element)
          val newShortest = ShortestPathPattern(None, rels.head, single)(sps)
          (accIdNames ++ idNames , accRels, accShortest ++ Seq(newShortest))

        case ((accIdNames, accRels, accShortest), everyPath: EveryPath) =>
          val (idNames, rels) = destruct(everyPath.element)
          (accIdNames ++ idNames, accRels ++ rels, accShortest)

        case _ =>
          throw new CantHandleQueryException
      }

    private def destruct(element: PatternElement): (Seq[IdName], Seq[PatternRelationship]) = element match {
      case relchain: RelationshipChain => destruct(relchain)
      case node: NodePattern => destruct(node)
    }

    def destruct(chain: RelationshipChain): (Seq[IdName], Seq[PatternRelationship]) = chain match {
      // (a)->[r]->(b)
      case RelationshipChain(NodePattern(Some(leftNodeId), Seq(), None, _), RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val leftNode = IdName(leftNodeId.name)
        val rightNode = IdName(rightNodeId.name)
        val r = PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, asPatternLength(length))
        (Seq(leftNode, rightNode), Seq(r))

      // ...->[r]->(b)
      case RelationshipChain(relChain: RelationshipChain, RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val (idNames, rels) = destruct(relChain)
        val leftNode = IdName(rels.last.right.name)
        val rightNode = IdName(rightNodeId.name)
        val resultRels = rels :+ PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, asPatternLength(length))
        (idNames :+ rightNode, resultRels)

      case _ =>
        throw new CantHandleQueryException
    }

    private def destruct(node: NodePattern): (Seq[IdName], Seq[PatternRelationship]) =
      (Seq(IdName(node.identifier.get.name)), Seq.empty)

    private def asPatternLength(length: Option[Option[Range]]): PatternLength = length match {
      case Some(Some(Range(Some(left), Some(right)))) => VarPatternLength(left.value.toInt, Some(right.value.toInt))
      case Some(Some(Range(Some(left), None))) => VarPatternLength(left.value.toInt, None)
      case Some(Some(Range(None, Some(right)))) => VarPatternLength(1, Some(right.value.toInt))
      case Some(Some(Range(None, None))) => VarPatternLength.unlimited
      case Some(None) => VarPatternLength.unlimited
      case None => SimplePatternLength
    }
  }

}

class SimplePlannerQueryBuilder extends PlannerQueryBuilder {

  import SimplePlannerQueryBuilder.PatternDestructuring._

  private def getSelectionsAndSubQueries(optWhere: Option[Where]): (Selections, Seq[(PatternExpression, QueryGraph)]) = {
    import SimplePlannerQueryBuilder.SubQueryExtraction.extractQueryGraph

    val predicates = optWhere.map(SelectionPredicates.fromWhere).getOrElse(Set.empty)

    val predicatesWithCorrectDeps = predicates.map {
      case Predicate(deps, e: PatternExpression) =>
        Predicate(deps.filter(x => isNamed(x.name)), e)
      case Predicate(deps, ors@Ors(exprs)) =>
        val newDeps = exprs.foldLeft(Set.empty[IdName]) { (acc, exp) =>
          exp match {
            case exp: PatternExpression => acc ++ SelectionPredicates.idNames(exp).filter(x => isNamed(x.name))
            case _                      => acc ++ SelectionPredicates.idNames(exp)
          }
        }
        Predicate(newDeps, ors)
      case p                               => p
    }

    val subQueries = predicatesWithCorrectDeps.flatMap {
      case Predicate(_, Ors(orOperands)) =>
        orOperands.collect {
          case expr: PatternExpression => (expr, extractQueryGraph(expr))
          case Not(expr: PatternExpression) => (expr, extractQueryGraph(expr))
        }

      case Predicate(_, Not(patternExpr: PatternExpression)) =>
        Seq((patternExpr, extractQueryGraph(patternExpr)))

      case Predicate(_, patternExpr: PatternExpression) =>
        Seq((patternExpr, extractQueryGraph(patternExpr)))

      case _ => Seq.empty
    }

    (Selections(predicates), subQueries.toSeq)
  }

  private def extractPatternInExpressionFromWhere(optWhere: Option[Where]): Seq[(PatternExpression, QueryGraph)] = {
    val expressions = optWhere.map(SelectionPredicates.fromWhere).getOrElse(Set.empty).map(_.exp).toSeq

    def containsNestedPatternExpressions(expr: Expression): Boolean = expr match {
      case _: PatternExpression      => false
      case Not(_: PatternExpression) => false
      case Ors(exprs)                => exprs.exists(containsNestedPatternExpressions)
      case expr                      => expr.exists { case _: PatternExpression => true}
    }

    val expressionsWithNestedPatternExpr = expressions.filter(containsNestedPatternExpressions)
    getPatternInExpressionQueryGraphs(expressionsWithNestedPatternExpr)
  }

  private def getPatternInExpressionQueryGraphs(expressions: Seq[Expression]): Seq[(PatternExpression, QueryGraph)] = {
    import SimplePlannerQueryBuilder.SubQueryExtraction.extractQueryGraph

    val patternExpressions = expressions.treeFold(Seq.empty[PatternExpression]) {
      case p: PatternExpression =>
        (acc, _) => acc :+ p
    }

    patternExpressions.map { e => (e, extractQueryGraph(e))}
  }

  override def produce(parsedQuery: Query): QueryPlanInput = parsedQuery match {
    case Query(None, SingleQuery(clauses)) =>
      val singleQueryPlanInput = produceQueryGraphFromClauses(SingleQueryPlanInput.empty, clauses)
      QueryPlanInput(
        query = UnionQuery(Seq(singleQueryPlanInput.q), distinct = false),
        patternInExpression = singleQueryPlanInput.patternExprTable
      )

    case Query(None, u: ast.Union) =>
      val queries = u.unionedQueries
      val distinct = u match {
        case _: UnionAll      => false
        case _: UnionDistinct => true
      }
      val plannedQueries: Seq[SingleQueryPlanInput] = queries.reverseMap(x => produceQueryGraphFromClauses(SingleQueryPlanInput.empty, x.clauses))
      val table = plannedQueries.map(_.patternExprTable).reduce(_ ++ _)
      QueryPlanInput(
        query = UnionQuery(plannedQueries.map(_.q), distinct),
        patternInExpression = table
      )

    case _ =>
      throw new CantHandleQueryException
  }

  object SingleQueryPlanInput {
    val empty = new SingleQueryPlanInput(PlannerQuery.empty, Map.empty)
  }

  case class SingleQueryPlanInput(q: PlannerQuery, patternExprTable: Map[PatternExpression, QueryGraph])

  private def produceQueryGraphFromClauses(input: SingleQueryPlanInput, clauses: Seq[Clause]): SingleQueryPlanInput = {
    clauses match {
      case Return(distinct, ListedReturnItems(items), optOrderBy, skip, limit) :: tl =>
        val newPatternInExpressionTable = input.patternExprTable ++ getPatternInExpressionQueryGraphs(items.map(_.expression))
        val sortItems = produceSortItems(optOrderBy)
        val shuffle = QueryShuffle(sortItems, skip.map(_.expression), limit.map(_.expression))
        val projection = produceProjectionsMaps(items, distinct).withShuffle(shuffle)
        val newQG = input.q.withHorizon(projection)
        val nextStep = input.copy(
          q = newQG,
          patternExprTable = newPatternInExpressionTable
        )
        produceQueryGraphFromClauses(nextStep, tl)

      case Match(optional@false, pattern: Pattern, hints, optWhere) :: tl =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val nestedPatternPredicates = extractPatternInExpressionFromWhere(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries ++ nestedPatternPredicates

        val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship], shortest: Seq[ShortestPathPattern]) = destruct(pattern)

        val plannerQuery = input.q.updateGraph {
          qg => qg.
            addSelections(selections).
            addPatternNodes(nodeIds: _*).
            addPatternRels(rels).
            addHints(hints).
            addShortestPaths(shortest: _*)
        }

        val nextStep = input.copy(q = plannerQuery, patternExprTable = newPatternInExpressionTable)
        produceQueryGraphFromClauses(nextStep, tl)

      case Match(optional@true, pattern: Pattern, hints, optWhere) :: tl =>
        val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship], shortest: Seq[ShortestPathPattern]) = destruct(pattern)

        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val nestedPatternPredicates = extractPatternInExpressionFromWhere(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries ++ nestedPatternPredicates

        val optionalMatch = QueryGraph(
          selections = selections,
          patternNodes = nodeIds.toSet,
          patternRelationships = rels.toSet,
          hints = hints.toSet,
          shortestPathPatterns = shortest.toSet
        )

        val newQuery = input.q.updateGraph {
          qg => qg.withAddedOptionalMatch(optionalMatch)
        }

        val nextStep = input.copy(q = newQuery, patternExprTable = newPatternInExpressionTable)

        produceQueryGraphFromClauses(nextStep, tl)

      case With(false, _: ReturnAll, None, None, None, optWhere) :: tl if !input.q.graph.hasOptionalPatterns =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries
        val newQuery = input.q
          .updateGraph(_.addSelections(selections))

        val nextStep = input.copy(q = newQuery, patternExprTable = newPatternInExpressionTable)
        produceQueryGraphFromClauses(nextStep, tl)

      case With(distinct, _: ReturnAll, optOrderBy, skip, limit, optWhere) :: tl =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newPatternInExpressionTable = input.patternExprTable ++ subQueries

        val tailInput: SingleQueryPlanInput = SingleQueryPlanInput(PlannerQuery(QueryGraph(selections = selections)), newPatternInExpressionTable)
        val tailPlannedOutput = produceQueryGraphFromClauses(tailInput, tl)

        val inputIds = input.q.graph.coveredIds
        val tailQuery = tailPlannedOutput.q
        val argumentIds = inputIds intersect tailQuery.graph.coveredIds
        val items = QueryProjection.forIds(inputIds)
        val shuffle = QueryShuffle(
          produceSortItems(optOrderBy),
          skip.map(_.expression),
          limit.map(_.expression)
        )
        val projection = produceProjectionsMaps(items, distinct).withShuffle(shuffle)

        val newQuery =
          input.q
            .withHorizon(projection)
            .withTail(tailQuery.updateGraph(_.withArgumentIds(argumentIds)))

        input.copy(q = newQuery, tailPlannedOutput.patternExprTable)

      case With(distinct, ListedReturnItems(items), optOrderBy, skip, limit, optWhere) :: tl =>
        val orderBy = produceSortItems(optOrderBy)
        val shuffle = QueryShuffle(orderBy, skip.map(_.expression), limit.map(_.expression))
        val projection = produceProjectionsMaps(items, distinct).withShuffle(shuffle)

        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)

        val newPatternInExpressionTable = input.patternExprTable ++ getPatternInExpressionQueryGraphs(items.map(_.expression)) ++ subQueries
        val tailInput: SingleQueryPlanInput = SingleQueryPlanInput(PlannerQuery(QueryGraph(selections = selections)), newPatternInExpressionTable)
        val tail = produceQueryGraphFromClauses(tailInput, tl)

        val inputIds = projection.keySet.map(IdName)
        val argumentIds = inputIds intersect tail.q.graph.coveredIds

        val newQuery =
          input.q
            .withHorizon(projection)
            .withTail(tail.q.updateGraph(_.withArgumentIds(argumentIds)))

        input.copy(q = newQuery, patternExprTable = tail.patternExprTable)

      case Unwind(expression, identifier) :: tl =>

        val tailInput: SingleQueryPlanInput = SingleQueryPlanInput(PlannerQuery.empty, input.patternExprTable)
        val tailPlannedOutput: SingleQueryPlanInput = produceQueryGraphFromClauses(tailInput, tl)
        val tailQuery: PlannerQuery = tailPlannedOutput.q
        val inputIds = input.q.graph.coveredIds
        val argumentIds = inputIds intersect tailQuery.graph.coveredIds
        val unwind = UnwindProjection(IdName(identifier.name), expression)

        val newQuery =
          input.q
            .withHorizon(unwind)
            .withTail(tailQuery.updateGraph(_.withArgumentIds(argumentIds)))

        input.copy(q = newQuery, tailPlannedOutput.patternExprTable)

      case Seq() =>
        input

      case _ =>
        throw new CantHandleQueryException
    }
  }

  private def produceSortItems(optOrderBy: Option[OrderBy]) =
    optOrderBy.fold(Seq.empty[SortItem])(_.sortItems)

  private def produceProjectionsMaps(items: Seq[ReturnItem], distinct: Boolean): QueryProjection = {
    val (aggregatingItems: Seq[ReturnItem], nonAggrItems: Seq[ReturnItem]) =
      items.partition(item => IsAggregate(item.expression))

    def turnIntoMap(x: Seq[ReturnItem]) = x.map(e => e.name -> e.expression).toMap

    val projectionMap = turnIntoMap(nonAggrItems)
    val aggregationsMap = turnIntoMap(aggregatingItems)

    if(projectionMap.values.exists(containsAggregate))
      throw new InternalException("Grouping keys contains aggregation. AST has not been rewritten?")

    if (aggregationsMap.nonEmpty || distinct)
      AggregatingQueryProjection(groupingKeys = projectionMap, aggregationExpressions = aggregationsMap)
    else
      RegularQueryProjection(projections = projectionMap)
  }
}
