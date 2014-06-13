/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
      case p => p
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
      case expr                      => expr.exists {case _: PatternExpression => true }
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

    patternExpressions.map { e => (e, extractQueryGraph(e)) }
  }

  override def produce(ast: Query): QueryPlanInput = ast match {
    case Query(None, SingleQuery(clauses)) =>
      val (query, subQueryTable, patternExprTable) = produceQueryGraphFromClauses(PlannerQuery.empty, Map.empty, Map.empty, clauses)
      QueryPlanInput(UnionQuery(Seq(query), distinct = false), subQueryTable, patternExprTable)

    case _ =>
      throw new CantHandleQueryException
  }

  private def produceQueryGraphFromClauses(querySoFar: PlannerQuery,
                                           subQueryLookupTable: Map[PatternExpression, QueryGraph],
                                           patternInExpressionTable: Map[PatternExpression, QueryGraph],
                                           clauses: Seq[Clause]): (PlannerQuery,  Map[PatternExpression, QueryGraph], Map[PatternExpression, QueryGraph]) =
    clauses match {
      case Return(false, ListedReturnItems(items), optOrderBy, skip, limit) :: tl =>
        val newPatternInExpressionTable = patternInExpressionTable ++ getPatternInExpressionQueryGraphs(items.map(_.expression))
        val sortItems = produceSortItems(optOrderBy)
        val projection = produceProjectionsMaps(items)
          .withSortItems(sortItems)
          .withLimit(limit.map(_.expression))
          .withSkip(skip.map(_.expression))

        val newQG = querySoFar.withProjection(projection)
        produceQueryGraphFromClauses(newQG, subQueryLookupTable, newPatternInExpressionTable, tl)

      case Match(optional@false, pattern: Pattern, hints, optWhere) :: tl =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newSubQueryPlanTable = subQueryLookupTable ++ subQueries

        val nestedPatternPredicates = extractPatternInExpressionFromWhere(optWhere)
        val newPatternInExpressionTable = patternInExpressionTable ++ nestedPatternPredicates

        val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship], shortest: Seq[ShortestPathPattern]) = destruct(pattern)

        val newQuery = querySoFar.updateGraph {
          qg => qg.
            addSelections(selections).
            addPatternNodes(nodeIds: _*).
            addPatternRels(rels).
            addHints(hints).
            addShortestPaths(shortest: _*)
        }

        produceQueryGraphFromClauses(newQuery, newSubQueryPlanTable, newPatternInExpressionTable, tl)

      case Match(optional@true, pattern: Pattern, hints, optWhere) :: tl =>
        val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship], shortest: Seq[ShortestPathPattern]) = destruct(pattern)

        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newSubQueryPlanTable = subQueryLookupTable ++ subQueries

        val nestedPatternPredicates = extractPatternInExpressionFromWhere(optWhere)
        val newPatternInExpressionTable = patternInExpressionTable ++ nestedPatternPredicates

        val optionalMatch = QueryGraph(
          selections = selections,
          patternNodes = nodeIds.toSet,
          patternRelationships = rels.toSet,
          hints = hints.toSet,
          shortestPathPatterns = shortest.toSet
        )

        val newQuery = querySoFar.updateGraph {
          qg => qg.withAddedOptionalMatch(optionalMatch)
        }

        produceQueryGraphFromClauses(newQuery, newSubQueryPlanTable, newPatternInExpressionTable, tl)

      case With(false, _: ReturnAll, optOrderBy, None, None, optWhere) :: tl if !querySoFar.graph.hasOptionalPatterns =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newSubQueryPlanTable = subQueryLookupTable ++ subQueries

        val newQuery = querySoFar
          .updateGraph(_.addSelections(selections))
          .updateProjections(
            _.withSortItems(produceSortItems(optOrderBy))
          )

        produceQueryGraphFromClauses(newQuery, newSubQueryPlanTable, patternInExpressionTable, tl)

      case With(false, _: ReturnAll, optOrderBy, skip, limit, optWhere) :: tl =>
        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newSubQueryPlanTable = subQueryLookupTable ++ subQueries

        val (tailQuery: PlannerQuery, tailSubQuery, tailPatternInExpression) = produceQueryGraphFromClauses(
          PlannerQuery(QueryGraph(selections = selections)),
          newSubQueryPlanTable,
          patternInExpressionTable,
          tl
        )

        val inputIds = querySoFar.graph.coveredIds
        val argumentIds = inputIds intersect tailQuery.graph.coveredIds

        val newQuery =
          querySoFar
            .withProjection(
              QueryProjection.forIds(inputIds)
                .withSortItems(produceSortItems(optOrderBy))
                .withLimit(limit.map(_.expression))
                .withSkip(skip.map(_.expression))
            )
            .withTail(tailQuery.updateGraph(_.withArgumentIds(argumentIds)))

        (newQuery, tailSubQuery, tailPatternInExpression)

      case With(false, ListedReturnItems(items), optOrderBy, skip, limit, optWhere) :: tl =>
        val newPatternInExpressionTable = patternInExpressionTable ++ getPatternInExpressionQueryGraphs(items.map(_.expression))
        val orderBy = produceSortItems(optOrderBy)
        val projection = produceProjectionsMaps(items)
          .withSortItems(orderBy)
          .withLimit(limit.map(_.expression))
          .withSkip(skip.map(_.expression))

        val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
        val newSubQueryPlanTable = subQueryLookupTable ++ subQueries

        val (tailQuery: PlannerQuery, tailSubQuery, tailPatternInExpression) = produceQueryGraphFromClauses(
          PlannerQuery(QueryGraph(selections = selections)),
          newSubQueryPlanTable,
          newPatternInExpressionTable,
          tl
        )

        val inputIds = projection.keySet.map(IdName)
        val argumentIds = inputIds intersect tailQuery.graph.coveredIds

        val newQuery =
          querySoFar
            .withProjection(projection)
            .withTail(tailQuery.updateGraph(_.withArgumentIds(argumentIds)))

        (newQuery, tailSubQuery, tailPatternInExpression)

      case Seq() =>
        (querySoFar, subQueryLookupTable, patternInExpressionTable)

      case _ =>
        throw new CantHandleQueryException
      }

  private def produceSortItems(optOrderBy: Option[OrderBy]) =
    optOrderBy.fold(Seq.empty[SortItem])(_.sortItems)

  private def produceProjectionsMaps(items: Seq[ReturnItem]): QueryProjection = {
    val (aggregatingItems: Seq[ReturnItem], nonAggrItems: Seq[ReturnItem]) =
      items.partition(item => IsAggregate(item.expression))

    def turnIntoMap(x: Seq[ReturnItem]) = x.map(e => e.name -> e.expression).toMap

    val projectionMap = turnIntoMap(nonAggrItems)
    val aggregationsMap = turnIntoMap(aggregatingItems)

    if(projectionMap.values.exists(containsAggregate))
      throw new InternalException("Grouping keys contains aggregation. AST has not been rewritten?")

    if (aggregationsMap.isEmpty)
      QueryProjection(projections = projectionMap)
    else
      AggregationProjection(groupingKeys = projectionMap, aggregationExpressions = aggregationsMap)
  }
}
