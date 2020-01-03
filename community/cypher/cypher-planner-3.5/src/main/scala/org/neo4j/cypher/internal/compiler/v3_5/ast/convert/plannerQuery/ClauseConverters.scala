/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.ir.v3_5.helpers.ExpressionConverters._
import org.neo4j.cypher.internal.ir.v3_5.helpers.PatternConverters._
import org.neo4j.cypher.internal.ir.v3_5.{NoHeaders, _}
import org.neo4j.cypher.internal.v3_5.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.{InternalException, SyntaxException}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ClauseConverters {

  def addToLogicalPlanInput(acc: PlannerQueryBuilder, clause: Clause): PlannerQueryBuilder = clause match {
    case c: Return => addReturnToLogicalPlanInput(acc, c)
    case c: Match => addMatchToLogicalPlanInput(acc, c)
    case c: With => addWithToLogicalPlanInput(acc, c)
    case c: Unwind => addUnwindToLogicalPlanInput(acc, c)
    case c: ResolvedCall => addCallToLogicalPlanInput(acc, c)
    case c: Create => addCreateToLogicalPlanInput(acc, c)
    case c: SetClause => addSetClauseToLogicalPlanInput(acc, c)
    case c: Delete => addDeleteToLogicalPlanInput(acc, c)
    case c: Remove => addRemoveToLogicalPlanInput(acc, c)
    case c: Merge => addMergeToLogicalPlanInput(acc, c)
    case c: LoadCSV => addLoadCSVToLogicalPlanInput(acc, c)
    case c: Foreach => addForeachToLogicalPlanInput(acc, c)

    case x: UnresolvedCall => throw new IllegalArgumentException(s"$x is not expected here")
    case x => throw new InternalException(s"Received an AST-clause that has no representation the QG: $x")
  }

  private def addLoadCSVToLogicalPlanInput(acc: PlannerQueryBuilder, clause: LoadCSV): PlannerQueryBuilder =
    acc.withHorizon(
      LoadCSVProjection(
        variable = clause.variable.name,
        url = clause.urlString,
        format = if (clause.withHeaders) HasHeaders else NoHeaders,
        clause.fieldTerminator)
    ).withTail(PlannerQuery.empty)

  private def asSelections(optWhere: Option[Where]) = Selections(optWhere.
    map(_.expression.asPredicates).
    getOrElse(Set.empty))

  private def asQueryShuffle(optOrderBy: Option[OrderBy]) = {
    val sortItems: Seq[SortItem] = optOrderBy.fold(Seq.empty[SortItem])(_.sortItems)
    QueryShuffle(sortItems, None, None)
  }

  private def asQueryProjection(distinct: Boolean, items: Seq[ReturnItem]): QueryProjection = {
    val (aggregatingItems: Seq[ReturnItem], groupingKeys: Seq[ReturnItem]) =
      items.partition(item => IsAggregate(item.expression))

    def turnIntoMap(x: Seq[ReturnItem]) = x.map(e => e.name -> e.expression).toMap

    val projectionMap = turnIntoMap(groupingKeys)
    val aggregationsMap = turnIntoMap(aggregatingItems)

    if (projectionMap.values.exists(containsAggregate))
      throw new InternalException("Grouping keys contains aggregation. AST has not been rewritten?")

    if (aggregationsMap.nonEmpty)
      AggregatingQueryProjection(groupingExpressions = projectionMap, aggregationExpressions = aggregationsMap)
    else if (distinct)
      DistinctQueryProjection(groupingKeys = projectionMap)
    else
      RegularQueryProjection(projections = projectionMap)
  }

  private def addReturnToLogicalPlanInput(acc: PlannerQueryBuilder,
                                          clause: Return): PlannerQueryBuilder = clause match {
    case Return(distinct, ReturnItems(star, items), optOrderBy, skip, limit, _) if !star =>

      val shuffle = asQueryShuffle(optOrderBy).
        withSkip(skip).
        withLimit(limit)

      val projection = asQueryProjection(distinct, items).
        withShuffle(shuffle)
      val returns = items.collect {
        case AliasedReturnItem(_, variable) => variable.name
      }
      val requiredOrder = findRequiredOrder(projection)

      acc.
        withHorizon(projection).
        withReturns(returns).
        withInterestingOrder(requiredOrder)
    case _ =>
      throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)
  }

  def findRequiredOrder(horizon: QueryHorizon): InterestingOrder = {
    val requiredOrderColumns = horizon match {
      case RegularQueryProjection(projections, shuffle, _) =>
        extractColumnsFromHorizon(shuffle, projections)
      case AggregatingQueryProjection(groupingExpressions, _, shuffle, _) =>
        extractColumnsFromHorizon(shuffle, groupingExpressions)
      case DistinctQueryProjection(groupingExpressions, shuffle, _) =>
        extractColumnsFromHorizon(shuffle, groupingExpressions)
      case _ => Seq.empty
    }
    InterestingOrder(requiredOrderColumns)
  }

  private def extractColumnsFromHorizon(shuffle: QueryShuffle, projections: Map[String, Expression]): Seq[InterestingOrder.ColumnOrder] = {

    import InterestingOrder._

    shuffle.sortItems.foldLeft((Seq.empty[ColumnOrder], true))({
      case ((seq, true), AscSortItem(Property(LogicalVariable(varName), propName))) =>
        projections.get(varName) match {
          case Some(LogicalVariable(originalVarName)) => (seq :+ Asc(s"$originalVarName.${propName.name}"), true)
          case Some(_) => (Seq.empty, false)
          case None => (seq :+ Asc(s"$varName.${propName.name}"), true)
        }
      case ((seq, true), DescSortItem(Property(LogicalVariable(varName), propName))) =>
        projections.get(varName) match {
          case Some(LogicalVariable(originalVarName)) => (seq :+ Desc(s"$originalVarName.${propName.name}"), true)
          case Some(_) => (Seq.empty, false)
          case None => (seq :+ Desc(s"$varName.${propName.name}"), true)
        }

      case ((seq, true), AscSortItem(LogicalVariable(name))) =>
        projections.get(name) match {
          case Some(Property(LogicalVariable(varName), propName)) => (seq :+ Asc(s"$varName.${propName.name}"), true)
          case Some(Variable(oldName)) => (seq :+ Asc(oldName), true)
          case Some(_) => (Seq.empty, false)
          case None => (seq :+ Asc(name), true)
        }
      case ((seq, true), DescSortItem(LogicalVariable(name))) =>
        projections.get(name) match {
          case Some(Property(LogicalVariable(varName), propName)) => (seq :+ Desc(s"$varName.${propName.name}"), true)
          case Some(Variable(oldName)) => (seq :+ Desc(oldName), true)
          case Some(_) => (Seq.empty, false)
          case None => (seq :+ Desc(name), true)
        }

      case _ => (Seq.empty, false)
    })._1
  }

  private def addSetClauseToLogicalPlanInput(acc: PlannerQueryBuilder, clause: SetClause): PlannerQueryBuilder =
    clause.items.foldLeft(acc) {

      case (builder, item) =>
        builder.amendQueryGraph(_.addMutatingPatterns(toSetPattern(acc.semanticTable)(item)))
    }

  private def addCreateToLogicalPlanInput(builder: PlannerQueryBuilder, clause: Create): PlannerQueryBuilder = {

    val nodes = new ArrayBuffer[CreateNode]()
    val relationships = new ArrayBuffer[CreateRelationship]()

    // We need this locally to avoid creating nodes twice if they occur
    // multiple times in this clause, but haven't occured before
    val seenPatternNodes = mutable.Set[String]()
    seenPatternNodes ++= builder.allSeenPatternNodes

    clause.pattern.patternParts.foreach {
      //CREATE (n :L1:L2 {prop: 42})
      case EveryPath(NodePattern(Some(id), labels, props, _)) =>
        nodes += CreateNode(id.name, labels, props)
        seenPatternNodes += id.name

      //CREATE (n)-[r: R]->(m)
      case EveryPath(pattern: RelationshipChain) =>

        val (currentNodes, currentRelationships) = allCreatePatterns(pattern)

        //remove duplicates from loops, (a:L)-[:ER1]->(a)
        val dedupedNodes = dedup(currentNodes)

        //create nodes that are not already matched or created
        val (nodesCreatedBefore, nodesToCreate) = dedupedNodes.partition(pattern => seenPatternNodes(pattern.idName))

        //we must check that we are not trying to set a pattern or label on any already created nodes
        nodesCreatedBefore.collectFirst {
          case c if c.labels.nonEmpty || c.properties.nonEmpty =>
            throw new SyntaxException(
              s"Can't create node `${c.idName}` with labels or properties here. The variable is already declared in this context")
        }

        nodes ++= nodesToCreate
        seenPatternNodes ++= nodesToCreate.map(_.idName)
        relationships ++= currentRelationships

      case x => throw new InternalException(s"Received an AST-clause that has no representation the QG: $clause")
    }

    builder.amendQueryGraph(_.addMutatingPatterns(CreatePattern(nodes, relationships)))
  }

  private def dedup(nodePatterns: Vector[CreateNode]) = {
    val seen = mutable.Set.empty[String]
    val result = mutable.ListBuffer.empty[CreateNode]
    nodePatterns.foreach { pattern =>
      if (!seen(pattern.idName)) result.append(pattern)
      else if (pattern.labels.nonEmpty || pattern.properties.nonEmpty) {
        //reused patterns must be pure variable
        throw new SyntaxException(s"Can't create node `${pattern.idName}` with labels or properties here. The variable is already declared in this context")
      }
      seen.add(pattern.idName)
    }
    result.toIndexedSeq
  }

  private def allCreatePatterns(element: PatternElement): (Vector[CreateNode], Vector[CreateRelationship]) = element match {
    case NodePattern(None, _, _, _) => throw new InternalException("All nodes must be named at this instance")
    //CREATE ()
    case NodePattern(Some(variable), labels, props, _) =>
      (Vector(CreateNode(variable.name, labels, props)), Vector.empty)

    //CREATE ()-[:R]->()
    case RelationshipChain(leftNode: NodePattern, rel, rightNode) =>
      val leftIdName = leftNode.variable.get.name
      val rightIdName = rightNode.variable.get.name

      //Semantic checking enforces types.size == 1
      val relType = rel.types.headOption.getOrElse(
        throw new InternalException("Expected single relationship type"))

      (Vector(
        CreateNode(leftIdName, leftNode.labels, leftNode.properties),
        CreateNode(rightIdName, rightNode.labels, rightNode.properties)
      ), Vector(
        CreateRelationship(rel.variable.get.name, leftIdName, relType, rightIdName, rel.direction, rel.properties)
      ))

    //CREATE ()->[:R]->()-[:R]->...->()
    case RelationshipChain(left, rel, rightNode) =>
      val (nodes, rels) = allCreatePatterns(left)
      val rightIdName = rightNode.variable.get.name

      (nodes :+
        CreateNode(rightIdName, rightNode.labels, rightNode.properties)
        , rels :+
        CreateRelationship(rel.variable.get.name, nodes.last.idName, rel.types.head,
          rightIdName, rel.direction, rel.properties))
  }

  private def addDeleteToLogicalPlanInput(acc: PlannerQueryBuilder, clause: Delete): PlannerQueryBuilder = {
    acc.amendQueryGraph(_.addMutatingPatterns(clause.expressions.map(DeleteExpression(_, clause.forced))))
  }

  private def asReturnItems(current: QueryGraph, returnItems: ReturnItemsDef): Seq[ReturnItem] = returnItems match {
    case ReturnItems(star, items) if star =>
      QueryProjection.forIds(current.allCoveredIds) ++ items
    case ReturnItems(_, items) =>
      items
    case _ =>
      Seq.empty
  }

  private def addMatchToLogicalPlanInput(acc: PlannerQueryBuilder, clause: Match): PlannerQueryBuilder = {
    val patternContent = clause.pattern.destructed

    val selections = asSelections(clause.where)

    if (clause.optional) {
      acc.
        amendQueryGraph { qg => qg.withAddedOptionalMatch(
          // When adding QueryGraphs for optional matches, we always start with a new one.
          // It's either all or nothing per match clause.
          QueryGraph(
            selections = selections,
            patternNodes = patternContent.nodeIds.toSet,
            patternRelationships = patternContent.rels.toSet,
            hints = clause.hints,
            shortestPathPatterns = patternContent.shortestPaths.toSet
          ))
        }
    } else {
      acc.amendQueryGraph {
        qg => qg.
          addSelections(selections).
          addPatternNodes(patternContent.nodeIds: _*).
          addPatternRelationships(patternContent.rels).
          addHints(clause.hints).
          addShortestPaths(patternContent.shortestPaths: _*)
      }
    }
  }

  private def toPropertyMap(expr: Option[Expression]): Map[PropertyKeyName, Expression] = expr match {
    case None => Map.empty
    case Some(MapExpression(items)) => items.toMap
    case e => throw new InternalException(s"Expected MapExpression, got $e")
  }

  private def toPropertySelection(identifier: LogicalVariable,  map:Map[PropertyKeyName, Expression]): Seq[Expression] = map.map {
    case (k, e) => In(Property(identifier, k)(k.position), ListLiteral(Seq(e))(e.position))(identifier.position)
  }.toIndexedSeq

  private def toSetPattern(semanticTable: SemanticTable)(setItem: SetItem): SetMutatingPattern = setItem match {
    case SetLabelItem(id, labels) => SetLabelPattern(id.name, labels)

    case SetPropertyItem(Property(node: Variable, propertyKey), expr) if semanticTable.isNode(node) =>
      SetNodePropertyPattern(node.name, propertyKey, expr)

    case SetPropertyItem(Property(rel: Variable, propertyKey), expr) if semanticTable.isRelationship(rel) =>
      SetRelationshipPropertyPattern(rel.name, propertyKey, expr)

    case SetPropertyItem(Property(entityExpr, propertyKey), expr) =>
      SetPropertyPattern(entityExpr, propertyKey, expr)

    case SetExactPropertiesFromMapItem(node, expression) if semanticTable.isNode(node) =>
      SetNodePropertiesFromMapPattern(node.name, expression, removeOtherProps = true)

    case SetExactPropertiesFromMapItem(rel, expression) if semanticTable.isRelationship(rel) =>
      SetRelationshipPropertiesFromMapPattern(rel.name, expression, removeOtherProps = true)

    case SetExactPropertiesFromMapItem(vr, expression) =>
      SetPropertiesFromMapPattern(vr, expression, removeOtherProps = true)

    case SetIncludingPropertiesFromMapItem(node, expression) if semanticTable.isNode(node) =>
      SetNodePropertiesFromMapPattern(node.name, expression, removeOtherProps = false)

    case SetIncludingPropertiesFromMapItem(rel, expression) if semanticTable.isRelationship(rel) =>
      SetRelationshipPropertiesFromMapPattern(rel.name, expression, removeOtherProps = false)

    case SetIncludingPropertiesFromMapItem(vr, expression) =>
      SetPropertiesFromMapPattern(vr, expression, removeOtherProps = false)
  }

  private def addMergeToLogicalPlanInput(builder: PlannerQueryBuilder, clause: Merge): PlannerQueryBuilder = {

    val onCreate = clause.actions.collect {
      case OnCreate(setClause) => setClause.items.map(toSetPattern(builder.semanticTable))
    }.flatten
    val onMatch = clause.actions.collect {
      case OnMatch(setClause) => setClause.items.map(toSetPattern(builder.semanticTable))
    }.flatten

    clause.pattern.patternParts.foldLeft(builder) {
      //MERGE (n :L1:L2 {prop: 42})
      case (acc, EveryPath(NodePattern(Some(id), labels, props, _))) =>
        val currentlyAvailableVariables = builder.currentlyAvailableVariables
        val labelPredicates = labels.map(l => HasLabels(id, Seq(l))(id.position))
        val propertyPredicates = toPropertySelection(id, toPropertyMap(props))
        val createNodePattern = CreateNode(id.name, labels, props)

        val matchGraph = QueryGraph(
          patternNodes = Set(id.name),
          selections = Selections.from(labelPredicates ++ propertyPredicates),
          argumentIds = currentlyAvailableVariables
        )

        val queryGraph = QueryGraph.empty
          .withArgumentIds(matchGraph.argumentIds)
          .addMutatingPatterns(MergeNodePattern(createNodePattern, matchGraph, onCreate, onMatch))

        acc
          .withHorizon(PassthroughAllHorizon())
          .withTail(RegularPlannerQuery(queryGraph = queryGraph))
          .withHorizon(asQueryProjection(distinct = false, QueryProjection.forIds(queryGraph.allCoveredIds)))
          .withTail(RegularPlannerQuery())

      //MERGE (n)-[r: R]->(m)
      case (acc, EveryPath(pattern: RelationshipChain)) =>
        val (nodes, rels) = allCreatePatterns(pattern)
        //remove duplicates from loops, (a:L)-[:ER1]->(a)
        val dedupedNodes = dedup(nodes)

        val seenPatternNodes = acc.allSeenPatternNodes
        //create nodes that are not already matched or created
        val nodesToCreate = dedupedNodes.filterNot(pattern => seenPatternNodes(pattern.idName))
        //we must check that we are not trying to set a pattern or label on any already created nodes
        val nodesCreatedBefore = dedupedNodes.filter(pattern => seenPatternNodes(pattern.idName)).toSet

        nodesCreatedBefore.collectFirst {
          case c if c.labels.nonEmpty || c.properties.nonEmpty =>
            throw new SyntaxException(
              s"Can't create node `${c.idName}` with labels or properties here. The variable is already declared in this context")
        }

        val pos = pattern.position

        val selections = asSelections(clause.where)

        val hasLabels = nodes.flatMap(n =>
          n.labels.map(l => HasLabels(Variable(n.idName)(pos), Seq(l))(pos))
        )

        val hasProps = nodes.flatMap(n =>
          toPropertySelection(Variable(n.idName)(pos), toPropertyMap(n.properties))
        ) ++ rels.flatMap(r =>
          toPropertySelection(Variable(r.idName)(pos), toPropertyMap(r.properties)))

        val matchGraph = QueryGraph(
          patternNodes = nodes.map(_.idName).toSet,
          patternRelationships = rels.map(r => PatternRelationship(r.idName, (r.leftNode, r.rightNode),
            r.direction, Seq(r.relType), SimplePatternLength)).toSet,
          selections = selections ++ Selections.from(hasLabels ++ hasProps),
          argumentIds = builder.currentlyAvailableVariables ++ nodesCreatedBefore.map(_.idName)
        )

        val queryGraph = QueryGraph.empty
          .withArgumentIds(matchGraph.argumentIds)
          .addMutatingPatterns(MergeRelationshipPattern(nodesToCreate, rels, matchGraph, onCreate, onMatch))

        acc.
          withHorizon(PassthroughAllHorizon()).
          withTail(RegularPlannerQuery(queryGraph = queryGraph)).
          withHorizon(asQueryProjection(distinct = false, QueryProjection.forIds(queryGraph.allCoveredIds))).
          withTail(RegularPlannerQuery())

      case x => throw new InternalException(s"Received an AST-clause that has no representation the QG: ${x._2}")
    }
  }

  private def addWithToLogicalPlanInput(builder: PlannerQueryBuilder,
                                        clause: With): PlannerQueryBuilder = clause match {

    /*
    When encountering a WITH that is not an event horizon, and we have no optional matches in the current QueryGraph,
    we simply continue building on the current PlannerQuery. Our ASTRewriters rewrite queries in such a way that
    a lot of queries have these WITH clauses.

    Handles: ... WITH * [WHERE <predicate>] ...
     */
    case With(false, ri, None, None, None, where)
      if !(builder.currentQueryGraph.hasOptionalPatterns || builder.currentQueryGraph.containsUpdates)
        && ri.items.forall(item => !containsAggregate(item.expression))
        && builder.currentQueryGraph.shortestPathPatterns.isEmpty
        && ri.items.forall {
        case item: AliasedReturnItem => item.expression == item.variable
        case x => throw new InternalException("This should have been rewritten to an AliasedReturnItem.")
      } && builder.readOnly =>
      val selections = asSelections(where)
      builder.
        amendQueryGraph(_.addSelections(selections))

    /*
    When encountering a WITH that is an event horizon, we introduce the horizon and start a new empty QueryGraph.

    Handles all other WITH clauses
     */
    case With(distinct, projection, orderBy, skip, limit, where) =>
      val selections = asSelections(where)
      val returnItems = asReturnItems(builder.currentQueryGraph, projection)

      val shuffle =
        asQueryShuffle(orderBy).
          withLimit(limit).
          withSkip(skip)

      val queryProjection =
        asQueryProjection(distinct, returnItems).
          withShuffle(shuffle).
          withSelection(selections)

      val requiredOrder = findRequiredOrder(queryProjection)

      builder.
        withHorizon(queryProjection).
        withInterestingOrder(requiredOrder).
        withTail(RegularPlannerQuery(QueryGraph()))

    case _ =>
      throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)
  }

  private def addUnwindToLogicalPlanInput(builder: PlannerQueryBuilder, clause: Unwind): PlannerQueryBuilder =
    builder.
      withHorizon(
        UnwindProjection(
          variable = clause.variable.name,
          exp = clause.expression)
      ).
      withTail(PlannerQuery.empty)

  private def addCallToLogicalPlanInput(builder: PlannerQueryBuilder, call: ResolvedCall): PlannerQueryBuilder = {
    builder
      .withHorizon(ProcedureCallProjection(call))
      .withTail(PlannerQuery.empty)
  }

  private def addForeachToLogicalPlanInput(builder: PlannerQueryBuilder, clause: Foreach): PlannerQueryBuilder = {
    val currentlyAvailableVariables = builder.currentlyAvailableVariables

    val setOfNodeVariables: Set[String] =
      if (builder.semanticTable.isNode(clause.variable.name))
        Set(clause.variable.name)
      else Set.empty

    val foreachVariable = clause.variable
    val projectionToInnerUpdates = asQueryProjection(distinct = false, QueryProjection
      .forIds(currentlyAvailableVariables + foreachVariable.name))

    val innerBuilder = new PlannerQueryBuilder(PlannerQuery.empty, builder.semanticTable)
      .amendQueryGraph(_.addPatternNodes((builder.allSeenPatternNodes ++ setOfNodeVariables).toIndexedSeq:_*)
                        .addArgumentIds(foreachVariable.name +: currentlyAvailableVariables.toIndexedSeq))
      .withHorizon(projectionToInnerUpdates)

    val innerPlannerQuery =
      StatementConverters.flattenCreates(clause.updates)
        .foldLeft(innerBuilder) {
          case (acc, innerClause) => addToLogicalPlanInput(acc, innerClause)
        }.build()

    val foreachPattern = ForeachPattern(
      variable = clause.variable.name,
      expression = clause.expression,
      innerUpdates = innerPlannerQuery)

    val foreachGraph = QueryGraph(
      argumentIds = currentlyAvailableVariables,
      mutatingPatterns = IndexedSeq(foreachPattern)
    )

    // Since foreach can contain reads (via inner merge) we put it in its own separate planner query
    // to maintain the strict ordering of reads followed by writes within a single planner query
    builder
      .withHorizon(PassthroughAllHorizon())
      .withTail(RegularPlannerQuery(queryGraph = foreachGraph))
      .withHorizon(PassthroughAllHorizon()) // NOTE: We do not expose anything from foreach itself
      .withTail(RegularPlannerQuery())
  }

  private def addRemoveToLogicalPlanInput(acc: PlannerQueryBuilder, clause: Remove): PlannerQueryBuilder = {
    clause.items.foldLeft(acc) {
      // REMOVE n:Foo
      case (builder, RemoveLabelItem(variable, labelNames)) =>
        builder.amendQueryGraph(_.addMutatingPatterns(RemoveLabelPattern(variable.name, labelNames)))

      // REMOVE n.prop
      case (builder, RemovePropertyItem(Property(variable: Variable, propertyKey))) if acc.semanticTable.isNode(variable) =>
        builder.amendQueryGraph(_.addMutatingPatterns(
          SetNodePropertyPattern(variable.name,propertyKey, Null()(propertyKey.position))
        ))

      // REMOVE rel.prop
      case (builder, RemovePropertyItem(Property(variable: Variable, propertyKey))) if acc.semanticTable.isRelationship(variable) =>
        builder.amendQueryGraph(_.addMutatingPatterns(
          SetRelationshipPropertyPattern(variable.name, propertyKey, Null()(propertyKey.position))
        ))

      // REMOVE rel.prop when unknown whether node or rel
      case (builder, RemovePropertyItem(Property(variable, propertyKey))) =>
        builder.amendQueryGraph(_.addMutatingPatterns(
          SetPropertyPattern(variable, propertyKey, Null()(propertyKey.position))
        ))

      case (_, other) =>
        throw new InternalException(s"REMOVE $other not supported in cost planner yet")
    }
  }
}
