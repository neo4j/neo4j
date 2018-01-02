/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands


import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{And, Predicate, True}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{expressions => commandexpressions, values => commandvalues, _}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.SetAction
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, ast}
import org.neo4j.cypher.internal.frontend.v2_3.ast.SetClause
import org.neo4j.cypher.internal.frontend.v2_3.notification.JoinHintUnsupportedNotification
import org.neo4j.helpers.ThisShouldNotHappenError

object StatementConverters {

  implicit class StatementConverter(val statement: ast.Statement) extends AnyVal {
    def asQuery(notifications: InternalNotificationLogger, plannerName: String = ""): commands.AbstractQuery = statement match {
      case s: ast.Query =>
        val innerQuery = s.part.asQuery(notifications, plannerName)
        s.periodicCommitHint match {
          case Some(hint) => PeriodicCommitQuery(innerQuery, hint.size.map(_.value))
          case _          => innerQuery
        }
      case s: ast.CreateIndex =>
        commands.CreateIndex(s.label.name, Seq(s.property.name))
      case s: ast.DropIndex =>
        commands.DropIndex(s.label.name, Seq(s.property.name))
      case s: ast.CreateUniquePropertyConstraint =>
        commands.CreateUniqueConstraint(
          id = s.identifier.name,
          label = s.label.name,
          idForProperty = s.property.map.asInstanceOf[ast.Identifier].name,
          propertyKey = s.property.propertyKey.name)
      case s: ast.DropUniquePropertyConstraint =>
        commands.DropUniqueConstraint(
          id = s.identifier.name,
          label = s.label.name,
          idForProperty = s.property.map.asInstanceOf[ast.Identifier].name,
          propertyKey = s.property.propertyKey.name)
      case s: ast.CreateNodePropertyExistenceConstraint =>
        commands.CreateNodePropertyExistenceConstraint(
          id = s.identifier.name,
          label = s.label.name,
          idForProperty = s.property.map.asInstanceOf[ast.Identifier].name,
          propertyKey = s.property.propertyKey.name)
      case s: ast.DropNodePropertyExistenceConstraint =>
        commands.DropNodePropertyExistenceConstraint(
          id = s.identifier.name,
          label = s.label.name,
          idForProperty = s.property.map.asInstanceOf[ast.Identifier].name,
          propertyKey = s.property.propertyKey.name)
      case s: ast.CreateRelationshipPropertyExistenceConstraint =>
        commands.CreateRelationshipPropertyExistenceConstraint(
          id = s.identifier.name,
          relType = s.relType.name,
          idForProperty = s.property.map.asInstanceOf[ast.Identifier].name,
          propertyKey = s.property.propertyKey.name)
      case s: ast.DropRelationshipPropertyExistenceConstraint =>
        commands.DropRelationshipPropertyExistenceConstraint(
          id = s.identifier.name,
          relType = s.relType.name,
          idForProperty = s.property.map.asInstanceOf[ast.Identifier].name,
          propertyKey = s.property.propertyKey.name)
      case _ =>
        throw new ThisShouldNotHappenError("cleishm", s"Unknown statement during transformation ($statement)")
    }
  }

  implicit class QueryPartConverter(val queryPart: ast.QueryPart) extends AnyVal {
    def asQuery(notifications: InternalNotificationLogger, plannerName: String): commands.AbstractQuery = queryPart match {
      case s: ast.SingleQuery =>
        s.asQuery(notifications, plannerName)
      case s: ast.UnionAll =>
        commands.Union(s.unionedQueries.reverseMap(_.asQuery(notifications, plannerName)), commands.QueryString.empty, distinct = false)
      case s: ast.UnionDistinct =>
        commands.Union(s.unionedQueries.reverseMap(_.asQuery(notifications, plannerName)), commands.QueryString.empty, distinct = true)
    }
  }

  implicit class SingleQueryConverter(val singleQuery: ast.SingleQuery) extends AnyVal {
    def asQuery(notifications: InternalNotificationLogger, plannerName: String): commands.Query =
      groupClauses(singleQuery.clauses).foldRight(None: Option[commands.Query], (_: commands.QueryBuilder).returns()) {
        case (group, (tail, defaultClose)) =>
          val b = tail.foldLeft(commands.QueryBuilder())((b, t) => b.tail(t))

          val builder = group.foldLeft(b)((b, clause) => clause match {
            case c: ast.LoadCSV      => c.addToQueryBuilder(b)
            case c: ast.Start        => c.addToQueryBuilder(b)
            case c: ast.Match        => c.addToQueryBuilder(b, notifications, plannerName)
            case c: ast.Unwind       => c.addToQueryBuilder(b)
            case c: ast.Merge        => c.addToQueryBuilder(b)
            case c: ast.Create       => c.addToQueryBuilder(b)
            case c: ast.CreateUnique => c.addToQueryBuilder(b)
            case c: ast.SetClause    => c.addToQueryBuilder(b)
            case c: ast.Delete       => c.addToQueryBuilder(b)
            case c: ast.Remove       => c.addToQueryBuilder(b)
            case c: ast.Foreach      => c.addToQueryBuilder(b)
            case _: ast.With         => b
            case _: ast.Return       => b
            case _                   => throw new ThisShouldNotHappenError("cleishm", "Unknown clause while grouping")
          })

          val result = Some(group.takeRight(2) match {
            case Seq(w: ast.With, r: ast.Return) => w.closeQueryBuilder(r.closeQueryBuilder, builder)
            case Seq(_, w: ast.With)             => w.closeQueryBuilder(builder)
            case Seq(_, r: ast.Return)           => r.closeQueryBuilder(builder)
            case Seq(w: ast.With)                => w.closeQueryBuilder(builder)
            case Seq(r: ast.Return)              => r.closeQueryBuilder(builder)
            case _                               => defaultClose(builder)
          })

          (result, (_: commands.QueryBuilder).returns(commands.AllIdentifiers()))
      }._1.get

    private def groupClauses(clauses: Seq[ast.Clause]): IndexedSeq[IndexedSeq[ast.Clause]] = {
      val (groups, last) = clauses.sliding(2).foldLeft((Vector.empty[Vector[ast.Clause]], Vector(clauses.head))) {
        case ((groups, last), pair) =>
          def split   = (groups :+ last, pair.tail.toVector)
          def combine = (groups, last ++ pair.tail)

          pair match {
            case Seq(clause)                                   => (groups, last)
            case Seq(_: ast.With, _: ast.Return)               => combine
            case Seq(_: ast.ProjectionClause, _)                  => split
            case Seq(_, _: ast.ProjectionClause)                  => combine
            case Seq(_: ast.UpdateClause, _)                   => split
            case Seq(_, _: ast.UpdateClause)                   => split
            case Seq(_: ast.Match, _)                          => split
            case Seq(_, _)                                     => combine
          }
      }
      groups :+ last
    }
  }

  implicit class LoadCsvConverter(inner: ast.LoadCSV) {
    def addToQueryBuilder(builder: commands.QueryBuilder) = {
      val items: Seq[StartItem] = builder.startItems :+ commands.LoadCSV(
        inner.withHeaders,
        toCommandExpression(inner.urlString),
        inner.identifier.name,
        inner.fieldTerminator.map(_.value)
      )
      builder.startItems(items: _*)
    }
  }

  implicit class UnwindConverter(inner: ast.Unwind) {
    def addToQueryBuilder(builder: commands.QueryBuilder) = {
      val items: Seq[StartItem] = builder.startItems :+ commands.Unwind(toCommandExpression(inner.expression),inner.identifier.name)
      builder.startItems(items: _*)
    }
  }

  implicit class StartConverter(val clause: ast.Start) extends AnyVal {
    def addToQueryBuilder(builder: commands.QueryBuilder) = {
      val wherePredicate = (builder.where, clause.where) match {
        case (p, None)                  => p
        case (True(), Some(w)) => toCommandPredicate(w.expression)
        case (p, Some(w))               => And(p, toCommandPredicate(w.expression))
      }
      builder.startItems(builder.startItems ++ startItems: _*).where(wherePredicate)
    }

    private def startItems = clause.items.map(_.asCommandStartItem)
  }

  implicit class StartItemConverter(val item: ast.StartItem) extends AnyVal {
    def asCommandStartItem = item match {
      case ast.NodeByIds(identifier, ids) =>
        commands.NodeById(identifier.name, commandexpressions.Literal(ids.map(_.value)))
      case ast.NodeByParameter(identifier, parameter) =>
        commands.NodeById(identifier.name, toCommandParameter(parameter))
      case ast.AllNodes(identifier) =>
        commands.AllNodes(identifier.name)
      case ast.NodeByIdentifiedIndex(identifier, index, key, value) =>
        commands.NodeByIndex(identifier.name, index, commandexpressions.Literal(key), toCommandExpression(value))
      case ast.NodeByIndexQuery(identifier, index, query) =>
        commands.NodeByIndexQuery(identifier.name, index, toCommandExpression(query))
      case ast.RelationshipByIds(identifier, ids) =>
        commands.RelationshipById(identifier.name, commandexpressions.Literal(ids.map(_.value)))
      case ast.RelationshipByParameter(identifier, parameter) =>
        commands.RelationshipById(identifier.name, toCommandParameter(parameter))
      case ast.AllRelationships(identifier) =>
        commands.AllRelationships(identifier.name)
      case ast.RelationshipByIdentifiedIndex(identifier, index, key, value) =>
        commands.RelationshipByIndex(identifier.name, index, commandexpressions.Literal(key), toCommandExpression(value))
      case ast.RelationshipByIndexQuery(identifier, index, query) =>
        commands.RelationshipByIndexQuery(identifier.name, index, toCommandExpression(query))
    }
  }

  implicit class MatchConverter(val clause: ast.Match) extends AnyVal {
    def addToQueryBuilder(builder: commands.QueryBuilder, notifications: InternalNotificationLogger, plannerName: String) = {
      val matches = builder.matching ++ clause.pattern.asLegacyPatterns
      val namedPaths = builder.namedPaths ++ clause.pattern.asLegacyNamedPaths
      val indexHints: Seq[StartItem with Hint] = builder.using ++ clause.hints.flatMap(_.asCommandStartHint(notifications, plannerName))
      val wherePredicate: Predicate = (builder.where, clause.where) match {
        case (p, None)                  => p
        case (True(), Some(w))          => toCommandPredicate(w.expression)
        case (p, Some(w))               => And(p, toCommandPredicate(w.expression))
      }

      builder.
        matches(matches: _*).
        namedPaths(namedPaths: _*).
        using(indexHints: _*).
        where(wherePredicate).
        isOptional(clause.optional)
    }
  }

  implicit class RonjaHintConverter(val item: ast.UsingHint) extends AnyVal {
    def asCommandStartHint(notifications: InternalNotificationLogger, plannerName: String) = item match {
      case ast.UsingIndexHint(identifier, label, property) =>
        Some(commands.SchemaIndex(identifier.name, label.name, property.name, commands.AnyIndex, None))
      case ast.UsingScanHint(identifier, label) =>
        Some(commands.NodeByLabel(identifier.name, label.name))
      case ast.UsingJoinHint(identifiers) =>
        if (PlannerName(plannerName) == RulePlannerName) {
          notifications.log(JoinHintUnsupportedNotification(identifiers.map(_.name).toSeq))
        }
        None
    }
  }

  implicit class MergeConverter(val clause: ast.Merge) extends AnyVal {
    def addToQueryBuilder(builder: commands.QueryBuilder) = {
      val updates = builder.updates ++ clause.updateActions
      val namedPaths = builder.namedPaths ++ clause.pattern.asLegacyNamedPaths
      builder.updates(updates: _*).namedPaths(namedPaths: _*)
    }

    private def update(action: SetClause): Seq[SetAction] = action.items.map {
      case setItem: ast.SetPropertyItem =>
        mutation
          .PropertySetAction(toCommandProperty(setItem.property), toCommandExpression(setItem.expression))
      case setItem: ast.SetExactPropertiesFromMapItem =>
        mutation.MapPropertySetAction(commandexpressions.Identifier(setItem.identifier.name),
          toCommandExpression(setItem.expression), removeOtherProps = true)
      case setItem: ast.SetIncludingPropertiesFromMapItem =>
        mutation.MapPropertySetAction(commandexpressions.Identifier(setItem.identifier.name),
          toCommandExpression(setItem.expression), removeOtherProps = false)
      case setItem: ast.SetLabelItem =>
        commands.LabelAction(toCommandExpression(setItem.expression), commands.LabelSetOp, setItem.labels.map(l => commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)))

      case e => throw new InternalException(s"MERGE cannot contain ${e.getClass.getSimpleName}")
    }

    def toCommand = {
      val toAbstractPatterns = clause.pattern.asAbstractPatterns
      val map = clause.actions.map {
        case ast.OnCreate(action: SetClause) =>
          OnAction(On.Create, update(action))
        case ast.OnMatch(action) =>
          OnAction(On.Match, update(action))
      }
      val legacyPatterns = clause.pattern.asLegacyPatterns.filterNot(_.isInstanceOf[commands.SingleNode])
      val creates = clause.pattern.asLegacyCreates.filterNot(_.isInstanceOf[mutation.CreateNode])
      commands.MergeAst(toAbstractPatterns, map, legacyPatterns, creates)
    }
  }

  implicit class CreateConverter(val clause: ast.Create) extends AnyVal {
    def addToQueryBuilder(builder: commands.QueryBuilder) = {
      val startItems = builder.startItems ++ clause.updateActions.map {
        case createNode: mutation.CreateNode                 => commands.CreateNodeStartItem(createNode)
        case createRelationship: mutation.CreateRelationship => commands.CreateRelationshipStartItem(createRelationship)
      }
      val namedPaths = builder.namedPaths ++ clause.pattern.asLegacyNamedPaths
      builder.startItems(startItems: _*).namedPaths(namedPaths: _*)
    }
  }

  implicit class CreateUniqueConverter(val clause: ast.CreateUnique) extends AnyVal {
    def addToQueryBuilder(builder: commands.QueryBuilder) = {
      val (newStartItems, newNamedPaths) = toCommand
      val startItems = builder.startItems ++ newStartItems
      val namedPaths = builder.namedPaths ++ newNamedPaths
      builder.startItems(startItems: _*).namedPaths(namedPaths: _*)
    }

    def toCommand = commands.CreateUniqueAst(clause.pattern.asAbstractPatterns.map(_.makeOutgoing)).nextStep()
  }

  implicit class UpdateClauseConverter(val clause: ast.UpdateClause) extends AnyVal {
    def addToQueryBuilder(builder: commands.QueryBuilder) = {
      val updates = builder.updates ++ updateActions
      builder.updates(updates: _*)
    }

    def updateActions(): Seq[mutation.UpdateAction] = clause match {
      case c: ast.Merge =>
        c.toCommand.nextStep()
      case c: ast.Create =>
        c.pattern.asLegacyCreates
      case c: ast.CreateUnique =>
        c.toCommand._1.map(_.inner)
      case c: ast.SetClause =>
        c.items.map {
          case setItem: ast.SetPropertyItem =>
            mutation.PropertySetAction(toCommandProperty(setItem.property), toCommandExpression(setItem.expression))
          case setItem: ast.SetLabelItem =>
            commands.LabelAction(toCommandExpression(setItem.expression), commands.LabelSetOp, setItem.labels.map(l => commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)))
          case setItem: ast.SetExactPropertiesFromMapItem =>
            mutation.MapPropertySetAction(commandexpressions.Identifier(setItem.identifier.name), toCommandExpression(setItem.expression), removeOtherProps = true)
          case setItem: ast.SetIncludingPropertiesFromMapItem =>
            mutation.MapPropertySetAction(commandexpressions.Identifier(setItem.identifier.name), toCommandExpression(setItem.expression), removeOtherProps = false)
        }
      case c: ast.Delete =>
        c.expressions.map(e => mutation.DeleteEntityAction(toCommandExpression(e), c.forced))
      case c: ast.Remove =>
        c.items.map {
          case remItem: ast.RemoveLabelItem =>
            commands.LabelAction(toCommandExpression(remItem.expression), commands.LabelRemoveOp, remItem.labels.map(l => commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)))
          case remItem: ast.RemovePropertyItem =>
            mutation.DeletePropertyAction(toCommandExpression(remItem.property.map), commandvalues.KeyToken.Unresolved(remItem.property.propertyKey.name, commandvalues.TokenType.PropertyKey))
        }
      case c: ast.Foreach =>
        Seq(mutation.ForeachAction(toCommandExpression(c.expression), c.identifier.name, c.updates.flatMap {
          case update: ast.UpdateClause => update.updateActions()
          case _                        => throw new ThisShouldNotHappenError("cleishm", "a non-update clause in FOREACH didn't fail semantic check")
        }))
    }
  }

  implicit class WithConverter(val clause: ast.With) extends AnyVal {
    def closeQueryBuilder(builder: commands.QueryBuilder): commands.Query = {
      val builderToClose = clause.where.fold(builder) { w =>
        val subBuilder = new commands.QueryBuilder().where(toCommandPredicate(w.expression))
        val tailQueryBuilder = builder.tail.fold(subBuilder)(t => subBuilder.tail(t))
        builder.tail(tailQueryBuilder.returns(commands.AllIdentifiers()))
      }
      ProjectionClauseConverter(clause).closeQueryBuilder(builderToClose)
    }

    def closeQueryBuilder(close: commands.QueryBuilder => commands.Query, builder: commands.QueryBuilder): commands.Query = {
      val subBuilder = clause.where.foldLeft(new commands.QueryBuilder())((b, w) => b.where(toCommandPredicate(w.expression)))
      val tailQueryBuilder = builder.tail.fold(subBuilder)(t => subBuilder.tail(t))
      ProjectionClauseConverter(clause).closeQueryBuilder(builder.tail(close(tailQueryBuilder)))
    }
  }

  implicit class ProjectionClauseConverter(val clause: ast.ProjectionClause) extends AnyVal {
    def closeQueryBuilder(builder: commands.QueryBuilder): commands.Query = {
      val columns = returnColumns

      (
        addAggregates(columns) andThen
        addSkip andThen
        addLimit andThen
        addOrder
      )(builder).returns(columns:_*)
    }

    private def returnColumns = {
      val maybeAllIdentifiers = if (clause.returnItems.includeExisting)
        Some(commands.AllIdentifiers(): ReturnColumn)
      else
        None

      maybeAllIdentifiers.toSeq ++ clause.returnItems.items.map {
        case ast.AliasedReturnItem(expr, identifier) =>
          commands.ReturnItem(toCommandExpression(expr), identifier.name)
        case ast.UnaliasedReturnItem(expr, identifier) =>
          commands.ReturnItem(toCommandExpression(expr), identifier)
      }
    }

    private def addAggregates(columns: Seq[commands.ReturnColumn]) = (b: commands.QueryBuilder) =>
      extractAggregationExpressions(columns).fold(b) { b.aggregation(_:_*) }

    private def addSkip = (b: commands.QueryBuilder) => clause.skip.fold(b)(l => l.expression match {
      case integer: ast.UnsignedIntegerLiteral =>
        b.skip(commandexpressions.Literal(integer.value.toInt))
      case expression =>
        b.skip(toCommandExpression(expression))
    })

    private def addLimit = (b: commands.QueryBuilder) => clause.limit.fold(b)(l => l.expression match {
      case integer: ast.UnsignedIntegerLiteral =>
        b.limit(commandexpressions.Literal(integer.value.toInt))
      case expression =>
        b.limit(toCommandExpression(expression))
    })

    private def addOrder = (b: commands.QueryBuilder) => clause.orderBy.fold(b)(o => b.orderBy(o.sortItems map {
      case ast.AscSortItem(expression) =>
        commands.SortItem(toCommandExpression(expression), ascending = true)
      case ast.DescSortItem(expression) =>
        commands.SortItem(toCommandExpression(expression), ascending = false)
    }:_*))

    private def extractAggregationExpressions(items: Seq[commands.ReturnColumn]) = {
      val aggregationExpressions = items.collect {
        case commands.ReturnItem(expression, _) => (expression.subExpressions :+ expression).collect {
          case agg: commandexpressions.AggregationExpression => agg
        }
      }.flatten

      (aggregationExpressions, clause.distinct) match {
        case (Seq(), false) => None
        case _              => Some(aggregationExpressions)
      }
    }
  }
}
