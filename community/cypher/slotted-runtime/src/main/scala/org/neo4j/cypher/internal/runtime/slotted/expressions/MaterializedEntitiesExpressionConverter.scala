/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.functions.Function
import org.neo4j.cypher.internal.expressions.functions.Keys
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.physicalplanning
import org.neo4j.cypher.internal.physicalplanning.ast.PropertyProjectionEntry
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NullInNullOutExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsFalse
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsMatchResult
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsTrue
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsUnknown
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues

case class MaterializedEntitiesExpressionConverter(tokenContext: ReadTokenContext) extends ExpressionConverter {

  override def toCommandExpression(
    id: Id,
    expression: expressions.Expression,
    self: ExpressionConverters
  ): Option[commands.expressions.Expression] = {
    expression match {
      case e: expressions.LogicalProperty  => Some(toCommandProperty(id, e, self))
      case e: expressions.HasLabels        => hasLabels(id, e, self)
      case e: expressions.HasTypes         => hasTypes(id, e, self)
      case e: expressions.HasLabelsOrTypes => hasLabelsOrTypes(id, e, self)
      case e: expressions.DesugaredMapProjection =>
        Some(MaterializedDesugaredMapExpression(
          self.toCommandExpression(id, e.entity),
          e.items.map(le => le.key.name -> self.toCommandExpression(id, le.exp)).toMap,
          e.includeAllProps
        ))
      case e: physicalplanning.ast.PropertyProjection =>
        Some(MaterializedPropertyProjectionExpression(self.toCommandExpression(id, e.map), e.entries))
      case e: expressions.FunctionInvocation => toCommandExpression(id, e.function, e, self)
      case _                                 => None
    }
  }

  override def toGroupingExpression(
    id: Id,
    groupings: Map[LogicalVariable, expressions.Expression],
    orderToLeverage: collection.Seq[expressions.Expression],
    self: ExpressionConverters
  ): Option[GroupingExpression] = None

  override def toCommandProjection(
    id: Id,
    projections: Map[LogicalVariable, expressions.Expression],
    self: ExpressionConverters
  ): Option[CommandProjection] = None

  private def toCommandExpression(
    id: Id,
    expression: Function,
    invocation: expressions.FunctionInvocation,
    self: ExpressionConverters
  ): Option[commands.expressions.Expression] =
    (expression, invocation.arguments.headOption) match {
      case (Keys, Some(arg))   => Some(MaterializedEntityKeysFunction(self.toCommandExpression(id, arg)))
      case (Labels, Some(arg)) => Some(MaterializedEntityLabelsFunction(self.toCommandExpression(id, arg)))
      case (Type, Some(arg))   => Some(MaterializedEntityTypeFunction(self.toCommandExpression(id, arg)))
      case _                   => None
    }

  private def toCommandProperty(
    id: Id,
    e: expressions.LogicalProperty,
    self: ExpressionConverters
  ): commands.expressions.Expression =
    e match {
      case Property(map, propertyKey) =>
        MaterializedEntityProperty(self.toCommandExpression(id, map), getPropertyKey(propertyKey))
      case _ =>
        throw new IllegalStateException(
          "We do not expect this property in MaterializedEntitiesExpressionConverter: " + e
        )
    }

  private def getPropertyKey(propertyKey: PropertyKeyName) = tokenContext.getOptPropertyKeyId(propertyKey.name) match {
    case Some(propertyKeyId) =>
      PropertyKey(propertyKey.name, propertyKeyId)
    case _ =>
      PropertyKey(propertyKey.name)
  }

  private def hasLabels(id: Id, e: expressions.HasLabels, self: ExpressionConverters): Option[Predicate] = {
    val entity = self.toCommandExpression(id, e.expression)
    val hasLabelPredicates =
      e.labels.map(l => MaterializedEntityHasLabel(entity, KeyToken.Unresolved(l.name, TokenType.Label)))
    Some(predicates.Ands(hasLabelPredicates: _*))
  }

  private def hasTypes(id: Id, e: expressions.HasTypes, self: ExpressionConverters): Option[Predicate] = {
    val entity = self.toCommandExpression(id, e.expression)
    val hasTypePredicates =
      e.types.map(t => MaterializedEntityHasType(entity, KeyToken.Unresolved(t.name, TokenType.RelType)))
    Some(predicates.Ands(hasTypePredicates: _*))
  }

  private def hasLabelsOrTypes(
    id: Id,
    e: expressions.HasLabelsOrTypes,
    self: ExpressionConverters
  ): Option[Predicate] = {
    val entity = self.toCommandExpression(id, e.entityExpression)
    val hasLabelOrTypePredicates = e.labelsOrTypes.map(lOrT => MaterializedEntityHasLabelOrType(entity, lOrT.name))
    Some(predicates.Ands(hasLabelOrTypePredicates: _*))
  }
}

case class MaterializedEntityProperty(mapExpr: commands.expressions.Expression, propertyKey: KeyToken)
    extends commands.expressions.Expression
    with Product with Serializable {

  private val property = commands.expressions.Property(mapExpr, propertyKey)

  def apply(row: ReadableRow, state: QueryState): AnyValue = mapExpr(row, state) match {
    case n: NodeValue         => n.properties().get(propertyKey.name)
    case r: RelationshipValue => r.properties().get(propertyKey.name)
    case _                    => property.apply(row, state)
  }

  override def rewrite(f: commands.expressions.Expression => commands.expressions.Expression)
    : commands.expressions.Expression = f(MaterializedEntityProperty(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  override def arguments: Seq[commands.expressions.Expression] = Seq(mapExpr)

  override def toString = s"$mapExpr.${propertyKey.name}"
}

case class MaterializedEntityHasLabel(entity: commands.expressions.Expression, label: KeyToken) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case value =>
      val node = CastSupport.castOrFail[NodeValue](value)

      var i = 0
      while (i < node.labels().intSize()) {
        if (node.labels().stringValue(i).equals(label.name)) {
          return IsTrue
        }

        i += 1
      }

      IsFalse
  }

  override def toString = s"$entity:${label.name}"

  override def rewrite(f: commands.expressions.Expression => commands.expressions.Expression)
    : commands.expressions.Expression =
    f(MaterializedEntityHasLabel(entity.rewrite(f), label.typedRewrite[KeyToken](f)))

  override def children: Seq[commands.expressions.Expression] = Seq(label, entity)

  override def arguments: Seq[commands.expressions.Expression] = Seq(entity)
}

case class MaterializedEntityHasType(entity: commands.expressions.Expression, typeToken: KeyToken) extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case value =>
      val relationship = CastSupport.castOrFail[RelationshipValue](value)
      IsMatchResult(relationship.`type`().equals(typeToken.name))
  }

  override def toString = s"$entity:${typeToken.name}"

  override def rewrite(f: commands.expressions.Expression => commands.expressions.Expression)
    : commands.expressions.Expression =
    f(MaterializedEntityHasType(entity.rewrite(f), typeToken.typedRewrite[KeyToken](f)))

  override def children: Seq[commands.expressions.Expression] = Seq(typeToken, entity)

  override def arguments: Seq[commands.expressions.Expression] = Seq(entity)
}

case class MaterializedEntityHasLabelOrType(entity: commands.expressions.Expression, labelOrType: String)
    extends Predicate {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = entity(ctx, state) match {

    case IsNoValue() =>
      IsUnknown

    case node: NodeValue =>
      var i = 0
      while (i < node.labels().intSize()) {
        if (node.labels().stringValue(i).equals(labelOrType)) {
          return IsTrue
        }

        i += 1
      }

      IsFalse

    case relationship: RelationshipValue =>
      IsMatchResult(relationship.`type`().equals(labelOrType))
  }

  override def toString = s"$entity:$labelOrType"

  override def rewrite(f: commands.expressions.Expression => commands.expressions.Expression)
    : commands.expressions.Expression =
    f(MaterializedEntityHasLabelOrType(entity.rewrite(f), labelOrType))

  override def children: Seq[commands.expressions.Expression] = Seq(entity)

  override def arguments: Seq[commands.expressions.Expression] = Seq(entity)
}

case class MaterializedEntityKeysFunction(expr: Expression) extends Expression {

  override def apply(ctx: ReadableRow, state: QueryState): AnyValue =
    expr(ctx, state) match {
      case n: NodeValue         => n.properties().keys()
      case r: RelationshipValue => r.properties().keys()
      case value =>
        CypherFunctions.keys(
          value,
          state.query,
          state.cursors.nodeCursor,
          state.cursors.relationshipScanCursor,
          state.cursors.propertyCursor
        )
    }

  override def rewrite(f: Expression => Expression): Expression = f(MaterializedEntityKeysFunction(expr.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(expr)

  override def children: Seq[AstNode[_]] = Seq(expr)
}

case class MaterializedDesugaredMapExpression(
  entityExpr: Expression,
  entries: Map[String, Expression],
  includeAll: Boolean
) extends NullInNullOutExpression(entityExpr) {

  override def compute(value: AnyValue, ctx: ReadableRow, state: QueryState): MapValue = {
    val allProps = value match {
      case _ if !includeAll     => VirtualValues.EMPTY_MAP
      case n: NodeValue         => n.properties()
      case r: RelationshipValue => r.properties()
      case _ =>
        CypherFunctions.properties(
          value,
          state.query,
          state.cursors.nodeCursor,
          state.cursors.relationshipScanCursor,
          state.cursors.propertyCursor
        ).asInstanceOf[MapValue]
    }

    if (entries.nonEmpty) {
      val mapBuilder = new MapValueBuilder()
      entries.foreach {
        case (k, e) => mapBuilder.add(k, e(ctx, state))
      }
      allProps.updatedWith(mapBuilder.build())
    } else {
      allProps
    }

  }

  override def rewrite(f: Expression => Expression): Expression =
    f(MaterializedDesugaredMapExpression(
      entityExpr.rewrite(f),
      entries.map { case (k, e) => k -> e.rewrite(f) },
      includeAll
    ))

  override def arguments: Seq[Expression] = Seq(entityExpr) ++ entries.values

  override def children: Seq[AstNode[_]] = Seq(entityExpr) ++ entries.values
}

case class MaterializedEntityLabelsFunction(nodeExpr: Expression) extends NullInNullOutExpression(nodeExpr) {

  override def compute(value: AnyValue, ctx: ReadableRow, state: QueryState): AnyValue = {
    value match {
      case n: NodeValue => n.labels()
      case _            => CypherFunctions.labels(value, state.query, state.cursors.nodeCursor)
    }
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(MaterializedEntityLabelsFunction(nodeExpr.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(nodeExpr)

  override def children: Seq[AstNode[_]] = Seq(nodeExpr)
}

case class MaterializedEntityTypeFunction(relExpression: Expression) extends NullInNullOutExpression(relExpression) {

  override def compute(value: AnyValue, ctx: ReadableRow, state: QueryState): AnyValue = {
    value match {
      case n: RelationshipValue => n.`type`()
      case _ => CypherFunctions.`type`(
          value,
          state.query,
          state.cursors.relationshipScanCursor,
          state.query.transactionalContext.dataRead
        )
    }
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(MaterializedEntityLabelsFunction(relExpression.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(relExpression)

  override def children: Seq[AstNode[_]] = Seq(relExpression)
}

case class MaterializedPropertyProjectionExpression(mapExpression: Expression, entries: Seq[PropertyProjectionEntry])
    extends NullInNullOutExpression(mapExpression) {

  override def compute(value: AnyValue, ctx: ReadableRow, state: QueryState): MapValue = {
    val allProps = value match {
      case n: NodeValue         => n.properties()
      case r: RelationshipValue => r.properties()
      case _ =>
        CypherFunctions.properties(
          value,
          state.query,
          state.cursors.nodeCursor,
          state.cursors.relationshipScanCursor,
          state.cursors.propertyCursor
        ).asInstanceOf[MapValue]
    }
    val mapBuilder = new MapValueBuilder()
    entries.foreach(e => mapBuilder.add(e.key, allProps.get(e.propertyKeyName.name)))
    mapBuilder.build()
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(MaterializedPropertyProjectionExpression(mapExpression.rewrite(f), entries))

  override def arguments: Seq[Expression] = Seq(mapExpression)

  override def children: Seq[AstNode[_]] = Seq(mapExpression)
}
