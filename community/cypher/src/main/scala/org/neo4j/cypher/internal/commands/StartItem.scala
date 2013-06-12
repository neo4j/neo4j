/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import org.neo4j.cypher.internal.commands.expressions._
import org.neo4j.cypher.internal.mutation._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.parser._
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.PatternException
import org.neo4j.cypher.internal.parser.ParsedEntity
import org.neo4j.cypher.internal.mutation.PropertySetAction
import org.neo4j.cypher.internal.mutation.MergeNodeAction
import org.neo4j.cypher.internal.mutation.CreateUniqueAction
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.mutation.CreateNode
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.mutation.CreateRelationship
import org.neo4j.cypher.internal.parser.OnAction
import org.neo4j.cypher.internal.commands.expressions.Nullable
import org.neo4j.cypher.internal.commands.expressions.Property
import scala.collection.mutable

abstract class StartItem(val identifierName: String, val args: Map[String, String])
  extends TypeSafe with AstNode[StartItem] {
  def mutating: Boolean
  def name: String = getClass.getSimpleName
}

trait ReadOnlyStartItem extends StartItem {
  def mutating = false

  def children: Seq[AstNode[_]] = Nil

  def throwIfSymbolsMissing(symbols: SymbolTable) {}
  def symbolTableDependencies = Set.empty
  def rewrite(f: (Expression) => Expression) = this
}

case class RelationshipById(varName: String, expression: Expression)
  extends StartItem(varName, Map.empty) with ReadOnlyStartItem

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "key" -> key.toString(), "expr" -> expression.toString()))
  with ReadOnlyStartItem

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "query" -> query.toString()))
  with ReadOnlyStartItem

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "key" -> key.toString(), "expr" -> expression.toString()))
  with ReadOnlyStartItem

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "query" -> query.toString()))
  with ReadOnlyStartItem

trait Hint

case class SchemaIndex(identifier: String, label: String, property: String, query: Option[Expression])
  extends StartItem(identifier, Map("label" -> label, "property" -> property) ++ query.map("query" -> _.toString()))
  with ReadOnlyStartItem with Hint

case class NodeById(varName: String, expression: Expression)
  extends StartItem(varName, Map("name" -> expression.toString()))
  with ReadOnlyStartItem

case class NodeByLabel(varName: String, label: String)
  extends StartItem(varName, Map("label" -> label.toString)) with ReadOnlyStartItem with Hint

case class AllNodes(columnName: String) extends StartItem(columnName, Map.empty) with ReadOnlyStartItem

case class AllRelationships(columnName: String) extends StartItem(columnName, Map.empty) with ReadOnlyStartItem

//We need to wrap the inner classes to be able to have two different rewrite methods
abstract class UpdatingStartItem(val updateAction: UpdateAction, name: String) extends StartItem(name, Map.empty) {

  override def mutating = true
  override def children = Seq(updateAction)
  override def throwIfSymbolsMissing(symbols: SymbolTable) {
    updateAction.throwIfSymbolsMissing(symbols)
  }
  override def symbolTableDependencies = updateAction.symbolTableDependencies
}

case class CreateNodeStartItem(inner: CreateNode) extends UpdatingStartItem(inner, inner.key) {
  override def rewrite(f: (Expression) => Expression) = CreateNodeStartItem(inner.rewrite(f))
}

case class CreateRelationshipStartItem(inner: CreateRelationship) extends UpdatingStartItem(inner, inner.key) {
  override def rewrite(f: (Expression) => Expression) = CreateRelationshipStartItem(inner.rewrite(f))
}

case class CreateUniqueStartItem(inner: CreateUniqueAction) extends UpdatingStartItem(inner, "oh noes") {
  override def rewrite(f: (Expression) => Expression) = CreateUniqueStartItem(inner.rewrite(f))
}

case class MergeNodeStartItem(inner: MergeNodeAction) extends UpdatingStartItem(inner, inner.identifier) {
  override def rewrite(f: (Expression) => Expression) = MergeNodeStartItem(inner.rewrite(f))
}

case class MergeAst(patterns: Seq[AbstractPattern], onActions: Seq[OnAction]) extends UpdatingStartItem(PlaceHolder, "") {
  override def rewrite(f: (Expression) => Expression) = MergeAst(patterns.map(_.rewrite(f)), onActions)

  def nextStep(): Seq[MergeNodeAction] = {
    val actionsMap = new mutable.HashMap[(String, Action), mutable.Set[UpdateAction]] with mutable.MultiMap[(String, Action), UpdateAction]

    for (
      actions <- onActions;
      action <- actions.set) {
       actionsMap.addBinding((actions.identifier, actions.verb), action)
    }

    patterns.map {
      case ParsedEntity(name, _, props, labelsNames, _) =>
        val labelPredicates = labelsNames.map(labelName => HasLabel(Identifier(name), labelName))
        val propertyPredicates = props.map {
          case (propertyKey, expression) => Equals(Nullable(Property(Identifier(name), propertyKey)), expression)
        }
        val predicates = labelPredicates ++ propertyPredicates

        val labelActions = labelsNames.map(labelName => LabelAction(Identifier(name), LabelSetOp, Seq(labelName)))
        val propertyActions = props.map {
          case (propertyKey, expression) => PropertySetAction(Property(Identifier(name), propertyKey), expression)
        }

        val actionsFromOnCreateClause = actionsMap.get((name, On.Create)).getOrElse(Set.empty)
        val actionsFromOnMatchClause = actionsMap.get((name, On.Match)).getOrElse(Set.empty)

        val onCreate: Seq[UpdateAction] = labelActions ++ propertyActions ++ actionsFromOnCreateClause

        MergeNodeAction(name, predicates, onCreate, actionsFromOnMatchClause.toSeq, None)

      case _                                            =>
        throw new PatternException("MERGE only supports single node patterns")
    }
  }
}

object NodeById {
  def apply(varName: String, id: Long*) = new NodeById(varName, Literal(id))
}

object RelationshipById {
  def apply(varName: String, id: Long*) = new RelationshipById(varName, Literal(id))
}

case object PlaceHolder extends UpdateAction {
  def children: Seq[AstNode[_]] = Seq.empty

  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] =
    throw new ThisShouldNotHappenError("Andres", "This object should not make it to a runtime execution plan")

  def identifiers: Seq[(String, CypherType)] = Seq.empty

  def rewrite(f: (Expression) => Expression): UpdateAction = PlaceHolder

  def throwIfSymbolsMissing(symbols: SymbolTable) {}

  def symbolTableDependencies: Set[String] = Set.empty
}