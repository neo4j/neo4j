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
package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands

import PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_2._
import commands.{expressions => commandexpressions, values => commandvalues, Predicate => CommandPredicate}
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Expression => CommandExpression, ProjectedPath}
import commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.{UnresolvedRelType, UnresolvedProperty}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.graphdb.Direction

object ExpressionConverters {

  implicit class ExpressionConverter(val expression: ast.Expression) extends AnyVal {
    def asCommandExpression: CommandExpression = expression match {
      case e: ast.Null => e.asCommandNull
      case e: ast.True => e.asCommandTrue
      case e: ast.False => e.asCommandFalse
      case e: ast.Literal => e.asCommandLiteral
      case e: ast.Identifier => e.asCommandIdentifier
      case e: ast.Or => e.asCommandOr
      case e: ast.Xor => e.asCommandXor
      case e: ast.And => e.asCommandAnd
      case e: ast.Ands => e.asCommandAnds
      case e: ast.Ors => e.asCommandOrs
      case e: ast.Not => e.asCommandNot
      case e: ast.Equals => e.asCommandEquals
      case e: ast.NotEquals => e.asCommandNotEquals
      case e: ast.RegexMatch => e.asCommandRegex
      case e: ast.In => e.asCommandIn
      case e: ast.IsNull => e.asCommandIsNull
      case e: ast.IsNotNull => e.asCommandIsNotNull
      case e: ast.LessThan => e.asCommandLessThan
      case e: ast.LessThanOrEqual => e.asCommandLessThanOrEqual
      case e: ast.GreaterThan => e.asCommandGreaterThan
      case e: ast.GreaterThanOrEqual => e.asCommandGreaterThanOrEqual
      case e: ast.Add => e.asCommandAdd
      case e: ast.UnaryAdd => e.asCommandAdd
      case e: ast.Subtract => e.asCommandSubtract
      case e: ast.UnarySubtract => e.asCommandSubtract
      case e: ast.Multiply => e.asCommandMultiply
      case e: ast.Divide => e.asCommandDivide
      case e: ast.Modulo => e.asCommandModulo
      case e: ast.Pow => e.asCommandPow
      case e: ast.FunctionInvocation => e.asCommandFunction
      case e: ast.CountStar => e.asCommandCountStar
      case e: ast.Property => e.asCommandProperty
      case e: ast.Parameter => e.asCommandParameter
      case e: ast.CaseExpression => e.asCommandCase
      case e: ast.PatternExpression => e.asCommandPath
      case e: ast.ShortestPathExpression => e.asCommandShortestPath
      case e: ast.HasLabels => e.asCommandLabelsPredicate
      case e: ast.Collection => e.asCommandCollection
      case e: ast.MapExpression => e.asCommandMap
      case e: ast.CollectionSlice => e.asCommandCollectionSlice
      case e: ast.CollectionIndex => e.asCommandCollectionIndex
      case e: ast.FilterExpression => e.asCommandFilter
      case e: ast.ExtractExpression => e.asCommandExtract
      case e: ast.ListComprehension => e.asCommandListComprehension
      case e: ast.AllIterablePredicate => e.asCommandAllInCollection
      case e: ast.AnyIterablePredicate => e.asCommandAnyInCollection
      case e: ast.NoneIterablePredicate => e.asCommandNoneInCollection
      case e: ast.SingleIterablePredicate => e.asCommandSingleInCollection
      case e: ast.ReduceExpression => e.asCommandReduce
      case e: ast.PathExpression => e.asCommandProjectedPath
      case e: ast.NestedPipeExpression => e.asPipeCommand
      case e: ast.GetDegree => e.asCommandGetDegree
      case _ =>
        throw new ThisShouldNotHappenError("cleishm", s"Unknown expression type during transformation (${expression.getClass})")
    }

    def asCommandPredicate: CommandPredicate = expression match {
      case e: ast.PatternExpression => commands.NonEmpty(e.asCommandExpression)
      case e: ast.FilterExpression => commands.NonEmpty(e.asCommandExpression)
      case e: ast.ExtractExpression => commands.NonEmpty(e.asCommandExpression)
      case e: ast.ListComprehension => commands.NonEmpty(e.asCommandExpression)
      case e => e.asCommandExpression match {
        case c: commands.Predicate => c
        case c => commands.CoercedPredicate(c)
      }
    }
  }

  implicit class GetDegreeConverter(val original: ast.GetDegree) extends AnyVal {
    def asCommandGetDegree = {
      val typ = original.relType.map( relType => UnresolvedRelType(relType.name))
      commandexpressions.GetDegree(original.node.asCommandExpression, typ, original.dir)
    }
  }

  implicit class ExpressionOptionConverter(val expression: Option[ast.Expression]) extends AnyVal {
    def asCommandExpression: Option[CommandExpression] =
      expression.map(_.asCommandExpression)
  }

  implicit class ExpressionSeqConverter(val expressions: Seq[ast.Expression]) extends AnyVal {
    def asCommandExpressions: Seq[CommandExpression] =
      expressions.map(_.asCommandExpression)
  }

  implicit class LiteralConverter(val e: ast.Literal) extends AnyVal {
    def asCommandLiteral =
      commandexpressions.Literal(e.value)
  }

  implicit class IdentifierConverter(val e: ast.Identifier) extends AnyVal {
    def asCommandIdentifier =
      commands.expressions.Identifier(e.name)
  }

  implicit class NullConverter(val e: ast.Null) extends AnyVal {
    def asCommandNull: CommandExpression =
      commandexpressions.Literal(null)
  }

  implicit class TrueConverter(val e: ast.True) extends AnyVal {
    def asCommandTrue =
      commands.True()
  }

  implicit class FalseConverter(val e: ast.False) extends AnyVal {
    def asCommandFalse =
      commands.Not(commands.True())
  }

  implicit class CountStarConverter(val e: ast.CountStar) extends AnyVal {
    def asCommandCountStar =
      commandexpressions.CountStar()
  }

  implicit class PropertyConverter(val e: ast.Property) extends AnyVal {
    def asCommandProperty =
      commandexpressions.Property(e.map.asCommandExpression, PropertyKey(e.propertyKey.name))
  }

  implicit class ParameterConverter(val e: ast.Parameter) extends AnyVal {
    def asCommandParameter =
      commandexpressions.ParameterExpression(e.name)
  }

  implicit class OrConverter(val e: ast.Or) extends AnyVal {
    def asCommandOr =
      commands.Or(e.lhs.asCommandPredicate, e.rhs.asCommandPredicate)
  }

  implicit class XorConverter(val e: ast.Xor) extends AnyVal {
    def asCommandXor =
      commands.Xor(e.lhs.asCommandPredicate, e.rhs.asCommandPredicate)
  }

  implicit class AndConverter(val e: ast.And) extends AnyVal {
    def asCommandAnd =
      commands.And(e.lhs.asCommandPredicate, e.rhs.asCommandPredicate)
  }

  implicit class NotConverter(val e: ast.Not) extends AnyVal {
    def asCommandNot =
      commands.Not(e.rhs.asCommandPredicate)
  }

   implicit class OrsConverter(val e: ast.Ors) extends AnyVal {
    def asCommandOrs = commands.Ors(e.exprs.map(_.asCommandPredicate).toList)
  }

  implicit class AndsConverter(val e: ast.Ands) extends AnyVal {
    def asCommandAnds = commands.Ands(e.exprs.map(_.asCommandPredicate).toList)
  }

  implicit class EqualsConverter(val e: ast.Equals) extends AnyVal {
    def asCommandEquals =
      commands.Equals(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class NotEqualsConverter(val e: ast.NotEquals) extends AnyVal {
    def asCommandNotEquals =
      commands.Not(commands.Equals(e.lhs.asCommandExpression, e.rhs.asCommandExpression))
  }

  implicit class RegexMatchConverter(val e: ast.RegexMatch) extends AnyVal {
    def asCommandRegex = e.rhs.asCommandExpression match {
      case literal: commandexpressions.Literal =>
        commands.LiteralRegularExpression(e.lhs.asCommandExpression, literal)
      case command =>
        commands.RegularExpression(e.lhs.asCommandExpression, command)
    }
  }

  implicit class InConverter(val e: ast.In) extends AnyVal {
    def asCommandIn =
      commands.AnyInCollection(
        e.rhs.asCommandExpression,
        "-_-INNER-_-",
        commands.Equals(
          e.lhs.asCommandExpression,
          commandexpressions.Identifier("-_-INNER-_-")
        )
      )
  }

  implicit class IsNullConverter(val e: ast.IsNull) extends AnyVal {
    def asCommandIsNull =
      commands.IsNull(e.lhs.asCommandExpression)
  }

  implicit class IsNotNullConverter(val e: ast.IsNotNull) extends AnyVal {
    def asCommandIsNotNull =
      commands.Not(commands.IsNull(e.lhs.asCommandExpression))
  }

  implicit class LessThanConverter(val e: ast.LessThan) extends AnyVal {
    def asCommandLessThan =
      commands.LessThan(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class LessThanOrEqualConverter(val e: ast.LessThanOrEqual) extends AnyVal {
    def asCommandLessThanOrEqual =
      commands.LessThanOrEqual(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class GreaterThanConverter(val e: ast.GreaterThan) extends AnyVal {
    def asCommandGreaterThan =
      commands.GreaterThan(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class GreaterThanOrEqualConverter(val e: ast.GreaterThanOrEqual) extends AnyVal {
    def asCommandGreaterThanOrEqual =
      commands.GreaterThanOrEqual(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class AddConverter(val e: ast.Add) extends AnyVal {
    def asCommandAdd =
      commandexpressions.Add(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class UnaryAddConverter(val e: ast.UnaryAdd) extends AnyVal {
    def asCommandAdd = e.rhs.asCommandExpression
  }

  implicit class SubtractConverter(val e: ast.Subtract) extends AnyVal {
    def asCommandSubtract =
      commandexpressions.Subtract(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class UnarySubtactConverter(val e: ast.UnarySubtract) extends AnyVal {
    def asCommandSubtract = commandexpressions.Subtract(commandexpressions.Literal(0), e.rhs.asCommandExpression)
  }

  implicit class MultiplyConverter(val e: ast.Multiply) extends AnyVal {
    def asCommandMultiply = commandexpressions.Multiply(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class DivideConverter(val e: ast.Divide) extends AnyVal {
    def asCommandDivide = commandexpressions.Divide(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class ModuloConverter(val e: ast.Modulo) extends AnyVal {
    def asCommandModulo = commandexpressions.Modulo(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class PowConverter(val e: ast.Pow) extends AnyVal {
    def asCommandPow = commandexpressions.Pow(e.lhs.asCommandExpression, e.rhs.asCommandExpression)
  }

  implicit class CaseExpressionConverter(val e: ast.CaseExpression) extends AnyVal {
    def asCommandCase = e.expression match {
      case Some(innerExpression) =>
        val legacyAlternatives = e.alternatives.map { a => (a._1.asCommandExpression, a._2.asCommandExpression) }
        commandexpressions.SimpleCase(innerExpression.asCommandExpression, legacyAlternatives, e.default.asCommandExpression)
      case None =>
        val predicateAlternatives = e.alternatives.map { a => (a._1.asCommandPredicate, a._2.asCommandExpression) }
        commandexpressions.GenericCase(predicateAlternatives, e.default.asCommandExpression)
    }
  }

  implicit class PatternPathConverter(val e: ast.PatternExpression) extends AnyVal {
    def asCommandPath =
      commands.PathExpression(e.pattern.asLegacyPatterns)
  }

  implicit class ShortestPathConverter(val e: ast.ShortestPathExpression) extends AnyVal {
    def asCommandShortestPath =
      commandexpressions.ShortestPathExpression(e.pattern.asLegacyPatterns(None).head)
  }

  implicit class HasLabelsConverter(val e: ast.HasLabels) extends AnyVal {
    def asCommandLabelsPredicate = e.labels.map {
      l => commands.HasLabel(e.expression.asCommandExpression, commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)): CommandPredicate
    } reduceLeft { commands.And(_, _) }
  }

  implicit class CollectionConverter(val e: ast.Collection) extends AnyVal {
    def asCommandCollection: commandexpressions.Collection =
      commandexpressions.Collection(e.expressions.asCommandExpressions:_*)
  }

  implicit class MapConverter(val e: ast.MapExpression) extends AnyVal {
    def asCommandMap: commandexpressions.LiteralMap = {
      val literalMap: Map[String, CommandExpression] = e.items.map {
        case (id, ex) => id.name -> ex.asCommandExpression
      }.toMap
      commandexpressions.LiteralMap(literalMap)
    }
  }

  implicit class CollectionSliceConverter(val e: ast.CollectionSlice) extends AnyVal {
    def asCommandCollectionSlice: commandexpressions.CollectionSliceExpression =
      commandexpressions.CollectionSliceExpression(e.collection.asCommandExpression, e.from.asCommandExpression, e.to.asCommandExpression)
  }

  implicit class CollectionIndexConverter(val e: ast.CollectionIndex) extends AnyVal {
    def asCommandCollectionIndex: commandexpressions.CollectionIndex =
      commandexpressions.CollectionIndex(e.collection.asCommandExpression, e.idx.asCommandExpression)
  }

  implicit class FilterConverter(val e: ast.FilterExpression) extends AnyVal {
    def asCommandFilter: commandexpressions.FilterFunction =
      commandexpressions.FilterFunction(e.expression.asCommandExpression, e.identifier.name, e.innerPredicate.map(_.asCommandPredicate).getOrElse(commands.True()))
  }

  implicit class ExtractConverter(val e: ast.ExtractExpression) extends AnyVal {
    def asCommandExtract: commandexpressions.ExtractFunction =
      commandexpressions.ExtractFunction(e.expression.asCommandExpression, e.identifier.name, e.scope.extractExpression.get.asCommandExpression)
  }

  implicit class ListComprehensionConverter(val e: ast.ListComprehension) extends AnyVal {
    def asCommandListComprehension: CommandExpression = {
      val filter = e.innerPredicate match {
        case Some(_: ast.True) | None =>
          e.expression.asCommandExpression
        case Some(inner) =>
          commandexpressions.FilterFunction(e.expression.asCommandExpression, e.identifier.name, inner.asCommandPredicate)
      }
      e.extractExpression match {
        case Some(extractExpression) =>
          commandexpressions.ExtractFunction(filter, e.identifier.name, extractExpression.asCommandExpression)
        case None =>
          filter
      }
    }
  }

  implicit class AllIterableConverter(val e: ast.AllIterablePredicate) extends AnyVal {
    def asCommandAllInCollection: commands.AllInCollection =
      commands.AllInCollection(e.expression.asCommandExpression, e.identifier.name, e.innerPredicate.map(_.asCommandPredicate).getOrElse(commands.True()))
  }

  implicit class AnyIterableConverter(val e: ast.AnyIterablePredicate) extends AnyVal {
    def asCommandAnyInCollection: commands.AnyInCollection =
      commands.AnyInCollection(e.expression.asCommandExpression, e.identifier.name, e.innerPredicate.map(_.asCommandPredicate).getOrElse(commands.True()))
  }

  implicit class NoneIterableConverter(val e: ast.NoneIterablePredicate) extends AnyVal {
    def asCommandNoneInCollection: commands.NoneInCollection =
      commands.NoneInCollection(e.expression.asCommandExpression, e.identifier.name, e.innerPredicate.map(_.asCommandPredicate).getOrElse(commands.True()))
  }

  implicit class SingleIterableConverter(val e: ast.SingleIterablePredicate) extends AnyVal {
    def asCommandSingleInCollection: commands.SingleInCollection =
      commands.SingleInCollection(e.expression.asCommandExpression, e.identifier.name, e.innerPredicate.map(_.asCommandPredicate).getOrElse(commands.True()))
  }

  implicit class ReduceConverter(val e: ast.ReduceExpression) extends AnyVal {
    def asCommandReduce: commandexpressions.ReduceFunction =
      commandexpressions.ReduceFunction(e.collection.asCommandExpression, e.identifier.name, e.expression.asCommandExpression, e.accumulator.name, e.init.asCommandExpression)
  }

  implicit class FunctionConverter(val e: ast.FunctionInvocation) extends AnyVal {
    def asCommandFunction: CommandExpression = e.function.get.asCommandExpression(e)
  }

  implicit class NestedExpressionPipeConverter(val e: ast.NestedPipeExpression) extends AnyVal {
    def asPipeCommand: CommandExpression = commandexpressions.NestedPipeExpression(e.pipe, e.path.asCommandProjectedPath)
  }

  implicit class PathConverter(val e: ast.PathExpression) extends AnyVal {
    import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.ProjectedPath._

    def asCommandProjectedPath: commandexpressions.ProjectedPath = {
      def project(pathStep: PathStep): Projector = pathStep match {

        case NodePathStep(Identifier(node), next) =>
          singleNodeProjector(node, project(next))

        case SingleRelationshipPathStep(Identifier(rel), Direction.INCOMING, next) =>
          singleIncomingRelationshipProjector(rel, project(next))

        case SingleRelationshipPathStep(Identifier(rel), Direction.OUTGOING, next) =>
          singleOutgoingRelationshipProjector(rel, project(next))

        case SingleRelationshipPathStep(Identifier(rel), Direction.BOTH, next) =>
          singleUndirectedRelationshipProjector(rel, project(next))

        case MultiRelationshipPathStep(Identifier(rel), Direction.INCOMING, next) =>
          multiIncomingRelationshipProjector(rel, project(next))

        case MultiRelationshipPathStep(Identifier(rel), Direction.OUTGOING, next) =>
          multiOutgoingRelationshipProjector(rel, project(next))

        case MultiRelationshipPathStep(Identifier(rel), Direction.BOTH, next) =>
          multiUndirectedRelationshipProjector(rel, project(next))

        case NilPathStep =>
          nilProjector
      }

      val projector = project(e.step)
      val dependencies = e.step.dependencies.map(_.name)

      ProjectedPath(dependencies, projector)
    }
  }
}
