/**
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import org.neo4j.cypher.internal.compiler.v1_9.mutation._
import org.neo4j.graphdb.{GraphDatabaseService, NotInTransactionException}
import org.neo4j.cypher.{SyntaxException, ParameterWrongTypeException, InternalException}
import org.neo4j.cypher.internal.compiler.v1_9.mutation.CreateUniqueAction
import scala.Some
import org.neo4j.cypher.internal.compiler.v1_9.mutation.CreateNode
import collection.Map
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Identifier, Expression}
import org.neo4j.cypher.internal.compiler.v1_9.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.data.SimpleVal

class ExecuteUpdateCommandsPipe(source: Pipe, db: GraphDatabaseService, commands: Seq[UpdateAction])
  extends PipeWithSource(source) {

  assertNothingIsCreatedWhenItShouldNot()

  protected def internalCreateResults(input: Iterator[ExecutionContext],state: QueryState) = input.flatMap {
    case ctx => executeMutationCommands(ctx, state, commands.size == 1)
  }

  private def executeMutationCommands(ctx: ExecutionContext,
                                      state: QueryState,
                                      singleCommand: Boolean): Traversable[ExecutionContext] =
    try {
      commands.foldLeft(Traversable(ctx))((context, cmd) => context.flatMap(c => exec(cmd, c, state, singleCommand)))
    } catch {
      case e: NotInTransactionException => throw new InternalException("Expected to be in a transaction at this point", e)
    }

  private def exec(cmd: UpdateAction,
                   ctx: ExecutionContext,
                   state: QueryState,
                   singleCommand: Boolean): Traversable[ExecutionContext] = {
    val result = cmd.exec(ctx, state)
    if (result.size > 1 && !singleCommand)
      throw new ParameterWrongTypeException("If you create multiple elements, you can only create one of each.")
    result
  }


  private def extractEntitiesWithProperties(action: UpdateAction): Seq[NamedExpectation] = action match {
    case CreateNode(key, props)                      => Seq(NamedExpectation(key, props))
    case CreateRelationship(key, from, to, _, props) => Seq(NamedExpectation(key, props)) ++ extractIfEntity(from) ++ extractIfEntity(to)
    case CreateUniqueAction(links@_*)                         => links.flatMap(l => Seq(l.start, l.end, l.rel))
    case _                                                    => Seq()
  }


  def extractIfEntity(from: (Expression, Map[String, Expression])): Option[NamedExpectation] = {
    from match {
      case (Identifier(key), props) => Some(NamedExpectation(key, props))
      case _                        => None
    }
  }

  private def assertNothingIsCreatedWhenItShouldNot() {
    val entitiesAndProps: Seq[NamedExpectation] = commands.flatMap(cmd => extractEntitiesWithProperties(cmd))
    val entitiesWithProps = entitiesAndProps.filter(_.properties.nonEmpty)

    entitiesWithProps.foreach(l => if (source.symbols.keys.contains(l.name))
      throw new SyntaxException("Can't create `%s` with properties here. It already exists in this context".format(l.name))
    )
  }

  override def executionPlanDescription =
    source.executionPlanDescription.andThen(this, "UpdateGraph", "commands" -> SimpleVal.fromIterable(commands))

  def symbols = source.symbols.add(commands.flatMap(_.identifiers).toMap)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    commands.foreach(_.throwIfSymbolsMissing(symbols))
  }
}
