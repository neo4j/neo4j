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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_0.data.{MapVal, SeqVal}
import mutation._
import symbols._
import org.neo4j.cypher.{SyntaxException, ParameterWrongTypeException, InternalException}
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.graphdb.NotInTransactionException
import collection.mutable

case class ExecuteUpdateCommandsPipe(source: Pipe, commands: Seq[UpdateAction])
  extends PipeWithSource(source) with CollectionSupport with NoLushEntityCreation {

  protected def internalCreateResults(input: Iterator[ExecutionContext],state: QueryState) = input.flatMap {
    case ctx => executeMutationCommands(ctx, state, commands.size == 1)
  }

  val allKeys = commands.flatMap( c => c.identifiers.map(_._1) )

  private def executeMutationCommands(ctx: ExecutionContext,
                                      state: QueryState,
                                      singleCommand: Boolean): Iterator[ExecutionContext] =
    try {
      commands.foldLeft(Iterator(ctx))((context, cmd) => context.flatMap(c => exec(cmd, c, state, singleCommand)))
    } catch {
      case e: NotInTransactionException =>
        throw new InternalException("Expected to be in a transaction at this point", e)
    }

  private def exec(cmd: UpdateAction,
                   ctx: ExecutionContext,
                   state: QueryState,
                   singleCommand: Boolean): Iterator[ExecutionContext] = {

    val result: Iterator[ExecutionContext] = cmd.exec(ctx, state)

    cmd match {
      case _:CreateNode if !singleCommand =>
        singleOr(result, new ParameterWrongTypeException("If you create multiple elements, you can only create one of each."))
      case _ =>
        result
    }
  }

  override def executionPlanDescription =
    source.executionPlanDescription.andThen(this, "UpdateGraph",
      "commands" -> SeqVal(commands.map(command => MapVal(command.description.toMap)))
    )

  def symbols = source.symbols.add(commands.flatMap(_.identifiers).toMap)

  def sourceSymbols: SymbolTable = source.symbols
}

// TODO: Write unit tests for this
trait NoLushEntityCreation {
  def commands: Seq[UpdateAction]

  def sourceSymbols: SymbolTable

  assertNothingIsCreatedWhenItShouldNot()

  private def extractEntities(action: UpdateAction): Seq[NamedExpectation] = action match {
    case CreateNode(key, props, labels) =>
      Seq(NamedExpectation(key, props, labels))
    case CreateRelationship(key, from, to, _, props) =>
      Seq(NamedExpectation(key, props, Seq.empty)) ++ extractIfEntity(from) ++ extractIfEntity(to)
    case CreateUniqueAction(links@_*) =>
      links.flatMap(l => Seq(l.start, l.end, l.rel))
    case MergePatternAction(_, _, _, Some(updates), _) =>
      updates.flatMap(extractEntities(_))
    case _ =>
      Seq()
  }

  private def extractIfEntity(from: RelationshipEndpoint): Option[NamedExpectation] =
    from match {
      case RelationshipEndpoint(Identifier(key), props, labels) => Some(NamedExpectation(key, props, labels))
      case _                                                    => None
    }

  private def assertNothingIsCreatedWhenItShouldNot() {
    var symbols = sourceSymbols
    val lushNodes = new mutable.HashMap[String, NamedExpectation]

    commands.foreach {
      cmd =>
        val namedExpectations = extractEntities(cmd)

        // If we find multiple lush elements, make sure they all have the same lushness
        namedExpectations.filter(expectation => !expectation.bare).foreach {
          lushExpectation =>
            lushNodes.get(lushExpectation.name) match {
              case None if symbols.hasIdentifierNamed(lushExpectation.name) => failOn(lushExpectation.name)
              case None                                                     => lushNodes += lushExpectation.name -> lushExpectation
              case Some(x) if x != lushExpectation                          => failOn(lushExpectation.name)
              case Some(x)                                                  => // same thing we've already seen. move along
            }
        }

        symbols = symbols.add(cmd.identifiers.toMap)
    }
  }

  private def failOn(name:String)=
    throw new SyntaxException("Can't create `%s` with properties or labels here. It already exists in this context".format(name))
}

