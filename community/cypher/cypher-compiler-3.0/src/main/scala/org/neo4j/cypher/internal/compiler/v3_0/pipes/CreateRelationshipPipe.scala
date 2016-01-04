/*
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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{CreatesRelationship, CreatesAnyNode, CreatesNodesWithLabels, Effects}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CollectionSupport, IsMap}
import org.neo4j.cypher.internal.compiler.v3_0.mutation.{makeValueNeoSafe, GraphElementPropertyFunctions}
import org.neo4j.cypher.internal.frontend.v3_0.{InternalException, CypherTypeException}
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.Map

case class CreateRelationshipPipe(src: Pipe, key: String, startNode: String, typ: LazyType, endNode:String, properties: Option[Expression])(val estimatedCardinality: Option[Double] = None)
                           (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with CollectionSupport {

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      createRelationship(row, state)
    }
  }

  private def createRelationship(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val start = getNode(context, startNode)
    val end = getNode(context, endNode)
    val relationship = typ.typ(state.query) match {
      case Some(v) if v != LazyType.UNINITIALIZED => state.query.createRelationship(start.getId, end.getId, v)
      case _ => state.query.createRelationship(start, end, typ.name)
    }
    setProperties(context, state, relationship.getId)

    context += key -> relationship
  }

  private def getNode(row: ExecutionContext, name: String): Node =
    row.get(name) match {
     case Some(n: Node) => n
     case _ => throw new InternalException(s"Expected to find a node at $name but found nothing")
    }

 private def setProperties(context: ExecutionContext, state: QueryState, relId: Long) = {
   properties.foreach { expr =>
     expr(context)(state) match {
       case _: Node | _: Relationship => throw new
           CypherTypeException("Parameter provided for relationship creation is not a Map")
       case IsMap(f) =>
         val propertiesMap: Map[String, Any] = f(state.query)
         propertiesMap.foreach {
           case (k, v) =>
             //do not set properties for null values
             if (v != null) {
               val propertyKeyId = state.query.getOrCreatePropertyKeyId(k)
               state.query.relationshipOps.setProperty(relId, propertyKeyId, makeValueNeoSafe(v))
             }
         }
       case _ =>
         throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
     }
   }
 }

  def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "CreateRelationship", variables)

  def symbols = src.symbols.add(key, CTRelationship)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    CreateRelationshipPipe(onlySource, key, startNode, typ, endNode, properties)(estimatedCardinality)
  }

  override def localEffects = Effects(CreatesRelationship(typ.name))
}
