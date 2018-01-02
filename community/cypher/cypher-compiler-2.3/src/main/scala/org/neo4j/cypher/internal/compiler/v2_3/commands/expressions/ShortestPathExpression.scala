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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{PathExtractor, Pattern, ShortestPath, SingleNode, _}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsAllNodes, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SyntaxException
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, Path, PropertyContainer}

import scala.collection.Map

case class ShortestPathExpression(shortestPathPattern: ShortestPath, predicates: Seq[Predicate] = Seq.empty) extends Expression with PathExtractor {
  val pathPattern: Seq[Pattern] = Seq(shortestPathPattern)
  val pathIdentifiers = Set(shortestPathPattern.pathName, shortestPathPattern.relIterator.getOrElse(""))


  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    if (anyStartpointsContainNull(ctx)) {
      Stream.empty
    } else {
      getMatches(ctx)
    }
  }

  private def getMatches(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val start = getEndPoint(ctx, shortestPathPattern.left)
    val end = getEndPoint(ctx, shortestPathPattern.right)
    val (expander, nodePredicates) = addPredicates(ctx, makeRelationshipTypeExpander())
    val maybePredicate = if (predicates.isEmpty) None else Some(Ands(NonEmptyList.from(predicates)))
    /* This test is made after a full shortest path candidate has been produced,
     * accepting or disqualifying it as appropriate.
     */
    val shortestPathPredicate = createShortestPathPredicate(ctx, maybePredicate)

    if (shortestPathPattern.single) {
      val result = state.query.singleShortestPath(start, end, shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), expander, shortestPathPredicate, nodePredicates)
      if (!shortestPathPattern.allowZeroLength && result.forall(p => p.length() == 0))
        null
      else
        result.orNull
    }
    else {
      val result = state.query.allShortestPath(start, end, shortestPathPattern.maxDepth.getOrElse(Int.MaxValue), expander, shortestPathPredicate, nodePredicates)
      result.filter { p => shortestPathPattern.allowZeroLength || p.length() > 0 }
    }
  }

  private def createShortestPathPredicate(incomingCtx: ExecutionContext, maybePredicate: Option[Predicate])(implicit state: QueryState): KernelPredicate[Path] =
    new KernelPredicate[Path] {
      override def test(path: Path): Boolean = maybePredicate.map {
        predicate =>
          incomingCtx += shortestPathPattern.pathName -> path
          incomingCtx += shortestPathPattern.relIterator.get -> path.relationships()
          predicate.isTrue(incomingCtx)
      }.getOrElse(true)
    }

  private def getEndPoint(m: Map[String, Any], start: SingleNode): Node = m.getOrElse(start.name,
    throw new SyntaxException(s"To find a shortest path, both ends of the path need to be provided. Couldn't find `$start`")).asInstanceOf[Node]

  private def anyStartpointsContainNull(m: Map[String, Any]): Boolean =
    m(shortestPathPattern.left.name) == null || m(shortestPathPattern.right.name) == null

  override def children = Seq(shortestPathPattern)

  def arguments = Seq.empty

  def rewrite(f: (Expression) => Expression): Expression = f(ShortestPathExpression(shortestPathPattern.rewrite(f)))

  def calculateType(symbols: SymbolTable) = if (shortestPathPattern.single) CTPath else CTCollection(CTPath)

  def symbolTableDependencies = shortestPathPattern.symbolTableDependencies + shortestPathPattern.left.name + shortestPathPattern.right.name

  private def propertyExistsExpander(name: String) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      t.hasProperty(name)
    }
  }

  private def propertyNotExistsExpander(name: String) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      !t.hasProperty(name)
    }
  }

  private def cypherPositivePredicatesAsExpander(incomingCtx: ExecutionContext, name: String, predicate: Predicate)(implicit state: QueryState) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      predicate.isTrue(incomingCtx += (name -> t))
    }
  }

  private def cypherNegativePredicatesAsExpander(incomingCtx: ExecutionContext, name: String, predicate: Predicate)(implicit state: QueryState) = new KernelPredicate[PropertyContainer] {
    override def test(t: PropertyContainer): Boolean = {
      !predicate.isTrue(incomingCtx += (name -> t))
    }
  }

  private def addAllOrNoneRelationshipExpander(ctx: ExecutionContext, currentExpander: Expander, all: Boolean, predicate: Predicate, relName: String)(implicit state: QueryState): Expander = {
    predicate match {
      case PropertyExists(_, propertyKey) =>
        currentExpander.addRelationshipFilter(
          if (all) propertyExistsExpander(propertyKey.name)
          else propertyNotExistsExpander(propertyKey.name))
      case Not(PropertyExists(_, propertyKey)) =>
        currentExpander.addRelationshipFilter(
          if (all) propertyNotExistsExpander(propertyKey.name)
          else propertyExistsExpander(propertyKey.name))
      case _ => currentExpander.addRelationshipFilter(
        if (all) cypherPositivePredicatesAsExpander(ctx, relName, predicate)
        else cypherNegativePredicatesAsExpander(ctx, relName, predicate))
    }
  }

  private def addAllOrNoneNodeExpander(ctx: ExecutionContext, currentExpander: Expander, all: Boolean,
                                       predicate: Predicate, relName: String,
                                       currentNodePredicates: Seq[KernelPredicate[PropertyContainer]])
                                      (implicit state: QueryState): (Expander, Seq[KernelPredicate[PropertyContainer]]) = {
    val filter = predicate match {
      case PropertyExists(_, propertyKey) =>
        if (all) propertyExistsExpander(propertyKey.name)
        else propertyNotExistsExpander(propertyKey.name)
      case Not(PropertyExists(_, propertyKey)) =>
        if (all) propertyNotExistsExpander(propertyKey.name)
        else propertyExistsExpander(propertyKey.name)
      case _ =>
        if (all) cypherPositivePredicatesAsExpander(ctx, relName, predicate)
        else cypherNegativePredicatesAsExpander(ctx, relName, predicate)
    }
    (currentExpander.addNodeFilter(filter), currentNodePredicates :+ filter)
  }

  private def makeRelationshipTypeExpander(): Expander = if (shortestPathPattern.relTypes.isEmpty) {
    Expander.expanderForAllTypes(shortestPathPattern.dir)
  } else {
    shortestPathPattern.relTypes.foldLeft(Expander.typeDirExpander()) {
      case (e, t) => e.add(t, shortestPathPattern.dir)
    }
  }

  private def addPredicates(ctx: ExecutionContext, relTypeAndDirExpander: Expander)(implicit state: QueryState):
  (Expander, Seq[KernelPredicate[PropertyContainer]]) =
    if (predicates.isEmpty) (relTypeAndDirExpander, Seq())
    else
      predicates.foldLeft((relTypeAndDirExpander, Seq[KernelPredicate[PropertyContainer]]())) {
        case ((currentExpander, currentNodePredicates: Seq[KernelPredicate[PropertyContainer]]), predicate) =>
          predicate match {
            case NoneInCollection(RelationshipFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              (addAllOrNoneRelationshipExpander(ctx, currentExpander, all = false, innerPredicate, symbolName), currentNodePredicates)
            case AllInCollection(RelationshipFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              (addAllOrNoneRelationshipExpander(ctx, currentExpander, all = true, innerPredicate, symbolName), currentNodePredicates)
            case NoneInCollection(NodesFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = false, innerPredicate, symbolName, currentNodePredicates)
            case AllInCollection(NodesFunction(_), symbolName, innerPredicate) if doesNotDependOnFullPath(innerPredicate) =>
              addAllOrNoneNodeExpander(ctx, currentExpander, all = true, innerPredicate, symbolName, currentNodePredicates)
            case _ => (currentExpander, currentNodePredicates)
          }
      }

  private def doesNotDependOnFullPath(predicate: Predicate): Boolean = {
    (predicate.symbolTableDependencies intersect pathIdentifiers).isEmpty
  }

  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes, ReadsRelationships)
}
