/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheckResult.success
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheckResult, SemanticState, _}

sealed trait GraphReturnItem extends ASTNode with ASTParticle {
  def graphs: Set[SingleGraphAs]

  def newSource: Option[SingleGraphAs] = None
  def newTarget: Option[SingleGraphAs] = None

  def filter(pred: SingleGraphAs => Boolean): Option[GraphReturnItem]
}

final case class ReturnedGraph(item: SingleGraphAs)(val position: InputPosition) extends GraphReturnItem {
  override def graphs: Set[SingleGraphAs] = Set(item)

  override def filter(pred: SingleGraphAs => Boolean): Option[GraphReturnItem] =
    if (pred(item)) Some(this) else None
}

final case class NewTargetGraph(target: SingleGraphAs)(val position: InputPosition) extends GraphReturnItem {
  override def graphs: Set[SingleGraphAs] = Set(target)
  override def newTarget = Some(target)

  override def filter(pred: SingleGraphAs => Boolean): Option[GraphReturnItem] =
    if (pred(target)) Some(this) else None
}

final case class NewContextGraphs(source: SingleGraphAs,
                                  override val newTarget: Option[SingleGraphAs] = None)
                                 (val position: InputPosition) extends GraphReturnItem {

  override def graphs: Set[SingleGraphAs] = Set(source) ++ newTarget.toSet

  override def newSource: Option[SingleGraphAs] = Some(source)

  override def filter(pred: SingleGraphAs => Boolean): Option[GraphReturnItem] =
    if (pred(source))
      newTarget.map(pred(_)) match {
        case Some(false) => Some(NewContextGraphs(source, None)(position))
        case _ => Some(this)
      }
    else
      newTarget.flatMap { target => if (pred(target)) Some(NewTargetGraph(target)(position)) else None }
}

object PassAllGraphReturnItems {
  def apply(position: InputPosition): GraphReturnItems =
    GraphReturnItems(includeExisting = true, Seq.empty)(position)
}

final case class GraphReturnItems(includeExisting: Boolean, items: Seq[GraphReturnItem])
                                 (val position: InputPosition)
  extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  def isGraphsStarOnly: Boolean = includeExisting && items.isEmpty

  val graphs: Seq[SingleGraphAs] = items.flatMap(_.graphs)

  val singleGraph: Option[SingleGraphAs] = if (!includeExisting && graphs.nonEmpty && graphs.tail.isEmpty) graphs.headOption else None
  val newSource: Option[SingleGraphAs] = singleGraph orElse items.flatMap(_.newSource).headOption
  val newTarget: Option[SingleGraphAs] = items.flatMap(_.newTarget).headOption orElse newSource

  override def semanticCheck: SemanticCheck =
    unless(items.isEmpty) { requireMultigraphSupport("Projecting and returning graphs", position) } chain(
      graphs.semanticCheck chain
      checkNoMultipleSources chain
      checkNoMultipleTargets chain
      checkUniqueGraphReference
    ).ifFeatureEnabled(SemanticFeature.MultipleGraphs)

  def declareGraphs(previousScope: Scope, isReturn: Boolean): SemanticCheck = {
    val updateContext = (s: SemanticState) => {
      if (isReturn)
        s.removeContextGraphs()
      else {
        val previousContext = previousScope.contextGraphs
        val newSourceName = newSource.flatMap(_.name(previousContext))
        val newTargetName = newTarget.flatMap(_.name(previousContext))
        val optNewContextGraphs =
          previousScope.contextGraphs.map(_.updated(newSourceName, newTargetName)) orElse
          newSourceName.map(source => newTargetName.map(target => ContextGraphs(source, target)).getOrElse(ContextGraphs(source, source)))
        optNewContextGraphs match {
          case Some(newContextGraphs) => s.updateContextGraphs(newContextGraphs)
            // If there are no context graphs that's fine -- use the ambient graph
          case None => Right(s)
        }
      }
    }
    (
      when (includeExisting) { s => success(s.importGraphsFromScope(previousScope)) } chain
      items.flatMap(_.graphs).foldSemanticCheck(_.declareGraph) chain
      items.flatMap(_.graphs).foldSemanticCheck(_.implicitGraph(previousScope.contextGraphs)) chain
      updateContext
    ).ifFeatureEnabled(SemanticFeature.MultipleGraphs)
  }

  private def checkNoMultipleSources: SemanticCheck =
    (s: SemanticState) =>
      if (items.flatMap(_.newSource).size > 1)
        SemanticCheckResult.error(s, SemanticError("Setting multiple source graphs is not allowed", position))
      else SemanticCheckResult.success(s)

  private def checkNoMultipleTargets: SemanticCheck =
    (s: SemanticState) =>
      if (items.flatMap(_.newTarget).size > 1)
        SemanticCheckResult.error(s, SemanticError("Setting multiple target graphs is not allowed", position))
      else SemanticCheckResult.success(s)

  private def checkUniqueGraphReference: SemanticCheck = {
    (s: SemanticState) => {
      val aliases = graphs.flatMap(_.as)
      if (aliases.size == aliases.toSet.size)
        SemanticCheckResult.success(s)
      else
        SemanticCheckResult.error(s, SemanticError("Multiple result graphs with the same name are not supported", position))
    }
  }
}

