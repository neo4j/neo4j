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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTGraphRef

// TODO: Unit test semantic checking
sealed trait GraphReturnItem extends ASTNode with ASTParticle {
  def graphs: List[SingleGraphItem]

  def newSource: Option[SingleGraphItem] = None
  def newTarget: Option[SingleGraphItem] = None
}

final case class ReturnedGraph(item: SingleGraphItem)(val position: InputPosition) extends GraphReturnItem {
  override def graphs = List(item)
}

final case class NewTargetGraph(target: SingleGraphItem)(val position: InputPosition) extends GraphReturnItem {
  override def graphs = List(target)
  override def newTarget = Some(target)
}

final case class NewContextGraphs(source: SingleGraphItem,
                                  override val newTarget: Option[SingleGraphItem] = None)
                                 (val position: InputPosition) extends GraphReturnItem {
  override def graphs = List(source) ++ newTarget.toList

  override def newSource: Option[SingleGraphItem] = Some(source)
}

final case class GraphReturnItems(star: Boolean, items: List[GraphReturnItem])
                                 (val position: InputPosition)
  extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  val graphs: List[SingleGraphItem] = items.flatMap(_.graphs)

  def newSource: Option[SingleGraphItem] = items.flatMap(_.newSource).headOption
  def newTarget: Option[SingleGraphItem] = items.flatMap(_.newTarget).headOption orElse newSource

  override def semanticCheck: SemanticCheck =
    graphs.flatMap(_.as).foldSemanticCheck(_.expectType(CTGraphRef.covariant)) chain
      reportNoMultigraphSupport.unlessFeatureEnabled('multigraph) chain(
        checkNoMultipleSources chain
        checkNoMultipleTargets
    ).ifFeatureEnabled('multigraph)

  private def checkNoMultipleSources: SemanticCheck =
    (s: SemanticState) =>
      if(items.flatMap(_.newSource).size > 1)
        SemanticCheckResult.error(s, SemanticError("Setting multiple source graphs is not allowed", position))
      else SemanticCheckResult.success(s)

  private def checkNoMultipleTargets: SemanticCheck =
    (s: SemanticState) =>
      if(items.flatMap(_.newTarget).size > 1)
        SemanticCheckResult.error(s, SemanticError("Setting multiple target graphs is not allowed", position))
      else SemanticCheckResult.success(s)

  private def reportNoMultigraphSupport: SemanticCheck =
    FeatureError("Projecting / returning graphs is not supported by Neo4j", position)


}

