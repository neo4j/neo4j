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
  def newTarget: Option[SingleGraphItem] = items.flatMap(_.newTarget).headOption

  override def semanticCheck = {
    // TODO: Make sure only one source and target is ever specified
    val covariant = CTGraphRef.covariant
    graphs.flatMap(_.as).foldSemanticCheck(_.expectType(covariant)) chain
      FeatureError("Projecting / returning graphs is not supported by Neo4j", position)
  }
}

