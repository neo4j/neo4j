/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticAnalysisTooling, SemanticState}
import org.neo4j.cypher.internal.util.v3_4.InputPosition

sealed trait CatalogDDL extends Statement with SemanticAnalysisTooling {
  override def semanticCheck: SemanticCheck =
    requireMultigraphSupport(s"The `$name` clause", position)

  def name: String

  override def returnColumns = List.empty
}

object CreateGraph {
  // Ignore the periodicCommitHint
  def apply(graphName: QualifiedGraphName, query: Query)(position: InputPosition): CreateGraph =
    CreateGraph(graphName, query.part)(position)
}

final case class CreateGraph(graphName: QualifiedGraphName, query: QueryPart)
  (val position: InputPosition) extends CatalogDDL {

  override def name = "CREATE GRAPH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this) chain
      query.semanticCheck
}

final case class DeleteGraph(graphName: QualifiedGraphName)(val position: InputPosition) extends CatalogDDL {

  override def name = "DELETE GRAPH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}
