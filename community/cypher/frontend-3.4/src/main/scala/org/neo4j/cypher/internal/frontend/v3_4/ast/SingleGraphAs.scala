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

import org.neo4j.cypher.internal.util.v3_4.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.semantics._
import org.neo4j.cypher.internal.v3_4.expressions.{Pattern, Variable}

sealed trait SingleGraphAs extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {

  def as: Option[Variable]

  def generated: Boolean

  def name(context: Option[ContextGraphs]): Option[String] = as.map(_.name)

  def isUnboundContextGraph: Boolean = false

  def withNewName(newName: Variable): SingleGraphAs
  def asGenerated: SingleGraphAs

  override def semanticCheck: SemanticCheck = SemanticCheckResult.success

  def declareGraph: SemanticCheck =
    semanticCheckFold(as)(v => if (generated) declareGraphMarkedAsGenerated(v) else declareGraph(v))

  def implicitGraph(context: Option[ContextGraphs]): SemanticCheck = SemanticCheckResult.success
}

sealed trait BoundGraphAs extends SingleGraphAs

sealed trait BoundContextGraphAs extends BoundGraphAs {

  override def isUnboundContextGraph: Boolean = as.isEmpty

  protected def contextGraphName: String

  override def declareGraph: SemanticCheck = as match {
    case Some(v) => if (generated) declareGraphMarkedAsGenerated(v) else declareGraph(v)
    case None => SemanticCheckResult.success
  }

  override def implicitGraph(context: Option[ContextGraphs]): SemanticCheck = as match {
    case Some(_) =>
      SemanticCheckResult.success
    case None =>
      val check = (_: SemanticState).implicitContextGraph(context.map(_.source), position, contextGraphName)
      check
  }
}

final case class SourceGraphAs(as: Option[Variable], generated: Boolean = false)(val position: InputPosition) extends BoundContextGraphAs {
  override protected def contextGraphName: String = "source graph"
  override def name(context: Option[ContextGraphs]): Option[String] = super.name(context) orElse context.map(_.source)
  override def withNewName(newName: Variable): SourceGraphAs = copy(as = Some(newName))(position)
  override def asGenerated: SourceGraphAs = copy(generated = true)(position)
}

final case class TargetGraphAs(as: Option[Variable], generated: Boolean = false)(val position: InputPosition) extends BoundContextGraphAs {
  override protected def contextGraphName: String = "target graph"
  override def name(context: Option[ContextGraphs]): Option[String] = super.name(context) orElse context.map(_.target)
  override def withNewName(newName: Variable): TargetGraphAs = copy(as = Some(newName))(position)
  override def asGenerated: TargetGraphAs = copy(generated = true)(position)
}

final case class GraphAs(ref: Variable, as: Option[Variable], generated: Boolean = false)(val position: InputPosition)
  extends BoundGraphAs {

  override def withNewName(newName: Variable): GraphAs = copy(as = Some(newName))(position)
  override def asGenerated: GraphAs = copy(generated = true)(position)

  override def semanticCheck: SemanticCheck = SemanticExpressionCheck.ensureGraphDefined(ref)
  override def declareGraph: SemanticCheck = semanticCheckFold(as)(v => implicitGraph(v))
}

sealed trait NewGraphAs extends SingleGraphAs

final case class GraphOfAs(of: Pattern, as: Option[Variable], generated: Boolean = false)(val position: InputPosition)
  extends NewGraphAs {

  override def withNewName(newName: Variable): GraphOfAs = copy(as = Some(newName))(position)
  override def asGenerated: GraphOfAs = copy(generated = true)(position)

  override def semanticCheck: SemanticCheck = SemanticPatternCheck.check(Pattern.SemanticContext.GraphOf, of)
}

final case class GraphAtAs(at: GraphUrl, as: Option[Variable], generated: Boolean = false)(val position: InputPosition)
  extends NewGraphAs {

  override def withNewName(newName: Variable): GraphAtAs = copy(as = Some(newName))(position)
  override def asGenerated: GraphAtAs = copy(generated = true)(position)

  override def semanticCheck: SemanticCheck = at.semanticCheck
}

