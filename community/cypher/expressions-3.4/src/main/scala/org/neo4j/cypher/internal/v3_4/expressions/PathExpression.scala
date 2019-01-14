/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.Rewritable._
import org.neo4j.cypher.internal.util.v3_4.{Foldable, InputPosition, Rewritable}

sealed trait PathStep extends Product with Foldable with Rewritable {

  self =>

  def dependencies: Set[Expression]

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.children)
      this
    else {
      val constructor = Rewritable.copyConstructor(this)
      val params = constructor.getParameterTypes
      val args = children.toVector
      val ctorArgs = args
      val duped = constructor.invoke(this, ctorArgs: _*)
      duped.asInstanceOf[self.type]
    }
}

final case class NodePathStep(node: Expression, next: PathStep) extends PathStep {
  val dependencies = next.dependencies + node
}

final case class SingleRelationshipPathStep(rel: Expression, direction: SemanticDirection, next: PathStep) extends PathStep {
  val dependencies = next.dependencies + rel
}

final case class MultiRelationshipPathStep(rel: Expression, direction: SemanticDirection, next: PathStep) extends PathStep {
  val dependencies = next.dependencies + rel
}

case object NilPathStep extends PathStep {
  def dependencies = Set.empty[Expression]
}

case class PathExpression(step: PathStep)(val position: InputPosition) extends Expression
