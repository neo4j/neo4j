/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker

import scala.reflect.ClassTag

case class BaseContains[T]()(implicit val tag: ClassTag[T]) extends ValidatingCondition {

  private val acceptableTypes: Set[Class[_]] = Set(
    classOf[Statement],
    classOf[SemanticState],
    classOf[SemanticTable]
  )

  assert(acceptableTypes.contains(tag.runtimeClass))

  override def apply(in: Any)(cancellationChecker: CancellationChecker): Seq[String] = in match {
    case state: BaseState =>
      tag.runtimeClass match {
        case x if classOf[Statement] == x && state.maybeStatement.isEmpty         => Seq("Statement missing")
        case x if classOf[SemanticState] == x && state.maybeSemantics.isEmpty     => Seq("Semantic state missing")
        case x if classOf[SemanticTable] == x && state.maybeSemanticTable.isEmpty => Seq("Semantic table missing")
        case _                                                                    => Seq.empty
      }
    case x => throw new IllegalStateException(s"Unknown state: $x")
  }

  override def name: String = s"$productPrefix[${tag.runtimeClass.getSimpleName}]"

  override def hashCode(): Int = tag.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case bc: BaseContains[_] => tag.equals(bc.tag)
    case _                   => false
  }
}
