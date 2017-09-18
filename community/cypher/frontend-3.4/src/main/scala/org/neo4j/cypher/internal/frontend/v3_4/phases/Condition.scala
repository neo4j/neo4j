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
package org.neo4j.cypher.internal.frontend.v3_4.phases

import org.neo4j.cypher.internal.frontend.v3_4.SemanticState
import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement

import scala.reflect.ClassTag

trait Condition {
  def check(state: AnyRef): Seq[String]
}

case class BaseContains[T: ClassTag](implicit manifest: Manifest[T]) extends Condition {
  private val acceptableTypes: Set[Class[_]] = Set(
    classOf[Statement],
    classOf[SemanticState]
  )

  assert(acceptableTypes.contains(manifest.runtimeClass))

  override def check(in: AnyRef): Seq[String] = in match {
    case state: BaseState =>
      manifest.runtimeClass match {
        case x if classOf[Statement] == x && state.maybeStatement.isEmpty => Seq("Statement missing")
        case x if classOf[SemanticState] == x && state.maybeSemantics.isEmpty => Seq("Semantic State missing")
        case _ => Seq.empty
      }
    case x => throw new IllegalArgumentException(s"Unknown state: $x")
  }
}
