/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.expressions

case class PatternExpression(pattern: RelationshipsPattern)(override val outerScope: Set[Variable]) extends ScopeExpression with ExpressionWithOuterScope {
  override def position = pattern.position

  override def introducedVariables: Set[LogicalVariable] = pattern.element.allVariables -- outerScope

  override def withOuterScope(outerScope: Set[Variable]): PatternExpression = copy()(outerScope)

  override def dup(children: Seq[AnyRef]): this.type = {
    PatternExpression(
      children.head.asInstanceOf[RelationshipsPattern]
    )(outerScope).asInstanceOf[this.type]
  }
}
