/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.util.InputPosition

case class PatternExpression(pattern: RelationshipsPattern)(
  override val outerScope: Set[LogicalVariable],
  override val variableToCollectName: String,
  override val collectionName: String
) extends ScopeExpression with ExpressionWithOuterScope with RollupApplySolvable {

  override def position: InputPosition = pattern.position

  override def introducedVariables: Set[LogicalVariable] = pattern.element.allVariables -- outerScope

  override def withOuterScope(outerScope: Set[LogicalVariable]): PatternExpression =
    copy()(outerScope, variableToCollectName, collectionName)

  override def dup(children: Seq[AnyRef]): this.type = {
    PatternExpression(
      children.head.asInstanceOf[RelationshipsPattern]
    )(outerScope, variableToCollectName, collectionName).asInstanceOf[this.type]
  }
}
