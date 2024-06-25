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
package org.neo4j.cypher.internal.parser.v6.ast.factory

import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.v6.Cypher6Parser

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.IterableHasAsScala

object Cypher6AstUtil {

  def nonEmptyPropertyKeyName(list: Cypher6Parser.NonEmptyNameListContext): ArraySeq[PropertyKeyName] = {
    ArraySeq.from(list.symbolicNameString().asScala.collect {
      case s: Cypher6Parser.SymbolicNameStringContext => PropertyKeyName(s.ast())(pos(s))
    })
  }
}
