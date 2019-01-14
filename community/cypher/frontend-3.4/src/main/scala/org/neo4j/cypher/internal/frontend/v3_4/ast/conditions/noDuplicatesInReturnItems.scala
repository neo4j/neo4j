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
package org.neo4j.cypher.internal.frontend.v3_4.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_4.ast.ReturnItems
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.Condition

case object noDuplicatesInReturnItems extends Condition {
  def apply(that: Any): Seq[String] = {
    val returnItems = collectNodesOfType[ReturnItems].apply(that)
    returnItems.collect {
      case ris@ReturnItems(_, items) if items.toSet.size != items.size =>
        s"ReturnItems at ${ris.position} contain duplicate return item: $ris"
    }
  }

  override def name: String = productPrefix
}
