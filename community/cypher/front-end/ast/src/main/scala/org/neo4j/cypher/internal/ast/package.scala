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
package org.neo4j.cypher.internal

package object ast {

  // For SHOW commands, the user can supply:
  //   * YIELD xxx WHERE xxx RETURN xxx which optionally introduces two new scopes
  //   * WHERE on it's own which is rewritten to YIELD * WHERE
  //   * Nothing in which case we just SHOW everything
  // This type represents these options as they come out of the parser
  type YieldOrWhere = Option[Either[(ast.Yield, Option[ast.Return]), ast.Where]]
}
