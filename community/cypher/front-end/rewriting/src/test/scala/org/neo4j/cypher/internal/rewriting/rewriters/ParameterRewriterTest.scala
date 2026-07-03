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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.util.ExactSize
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.UnknownSize
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ParameterRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Should give default values to explicit parameter after rewrite") {
    val temp = ExplicitParameter("prop", CTString, ExactSize(5))(InputPosition.NONE)
    temp.endoRewrite(parameterRewriter) should equal(
      ExplicitParameter("prop", CTAny, UnknownSize)(InputPosition.NONE)
    )
  }

  test("Should give default values to auto extracted parameter after rewrite") {
    val temp =
      AutoExtractedParameter("prop", CTString, ExactSize(5))(InputPosition.NONE)
    temp.endoRewrite(parameterRewriter) should equal(
      AutoExtractedParameter("prop", CTAny, UnknownSize)(InputPosition.NONE)
    )
  }
}
