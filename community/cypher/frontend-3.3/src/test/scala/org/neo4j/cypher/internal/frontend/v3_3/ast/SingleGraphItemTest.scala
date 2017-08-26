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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.{SemanticFeature, SemanticState}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class SingleGraphItemTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Self alias does not produce a semantic error") {

    val Right(state) = SemanticState.withFeatures(SemanticFeature.MultipleGraphs).declareGraphVariable(varFor("foo"))

    val result = graphAs("foo", "foo").semanticCheck(state)
    val errors = result.errors.toSet

    errors.isEmpty should be(true)
  }
}
