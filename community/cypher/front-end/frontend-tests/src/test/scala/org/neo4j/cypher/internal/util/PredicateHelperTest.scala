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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.PredicateHelper.isPredicate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PredicateHelperTest extends CypherFunSuite with AstConstructionTestSupport {

  test("isPredicate tests") {
    isPredicate(HasLabels(v"n", Seq(LabelName("L")(pos)))(pos)) shouldBe true
    isPredicate(GetDegree(v"n", None, SemanticDirection.OUTGOING)(pos)) shouldBe false
    isPredicate(function("exists", v"x")) shouldBe true
    isPredicate(function("toBoolean", v"x")) shouldBe true
    isPredicate(function("isEmpty", v"x")) shouldBe true
    isPredicate(function("collect", v"x")) shouldBe false
    isPredicate(GreaterThan(v"a", v"b")(pos)) shouldBe true
  }
}
