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
package org.neo4j.cypher.internal.v4_0.rewriting

import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.normalizeSargablePredicates
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.expressions.functions.Exists

class NormalizeSargablePredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  /*
  Test asserts that unsafe rewrite is not performed.
  Because IS NULL & IS NOT NULL are binary operators, whereas Exists is ternary, this rewrite is only safe when the node is not nullable.
  Nullability information is not yet available at time of AST rewriting:
     >          MATCH (n) WHERE n.foo IS NOT NULL RETURN n
     >          MATCH (n) WHERE exists(n.foo)     RETURN n
     > OPTIONAL MATCH (n) WHERE n.foo IS NOT NULL RETURN n
     > OPTIONAL MATCH (n) WHERE exists(n.foo)     RETURN n     <---- unsafe
   In the OPTIONAL MATCH case when n is null:
     > n IS NOT NULL  ==>  false
     > exists(n.foo)  ==>  null
       > null.foo     == null
       > exists(null) == null
  */
  test("a.prop IS NOT NULL should not be rewritten to: exists(a.prop)") {
    val input = isNotNull(prop("a", "prop"))

    normalizeSargablePredicates(input) should equal(input)
  }

  test("exists(a.prop) is not rewritten") {
    val input = Exists.asInvocation(prop("a", "prop"))(pos)

    normalizeSargablePredicates(input) should equal(input)
  }

  test("NOT x < y rewritten to: x >= y") {
    val input = not(lessThan(varFor("x"), varFor("y")))
    val output = greaterThanOrEqual(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x <= y rewritten to: x > y") {
    val input = not(lessThanOrEqual(varFor("x"), varFor("y")))
    val output = greaterThan(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x > y rewritten to: x <= y") {
    val input = not(greaterThan(varFor("x"), varFor("y")))
    val output = lessThanOrEqual(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x >= y rewritten to: x < y") {
    val input = not(greaterThanOrEqual(varFor("x"), varFor("y")))
    val output = lessThan(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }
}
