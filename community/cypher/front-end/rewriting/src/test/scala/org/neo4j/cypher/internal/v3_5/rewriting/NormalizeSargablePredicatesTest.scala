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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.normalizeSargablePredicates
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.expressions.functions.Exists

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
  */  test("a.prop IS NOT NULL should not be rewritten to: exists(a.prop)") {
    val input: Expression = IsNotNull(Property(varFor("a"), PropertyKeyName("prop")_)_)_

    normalizeSargablePredicates(input) should equal(input)
  }

  test("exists(a.prop) is not rewritten") {
    val input: Expression = Exists.asInvocation(Property(varFor("a"), PropertyKeyName("prop")_)_)(pos)
    val output: Expression = Exists.asInvocation(Property(varFor("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x < y rewritten to: x >= y") {
    val input: Expression = Not(LessThan(varFor("x"), varFor("y"))_)_
    val output: Expression = GreaterThanOrEqual(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x <= y rewritten to: x > y") {
    val input: Expression = Not(LessThanOrEqual(varFor("x"), varFor("y"))_)_
    val output: Expression = GreaterThan(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x > y rewritten to: x <= y") {
    val input: Expression = Not(GreaterThan(varFor("x"), varFor("y"))_)_
    val output: Expression = LessThanOrEqual(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x >= y rewritten to: x < y") {
    val input: Expression = Not(GreaterThanOrEqual(varFor("x"), varFor("y"))_)_
    val output: Expression = LessThan(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }
}
