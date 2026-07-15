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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizeInequalitiesTest extends CypherFunSuite with AstConstructionTestSupport {

  val expression1: Expression = varFor("foo1", pos)
  val expression2: Expression = varFor("foo2", pos)

  val comparisons: Seq[Expression] = List(
    Or(Equals(expression1, expression2)(pos), LessThan(expression1, expression2)(pos))(pos),
    Or(Equals(expression2, expression1)(pos), LessThan(expression1, expression2)(pos))(pos),
    Or(LessThan(expression1, expression2)(pos), Equals(expression1, expression2)(pos))(pos),
    Or(LessThan(expression1, expression2)(pos), Equals(expression2, expression1)(pos))(pos),
    Or(Equals(expression1, expression2)(pos), GreaterThan(expression1, expression2)(pos))(pos),
    Or(Equals(expression2, expression1)(pos), GreaterThan(expression1, expression2)(pos))(pos),
    Or(GreaterThan(expression1, expression2)(pos), Equals(expression1, expression2)(pos))(pos),
    Or(GreaterThan(expression1, expression2)(pos), Equals(expression2, expression1)(pos))(pos)
  )

  comparisons.foreach { exp =>
    test(exp.toString) {
      val rewritten = exp.rewrite(normalizeInequalities)
      rewritten shouldBe a[InequalityExpression]
    }
  }
}
