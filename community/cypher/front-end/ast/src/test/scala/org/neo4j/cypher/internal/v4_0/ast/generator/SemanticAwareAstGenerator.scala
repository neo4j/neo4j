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
package org.neo4j.cypher.internal.v4_0.ast.generator

import org.neo4j.cypher.internal.v4_0.ast.generator.AstGenerator.boolean
import org.neo4j.cypher.internal.v4_0.expressions.{FunctionInvocation, Namespace}
import org.neo4j.cypher.internal.v4_0.expressions.functions.{Avg, Collect, Count, Max, Min, PercentileCont, PercentileDisc, StdDev, StdDevP, Sum}
import org.scalacheck.Gen
import org.scalacheck.Gen.{listOfN, oneOf}

/**
 * Prototype of a generator that generates semantically valid expressions/ASTs.
 */
class SemanticAwareAstGenerator(override val simpleStrings: Boolean = true, override val allowedVarNames: Option[Seq[String]] = None)
  extends AstGenerator(simpleStrings, allowedVarNames) {
  // FIXME this generates too many invalid combinations
  def aggregationFunctionInvocation: Gen[FunctionInvocation] = {

    for {
      function <- oneOf(Avg, Collect, Count, Max, Min, PercentileCont, PercentileDisc, StdDev, StdDevP, Sum)
      signature <- oneOf(function.signatures)
      numArgs = signature.argumentTypes.length
      distinct <- boolean
      args <- listOfN(numArgs, _expression)
    } yield {
      val x = FunctionInvocation(Namespace()(pos), function.asFunctionName(pos), distinct, args.toIndexedSeq)(pos)
      println(x)
      x
    }
  }
}
