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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.expressions.functions.AllReduce
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AggregationAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlParams.StringParam
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class AllReducePredicateSemanticAnalysisTest extends CypherFunSuite with SemanticAnalysisTestSuite {

  private def run25(query: String): AnalysisAssertions = {
    run(
      query,
      disabledVersions = Set(CypherVersion.Cypher5)
    )
  }

  test("allReduce() is available") {
    run25(
      "MATCH (a)-[r]->+(b) RETURN allReduce(acc = [], iter IN r | acc + iter, size(acc) <= 5) AS result"
    ).hasNoErrors
  }

  test("should not allow undefined variables in init") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = zzz, rel IN r | acc + rel.prop, acc <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Variable `zzz` not defined")
  }

  test("should return an error when the iteration expression is not a list") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = 45, rel IN 1 | acc + rel.prop, acc <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages(
      "Type mismatch: expected List<T> but was Integer",
      "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Any"
    )
  }

  test("should not allow accumulator variable in init") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = acc, rel IN r | acc + rel.prop, acc <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Variable `acc` not defined")
  }

  test("should not allow aggregation in init in WHERE") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = count(*), rel IN r | acc + rel.prop, acc <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Invalid use of aggregating function count(...) in this context")
  }

  test("should allow aggregation in init in RETURN") {
    run25(
      """MATCH (a)-[r]->+(b)
        |RETURN r, allReduce(acc = count(*), rel IN r | acc + rel.prop, acc <= 5) AS result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should find implicit grouping expression in allReduce()") {
    run(
      """MATCH (a)-[r]->+(b)
        |RETURN allReduce(acc = count(*), rel IN r | acc + rel.prop, acc <= 5) AS result
        |""".stripMargin,
      pipeline = semanticAnalysisTwice() andThen ScopeSurveyor andThen AggregationAnalysis,
      disabledVersions = Set(CypherVersion.Cypher5)
    ).hasErrorMessages(SemanticError.implicitGroupingExpressionInAggregationColumnErrorMessage(Seq("r")))
  }

  test("accumulator variable does not shadow existing variables in init") {
    run25(
      """WITH 1 AS acc
        |MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = acc, rel IN r | acc + rel.prop, acc <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow subquery expression in init") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = COUNT { (a)-->(acc) WHERE a <> acc }, rel IN r | acc + rel.prop, acc <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow nested allReduce() in init") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = allReduce(acc = [], rel IN r | acc + rel, size(acc) < 5),
        |                rel IN r | toInteger(acc) + rel.prop,
        |                toInteger(acc) <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("return type of allReduce() is Boolean") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE isEmpty(allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5))
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: expected Map, Node, Relationship, String or List<T> but was Boolean")
  }

  test("type of projected allReduce() is Boolean") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WITH allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS arResult
        |RETURN isEmpty(arResult)
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: expected Map, Node, Relationship, String or List<T> but was Boolean")
  }

  test("predicate should be a boolean expression") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = 0, rel IN r | acc + rel.prop, acc)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: expected Boolean but was Integer")
  }

  test("should accept possible boolean expressions as predicate") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = [], rel IN r | acc + rel.prop, acc[2])
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should treat group variable in reduction step as singleton") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = [], rel IN r | acc + rel.prop, size(acc) <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow using group variable in reduction step as a list") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = 0, rel IN r | acc + size(r), acc <= 10)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should not allow using implicitly imported group variables in reduction step as singletons") {
    run25(
      """MATCH (a) ((n)-[r]->(m))+ (b)
        |WHERE allReduce(acc = 0, rel IN r | acc + rel.prop + m.prop, acc <= 10)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages(
      "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was List<Node>"
    )
  }

  test("can support reduction with no group variables") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0, rel IN [0,1] | acc + 1, acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow to reference additional non-group variables in reduction step") {
    run25(
      """WITH 123 AS threshold
        |MATCH (a) ((n)-[r]->(m))+ (b)
        |WHERE allReduce(acc = 0, rel IN r | acc + a.prop + b.prop + rel.prop, acc <= threshold)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should not allow aggregation in reduction step") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = [], rel IN r | acc + rel.prop + count(*), size(acc) <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Invalid use of aggregating function count(...) in this context")
  }

  test("should allow nested allReduce() in reduction step") {
    run25(
      """MATCH (a) ((n)-[r]->(m))+ (b)
        |WHERE allReduce(acc = [],
        |                node IN m | acc + node.prop + allReduce(nacc = 0, rel IN r | nacc + rel.prop, nacc < 10),
        |                size(acc) <= 123)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow subquery expression in reduction step") {
    run25(
      """MATCH (a) ((n)-[r]->(m))+ (b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + COUNT { (node)-[:FRIEND_OF]->() },
        |                acc <= 123)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow using reductionStepVariable through CALL (*) in reduction step") {
    run25(
      """MATCH (a) ((n)-[r]->(m))+ (b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + COUNT {
        |                        CALL (*) { MATCH (node)-[:FRIEND_OF]->(x) RETURN x }
        |                      },
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow using the accumulator through CALL (*) in reduction step") {
    run25(
      """MATCH (a) ((n)-[r]->(m))+ (b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + COUNT {
        |                        CALL (*) {
        |                          MATCH (node)-[:FRIEND_OF]->(x)
        |                          WHERE x.prop < acc
        |                          RETURN x
        |                        }
        |                      },
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should identify improper usage of group variables through CALL (*) in reduction step") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + COUNT {
        |                        CALL (*) { MATCH (node)-[:FRIEND_OF]->(m) RETURN node AS y, m AS x }
        |                      },
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: m defined with conflicting type List<Node> (expected Node)")
  }

  test("should identify improper usage of group variables through CALL with variable in reduction step") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + COUNT {
        |                        CALL (m) { MATCH (node)-[:FRIEND_OF]->(m) RETURN node AS k, m AS x }
        |                      },
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: m defined with conflicting type List<Node> (expected Node)")
  }

  test("should override group variables through CALL without variables in reduction step") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + COUNT {
        |                        CALL () { MATCH (node)-[:FRIEND_OF]->(m) RETURN node AS k, m AS x }
        |                      },
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should identify improper usage of group variables through CALL (*) in the predicate") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + node.prop,
        |                EXISTS {
        |                        CALL (*) { MATCH (a)-[:FRIEND_OF]->(m) WHERE a.prop = acc RETURN a AS k, m AS x}
        |                      })
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: m defined with conflicting type List<Node> (expected Node)")
  }

  test("should identify improper usage of group variables through CALL (m) in the predicate") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + node.prop,
        |                EXISTS {
        |                        CALL (m, acc) { MATCH (a)-[:FRIEND_OF]->(m) WHERE a.prop = acc RETURN a AS k, m AS x }
        |                      })
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: m defined with conflicting type List<Node> (expected Node)")
  }

  test("should not identify the usage of the accumulator if using CALL() in the predicate") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + node.prop,
        |                EXISTS {
        |                        CALL () { MATCH (a)-[:FRIEND_OF]->(m) WHERE a.prop = acc RETURN a AS k, m AS x }
        |                      })
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages(
      "Variable `acc` not defined"
    ) // Note that the variable m is newly defined here, so you shouldn't see the error from the other tests here.
  }

  test("should not allow variables introduced in predicates in the reduction step") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + node.prop + x.prop,
        |                EXISTS {
        |                  MATCH (x)-[:FRIEND_OF]->() WHERE x.prop = acc RETURN x
        |                })
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages(
      "Variable `x` not defined"
    ) // Note that the variable m is newly defined here, so you shouldn't see the error from the other tests here.
  }

  test("should ensure that the accumulator and the reduction step have matching types") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = 0,
        |                node IN ["Hello", "World"] | acc + node,
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Type mismatch: accumulator is Integer but reduction has type String")
  }

  test("should not throw an error when accumulator can be coerced to the reduction type") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = "0",
        |                node IN [1,2] | acc + node,
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should correctly find reduction step type mismatches through CALL (*) in reduction step") {
    run25(
      """MATCH (a)((n)-[r]->(m))+(b)
        |WHERE allReduce(acc = {},
        |                node IN n | acc + COUNT {
        |                        CALL (*) { MATCH (node)-[:FRIEND_OF]->(x) RETURN node AS k, x AS y}
        |                      },
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages(
      "Type mismatch: expected List<T> but was Integer",
      "Type mismatch: accumulator is Map but reduction has type Any"
    )
  }

  test("should correctly identify shadowed variables in CALL in reduction step") {
    run25(
      """MATCH (a) ((n)-[r]->(m))+ (b)
        |WHERE allReduce(acc = 0,
        |                node IN n | acc + COUNT { WITH 1 AS n RETURN n },
        |                acc < 10)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages(
      "The variable `n` is shadowing a variable with the same name from the outer scope and needs to be renamed"
    )
  }

  test("should carry group variables through WITH *") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WITH *
        |RETURN allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should carry group variables through NEXT") {
    run25(
      """MATCH (a)-[r]->+(b)
        |RETURN r
        |NEXT
        |RETURN allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should carry group variables through explicit WITH") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WITH r
        |RETURN allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should carry group variables through WITH aliasing") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WITH r AS alias
        |RETURN allReduce(acc = 0, rel IN alias | acc + rel.prop, acc <= 5) AS result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow running an allReduce on the output of coalesce()") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WITH coalesce(r, 123) AS alias
        |RETURN allReduce(acc = 0, rel IN alias | acc + rel.prop, acc <= 5) AS result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should carry group variables through CALL subquery import") {
    run25(
      """MATCH (a)-[r]->+(b)
        |CALL (r) {
        |  RETURN allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS result
        |}
        |RETURN result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should carry group variables through CALL (*) subquery import") {
    run25(
      """MATCH (a)-[r]->+(b)
        |CALL (*) {
        |  RETURN allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS result
        |}
        |RETURN result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should carry group variables through CALL subquery import into UNION") {
    run25(
      """MATCH (a)-[r]->+(b)
        |CALL (r) {
        |  RETURN 123 AS result
        |  UNION
        |  RETURN allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS result
        |}
        |RETURN result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should carry group variables through CALL (*) subquery import into UNION") {
    run25(
      """MATCH (a)-[r]->+(b)
        |CALL (*) {
        |  RETURN 123 AS result
        |  UNION
        |  RETURN allReduce(acc = 0, rel IN r | acc + rel.prop, acc <= 5) AS result
        |}
        |RETURN result
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow shadowed variable name for accumulator") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(a = 0, rel IN r | a + rel.prop, a <= 5)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow predicate without accumulator") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = 0, rel IN r | acc + rel.prop, a.prop = 5)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should allow reduction step variable in predicate") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = 0, rel IN r | acc + rel.prop, rel.prop = 5)
        |RETURN a, b
        |""".stripMargin
    ).hasNoErrors
  }

  test("should not allow using reduction step variable to initialize accumulator") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = rel.prop, rel IN r | acc + rel.prop, acc = 5)
        |RETURN a, b
        |""".stripMargin
    ).hasErrorMessages("Variable `rel` not defined")
  }

  test("should not allow using reduction step variable outside of reduction step") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = rel.prop, rel IN r | acc + rel.prop, acc = 5)
        |RETURN a, rel
        |""".stripMargin
    ).hasErrorMessages("Variable `rel` not defined")
  }

  test("should fail gracefully when allReduce() is used inside the QPP") {
    run25(
      """MATCH p = (a)-[r WHERE allReduce(acc = 0, rel IN r | acc + rel.p, acc <= 5)]->+(b)
        |RETURN p
        |""".stripMargin
    ).hasErrorMessages(
      "Type mismatch: expected List<T> but was Relationship",
      "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Any"
    )
  }

  test("reduction variable can shadow any variable outside of the allReduce scope") {
    run25(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = 0, r IN r | acc + r.prop, acc <= 5)
        |RETURN a, b, r
        |""".stripMargin
    ).hasNoErrors
  }

  test("should not allow parsing allReduce as a function invocation without a | expression") {
    run(
      """MATCH (a)-[r]->+(b)
        |RETURN allReduce(acc = 0, acc + 1, acc <= 5)
        |""".stripMargin
    ).hasAllGQLErrorsIn {
      case CypherVersion.Cypher5 => Seq((
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001).withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62).atPosition(37, 2, 18).withParam(
              StringParam.variable,
              "acc"
            ).build()
          ).atPosition(37, 2, 18).build(),
          "Variable `acc` not defined",
          InputPosition(line = 2, column = 18, offset = 37)
        )) // No errors in Cypher5 except for the unknown variable. It could be an unresolved function
      case _ => invalidAllReduceSyntax(line = 2, column = 8, offset = 27)
    }
  }

  test("should not allow parsing allReduce as a function invocation with no arguments") {
    run(
      """MATCH (a)-[r]->+(b)
        |RETURN allReduce()
        |""".stripMargin
    ).hasGQLErrorsIn {
      case CypherVersion.Cypher5 => Seq() // No errors in Cypher5. It could be an unresolved function
      case _                     => invalidAllReduceSyntax(line = 2, column = 8, offset = 27)
    }
  }

  test("should return a meaningful error when allReduce is without an initializer") {
    run25(
      """MATCH (a)-[r]->+(b)
        |RETURN allReduce(step IN r | acc + 1, acc <= 5)
        |""".stripMargin
    ).hasAllGQLErrorsIn(_ => invalidAllReduceSyntax(line = 2, column = 8, offset = 27))
  }

  test("should return a meaningful error when allReduce is with multiple reduction steps") {
    run25(
      """MATCH (a)-[r]->+(b)
        |RETURN allReduce(acc = 0, step IN r | acc + 1, stepA in a | acc + 1, acc <= 5)
        |""".stripMargin
    ).hasAllGQLErrorsIn(_ => invalidAllReduceSyntax(offset = 27, column = 8, line = 2))
  }

  private def invalidAllReduceSyntax(offset: Int, column: Int, line: Int) = Seq((
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001).withCause(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I51).atPosition(
        offset,
        line,
        column
      ).withParam(GqlParams.StringParam.procFun, AllReduce.name)
        .withParam(GqlParams.StringParam.sig, AllReduce.signatures.head.getSignatureAsString).build()
    ).atPosition(offset, line, column).build(),
    s"Invalid syntax for the `allReduce` function. The function allReduce must have the signature ${AllReduce.signatures.head.getSignatureAsString}",
    InputPosition(line = line, column = column, offset = offset)
  ))
}
