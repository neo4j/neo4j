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
package org.neo4j.cypher.internal.frontend.scoping.surveyor

import org.neo4j.cypher.internal.frontend.scoping.VariableCheckingTestSuite

class PatternScopeSurveyorTest extends VariableCheckingTestSuite {

  /*
   * For match
   */
  test("MATCH (n:A {p: 1})") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH (n:A {p: 1})"), // query level
        Outgoing(variables = Set("n")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A {p: 1})"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A {p: 1})"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("n")),
            ExpectedWorkingScope.constExp("{p: 1}", Set("n"))
          )
        )
      )
    )
  }

  test("MATCH (n:A WHERE n.p = 1)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH (n:A WHERE n.p = 1)"), // query level
        Outgoing(variables = Set("n")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A WHERE n.p = 1)"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A WHERE n.p = 1)"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("n")),
            ExpectedWorkingScope(
              Ast("n.p = 1"),
              Incoming(constants = Set("n")),
              Referenced(Set("n")),
              ExpectedWorkingScope.varExp("n", Set("n"))
            )
          )
        )
      )
    )
  }

  test("MATCH (n:A) WHERE n.p = 1") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH (n:A) WHERE n.p = 1"), // query level
        Outgoing(variables = Set("n")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A) WHERE n.p = 1"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A)"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("n"))
          ),
          ExpectedWorkingScope(
            Ast("n.p = 1"),
            Incoming(constants = Set("n")),
            Referenced(Set("n")),
            ExpectedWorkingScope.varExp("n", Set("n"))
          )
        )
      )
    )
  }

  test("MATCH (n:A|$all([x IN [1, 2, 3] | n.p + toString(x)]))") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH (n:A|$all([x IN [1, 2, 3] | n.p + toString(x)]))"), // query level
        Outgoing(variables = Set("n")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A|$all([x IN [1, 2, 3] | n.p + toString(x)]))"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A|$all([x IN [1, 2, 3] | n.p + toString(x)]))"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope(
              Ast("A|$all([x IN [1, 2, 3] | n.p + toString(x)])"),
              Incoming(constants = Set("n")),
              Referenced(Set("n")),
              ExpectedWorkingScope(
                Ast("[x IN [1, 2, 3] | n.p + toString(x)]"),
                Incoming(constants = Set("n")),
                Referenced(Set("n")),
                ExpectedWorkingScope.constExp("[1, 2, 3]", Set("n")),
                ExpectedWorkingScope(
                  Ast("[x | n.p + toString(x)]"),
                  Incoming(constants = Set("n", "x")),
                  Declared(constants = Seq("x")),
                  Referenced(Set("n")),
                  ExpectedWorkingScope(
                    Ast("n.p + toString(x)"),
                    Incoming(constants = Set("n", "x")),
                    Referenced(Set("n", "x")),
                    ExpectedWorkingScope.varExp("n", Set("n", "x")),
                    ExpectedWorkingScope(
                      Ast("toString(x)"),
                      Incoming(constants = Set("n", "x")),
                      Referenced(Set("x")),
                      ExpectedWorkingScope.varExp("x", Set("n", "x"))
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  }

  test(
    """MATCH (n:$all(COLLECT { MATCH (m) UNWIND labels(m) AS l RETURN l }))
      |RETURN n""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (n:$all(COLLECT { MATCH (m) UNWIND labels(m) AS l RETURN l }))
              |RETURN n""".stripMargin),
        Outgoing(variables = Set("n")),
        ExpectedResult.TableResult("n"),
        ExpectedWorkingScope(
          Ast("MATCH (n:$all(COLLECT { MATCH (m) UNWIND labels(m) AS l RETURN l }))"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:$all(COLLECT { MATCH (m) UNWIND labels(m) AS l RETURN l }))"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope(
              Ast("$all(COLLECT { MATCH (m) UNWIND labels(m) AS l RETURN l })"),
              Incoming(constants = Set("n")),
              ExpectedWorkingScope(
                Ast("COLLECT { MATCH (m) UNWIND labels(m) AS l RETURN l }"),
                Incoming(constants = Set("n")),
                ExpectedWorkingScope(
                  Ast("MATCH (m) UNWIND labels(m) AS l RETURN l"),
                  Incoming(constants = Set("n")),
                  Outgoing(variables = Set("l")),
                  ExpectedResult.TableResult("l"),
                  ExpectedWorkingScope(
                    Ast("MATCH (m)"),
                    Incoming(constants = Set("n")),
                    Declared(variables = Seq("m")),
                    Outgoing(constants = Set("n"), variables = Set("m")),
                    ExpectedWorkingScope(
                      Ast("(m)"),
                      PatternIncoming(topology = Set("n"), predicate = Set("n", "m")),
                      Declared(variables = Seq("m")),
                      Outgoing(variables = Set("m")),
                      ExpectedResult.TableResult("m")
                    )
                  ),
                  ExpectedWorkingScope(
                    Ast("UNWIND labels(m) AS l"),
                    Incoming(constants = Set("n"), variables = Set("m")),
                    Referenced(Set("m")),
                    Declared(variables = Seq("l")),
                    Outgoing(constants = Set("n"), variables = Set("m", "l")),
                    ExpectedWorkingScope(
                      Ast("labels(m)"),
                      Incoming(constants = Set("n", "m")),
                      Referenced(Set("m")),
                      ExpectedWorkingScope.varExp("m", Set("n", "m"))
                    )
                  ),
                  ExpectedWorkingScope(
                    Ast("RETURN l"),
                    Incoming(constants = Set("n"), variables = Set("m", "l")),
                    Referenced(Set("l")),
                    Outgoing(variables = Set("l")),
                    ExpectedResult.TableResult("l"),
                    ExpectedWorkingScope.varExp("l", Set("n", "m", "l"))
                  )
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN n"),
          Incoming(variables = Set("n")),
          Referenced(Set("n")),
          Outgoing(variables = Set("n")),
          ExpectedResult.TableResult("n"),
          ExpectedWorkingScope.varExp("n", Set("n"))
        )
      )
    )
  }

  test(
    """MATCH (n:$all(COLLECT { MATCH (n) UNWIND labels(n) AS l RETURN l }))
      |RETURN n""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (n:$all(COLLECT { MATCH (n) UNWIND labels(n) AS l RETURN l }))
              |RETURN n""".stripMargin),
        Outgoing(variables = Set("n")),
        ExpectedResult.TableResult("n"),
        ExpectedWorkingScope(
          Ast("MATCH (n:$all(COLLECT { MATCH (n) UNWIND labels(n) AS l RETURN l }))"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:$all(COLLECT { MATCH (n) UNWIND labels(n) AS l RETURN l }))"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope(
              Ast("$all(COLLECT { MATCH (n) UNWIND labels(n) AS l RETURN l })"),
              Incoming(constants = Set("n")),
              Referenced(Set("n")),
              ExpectedWorkingScope(
                Ast("COLLECT { MATCH (n) UNWIND labels(n) AS l RETURN l }"),
                Incoming(constants = Set("n")),
                Referenced(Set("n")),
                ExpectedWorkingScope(
                  Ast("MATCH (n) UNWIND labels(n) AS l RETURN l"),
                  Incoming(constants = Set("n")),
                  Referenced(Set("n")),
                  Outgoing(variables = Set("l")),
                  ExpectedResult.TableResult("l"),
                  ExpectedWorkingScope(
                    Ast("MATCH (n)"),
                    Incoming(constants = Set("n")),
                    Referenced(Set("n")),
                    Outgoing(constants = Set("n")),
                    ExpectedWorkingScope(
                      Ast("(n)"),
                      PatternIncoming(topology = Set("n"), predicate = Set("n")),
                      Referenced(Set("n")),
                      Outgoing(variables = Set("n")),
                      ExpectedResult.TableResult("n")
                    )
                  ),
                  ExpectedWorkingScope(
                    Ast("UNWIND labels(n) AS l"),
                    Incoming(constants = Set("n")),
                    Referenced(Set("n")),
                    Declared(variables = Seq("l")),
                    Outgoing(constants = Set("n"), variables = Set("l")),
                    ExpectedWorkingScope(
                      Ast("labels(n)"),
                      Incoming(constants = Set("n")),
                      Referenced(Set("n")),
                      ExpectedWorkingScope.varExp("n", Set("n"))
                    )
                  ),
                  ExpectedWorkingScope(
                    Ast("RETURN l"),
                    Incoming(constants = Set("n"), variables = Set("l")),
                    Referenced(Set("l")),
                    Outgoing(variables = Set("l")),
                    ExpectedResult.TableResult("l"),
                    ExpectedWorkingScope.varExp("l", Set("n", "l"))
                  )
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN n"),
          Incoming(variables = Set("n")),
          Referenced(Set("n")),
          Outgoing(variables = Set("n")),
          ExpectedResult.TableResult("n"),
          ExpectedWorkingScope.varExp("n", Set("n"))
        )
      )
    )
  }

  test(
    """MATCH (n:A {p: 1})
      |RETURN n""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (n:A {p: 1})
              |RETURN n""".stripMargin),
        Outgoing(variables = Set("n")),
        ExpectedResult.TableResult("n"),
        ExpectedWorkingScope(
          Ast("MATCH (n:A {p: 1})"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A {p: 1})"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("n")),
            ExpectedWorkingScope.constExp("{p: 1}", Set("n"))
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN n"),
          Incoming(variables = Set("n")),
          Referenced(Set("n")),
          Outgoing(variables = Set("n")),
          ExpectedResult.TableResult("n"),
          ExpectedWorkingScope.varExp("n", Set("n"))
        )
      )
    )
  }

  test(
    """UNWIND [1, 2, 3] AS x
      |MATCH (n:A {p: x})
      |RETURN n""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast(
          """UNWIND [1, 2, 3] AS x
            |MATCH (n:A {p: x})
            |RETURN n""".stripMargin
        ),
        Outgoing(variables = Set("n")),
        ExpectedResult.TableResult("n"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("MATCH (n:A {p: x})"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("x", "n")),
          ExpectedWorkingScope(
            Ast("(n:A {p: x})"),
            PatternIncoming(topology = Set("x"), predicate = Set("x", "n")),
            Referenced(Set("x")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("x", "n")),
            ExpectedWorkingScope(
              Ast("{p: x}"),
              Incoming(constants = Set("x", "n")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x", "n"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN n"),
          Incoming(variables = Set("x", "n")),
          Referenced(Set("n")),
          Outgoing(variables = Set("n")),
          ExpectedResult.TableResult("n"),
          ExpectedWorkingScope.varExp("n", Set("x", "n"))
        )
      )
    )
  }

  test("MATCH (n:A {p: 1})-[:R]->(n)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH (n:A {p: 1})-[:R]->(n)"), // query level
        Outgoing(variables = Set("n")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A {p: 1})-[:R]->(n)"),
          Declared(variables = Seq("n", "  SURVEYOR0")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A {p: 1})-[:R]->(n)"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n", "  SURVEYOR0")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1})"),
              PatternIncoming(predicate = Set("n")),
              Declared(variables = Seq("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n"),
              ExpectedWorkingScope.constExp("A", Set("n")),
              ExpectedWorkingScope.constExp("{p: 1}", Set("n"))
            ),
            ExpectedWorkingScope(
              Ast("-[:R]->"),
              PatternIncoming(topology = Set("n"), predicate = Set("n")),
              Declared(variables = Seq("  SURVEYOR0")),
              ExpectedResult.TableResult(),
              ExpectedWorkingScope.constExp("R", Set("n"))
            ),
            ExpectedWorkingScope(
              Ast("(n)"),
              PatternIncoming(topology = Set("n"), predicate = Set("n")),
              Referenced(Set("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n")
            )
          )
        )
      )
    )
  }

  test("MATCH (n:A WHERE n = m)-[:R]->(m)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH (n:A WHERE n = m)-[:R]->(m)"), // query level
        Outgoing(variables = Set("n", "m")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A WHERE n = m)-[:R]->(m)"),
          Declared(variables = Seq("n", "  SURVEYOR0", "m")),
          Outgoing(variables = Set("n", "m")),
          ExpectedWorkingScope(
            Ast("(n:A WHERE n = m)-[:R]->(m)"),
            PatternIncoming(predicate = Set("n", "m")),
            Declared(variables = Seq("n", "  SURVEYOR0", "m")),
            Outgoing(variables = Set("n", "m")),
            ExpectedResult.TableResult("n", "m"),
            ExpectedWorkingScope(
              Ast("(n:A WHERE n = m)"),
              PatternIncoming(predicate = Set("n", "m")),
              Referenced(Set("m")),
              Declared(variables = Seq("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n"),
              ExpectedWorkingScope.constExp("A", Set("n", "m")),
              ExpectedWorkingScope(
                Ast("n = m"),
                Incoming(constants = Set("n", "m")),
                Referenced(Set("n", "m")),
                ExpectedWorkingScope.varExp("n", Set("n", "m")),
                ExpectedWorkingScope.varExp("m", Set("n", "m"))
              )
            ),
            ExpectedWorkingScope(
              Ast("-[:R]->"),
              PatternIncoming(topology = Set("n"), predicate = Set("n", "m")),
              Declared(variables = Seq("  SURVEYOR0")),
              ExpectedResult.TableResult(),
              ExpectedWorkingScope.constExp("R", Set("n", "m"))
            ),
            ExpectedWorkingScope(
              Ast("(m)"),
              PatternIncoming(topology = Set("n"), predicate = Set("n", "m")),
              Declared(variables = Seq("m")),
              Outgoing(variables = Set("m")),
              ExpectedResult.TableResult("m")
            )
          )
        )
      )
    )
  }

  test("MATCH (n:A {p: 1})-[r:R WHERE r.p < n.p]->({p: n.p - r.p})") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH (n:A {p: 1})-[r:R WHERE r.p < n.p]->({p: n.p - r.p})"), // query level
        Outgoing(variables = Set("n", "r")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A {p: 1})-[r:R WHERE r.p < n.p]->({p: n.p - r.p})"),
          Declared(variables = Seq("n", "r", "  SURVEYOR0")),
          Outgoing(variables = Set("n", "r")),
          ExpectedWorkingScope(
            Ast("(n:A {p: 1})-[r:R WHERE r.p < n.p]->({p: n.p - r.p})"),
            PatternIncoming(predicate = Set("n", "r")),
            Declared(variables = Seq("n", "r", "  SURVEYOR0")),
            Outgoing(variables = Set("n", "r")),
            ExpectedResult.TableResult("n", "r"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1})"),
              PatternIncoming(predicate = Set("n", "r")),
              Declared(variables = Seq("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n"),
              ExpectedWorkingScope.constExp("A", Set("n", "r")),
              ExpectedWorkingScope.constExp("{p: 1}", Set("n", "r"))
            ),
            ExpectedWorkingScope(
              Ast("-[r:R WHERE r.p < n.p]->"),
              PatternIncoming(topology = Set("n"), predicate = Set("n", "r")),
              Referenced(Set("n")),
              Declared(variables = Seq("r")),
              Outgoing(variables = Set("r")),
              ExpectedResult.TableResult("r"),
              ExpectedWorkingScope.constExp("R", Set("n", "r")),
              ExpectedWorkingScope(
                Ast("r.p < n.p"),
                Incoming(constants = Set("n", "r")),
                Referenced(Set("r", "n")),
                ExpectedWorkingScope.varExp("r", Set("n", "r")),
                ExpectedWorkingScope.varExp("n", Set("n", "r"))
              )
            ),
            ExpectedWorkingScope(
              Ast("({p: n.p - r.p})"),
              PatternIncoming(topology = Set("n", "r"), predicate = Set("n", "r")),
              Declared(variables = Seq("  SURVEYOR0")),
              Referenced(Set("r", "n")),
              ExpectedResult.TableResult(),
              ExpectedWorkingScope(
                Ast("{p: n.p - r.p}"),
                Incoming(constants = Set("n", "r")),
                Referenced(Set("n", "r")),
                ExpectedWorkingScope.varExp("n", Set("n", "r")),
                ExpectedWorkingScope.varExp("r", Set("n", "r"))
              )
            )
          )
        )
      )
    )
  }

  test(
    """MATCH (n:A)
      |MATCH (n:B)""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (n:A)
              |MATCH (n:B)""".stripMargin),
        Outgoing(variables = Set("n")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A)"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A)"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("n"))
          )
        ),
        ExpectedWorkingScope(
          Ast("MATCH (n:B)"),
          Incoming(variables = Set("n")),
          Referenced(Set("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:B)"),
            PatternIncoming(topology = Set("n"), predicate = Set("n")),
            Referenced(Set("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("B", Set("n"))
          )
        )
      )
    )
  }

  test(
    """MATCH (n:A)
      |MATCH (n)-[r:R]->()""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (n:A)
              |MATCH (n)-[r:R]->()""".stripMargin),
        Outgoing(variables = Set("n", "r")),
        ExpectedWorkingScope(
          Ast("MATCH (n:A)"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n:A)"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("n"))
          )
        ),
        ExpectedWorkingScope(
          Ast("MATCH (n)-[r:R]->()"),
          Incoming(variables = Set("n")),
          Referenced(Set("n")),
          Declared(variables = Seq("r", "  SURVEYOR0")),
          Outgoing(variables = Set("n", "r")),
          ExpectedWorkingScope(
            Ast("(n)-[r:R]->()"),
            PatternIncoming(topology = Set("n"), predicate = Set("n", "r")),
            Referenced(Set("n")),
            Declared(variables = Seq("r", "  SURVEYOR0")),
            Outgoing(variables = Set("n", "r")),
            ExpectedResult.TableResult("n", "r"),
            ExpectedWorkingScope(
              Ast("(n)"),
              PatternIncoming(topology = Set("n"), predicate = Set("n", "r")),
              Referenced(Set("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n")
            ),
            ExpectedWorkingScope(
              Ast("-[r:R]->"),
              PatternIncoming(topology = Set("n"), predicate = Set("n", "r")),
              Declared(variables = Seq("r")),
              Outgoing(variables = Set("r")),
              ExpectedResult.TableResult("r"),
              ExpectedWorkingScope.constExp("R", Set("n", "r"))
            ),
            ExpectedWorkingScope(
              Ast("()"),
              PatternIncoming(topology = Set("n", "r"), predicate = Set("n", "r")),
              Declared(variables = Seq("  SURVEYOR0")),
              ExpectedResult.TableResult()
            )
          )
        )
      )
    )
  }

  test("MATCH p = (n:A {p: 1})-[r:R]->(m)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH p = (n:A {p: 1})-[r:R]->(m)"), // query level
        Outgoing(variables = Set("p", "n", "r", "m")),
        ExpectedWorkingScope(
          Ast("MATCH p = (n:A {p: 1})-[r:R]->(m)"),
          Declared(variables = Seq("p", "n", "r", "m")),
          Outgoing(variables = Set("p", "n", "r", "m")),
          ExpectedWorkingScope(
            Ast("p = (n:A {p: 1})-[r:R]->(m)"),
            PatternIncoming(predicate = Set("n", "r", "m"), path = Set("p")),
            Declared(variables = Seq("p", "n", "r", "m")),
            Outgoing(variables = Set("p", "n", "r", "m")),
            ExpectedResult.TableResult("p", "n", "r", "m"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1})-[r:R]->(m)"),
              PatternIncoming(predicate = Set("n", "r", "m"), path = Set("p")),
              Declared(variables = Seq("n", "r", "m")),
              Outgoing(variables = Set("n", "r", "m")),
              ExpectedResult.TableResult("n", "r", "m"),
              ExpectedWorkingScope(
                Ast("(n:A {p: 1})"),
                PatternIncoming(predicate = Set("n", "r", "m"), path = Set("p")),
                Declared(variables = Seq("n")),
                Outgoing(variables = Set("n")),
                ExpectedResult.TableResult("n"),
                ExpectedWorkingScope.constExp("A", Set("p", "n", "r", "m")),
                ExpectedWorkingScope.constExp("{p: 1}", Set("p", "n", "r", "m"))
              ),
              ExpectedWorkingScope(
                Ast("-[r:R]->"),
                PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "m"), path = Set("p")),
                Declared(variables = Seq("r")),
                Outgoing(variables = Set("r")),
                ExpectedResult.TableResult("r"),
                ExpectedWorkingScope.constExp("R", Set("p", "n", "r", "m"))
              ),
              ExpectedWorkingScope(
                Ast("(m)"),
                PatternIncoming(topology = Set("n", "r"), predicate = Set("n", "r", "m"), path = Set("p")),
                Declared(variables = Seq("m")),
                Outgoing(variables = Set("m")),
                ExpectedResult.TableResult("m")
              )
            )
          )
        )
      )
    )
  }

  test("MATCH p = (n:A {p: 1})-[r:R]->{1, 3} (m)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH p = (n:A {p: 1}) (()-[r:R]->()){1, 3} (m)"), // query level
        Outgoing(variables = Set("p", "n", "r", "m")),
        ExpectedWorkingScope(
          Ast("MATCH p = (n:A {p: 1}) (()-[r:R]->()){1, 3} (m)"),
          Declared(variables = Seq("p", "n", "  SURVEYOR0", "  SURVEYOR1", "r", "m")),
          Outgoing(variables = Set("p", "n", "r", "m")),
          ExpectedWorkingScope(
            Ast("p = (n:A {p: 1}) (()-[r:R]->()){1, 3} (m)"),
            PatternIncoming(predicate = Set("n", "r", "m"), path = Set("p")),
            Declared(variables = Seq("p", "n", "  SURVEYOR0", "  SURVEYOR1", "r", "m")),
            Outgoing(variables = Set("p", "n", "r", "m")),
            ExpectedResult.TableResult("p", "n", "r", "m"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1}) (()-[r:R]->()){1, 3} (m)"),
              PatternIncoming(predicate = Set("n", "r", "m"), path = Set("p")),
              Declared(variables = Seq("n", "  SURVEYOR0", "  SURVEYOR1", "r", "m")),
              Outgoing(variables = Set("n", "r", "m")),
              ExpectedResult.TableResult("n", "r", "m"),
              ExpectedWorkingScope(
                Ast("(n:A {p: 1})"),
                PatternIncoming(predicate = Set("n", "r", "m"), path = Set("p")),
                Declared(variables = Seq("n")),
                Outgoing(variables = Set("n")),
                ExpectedResult.TableResult("n"),
                ExpectedWorkingScope.constExp("A", Set("p", "n", "r", "m")),
                ExpectedWorkingScope.constExp("{p: 1}", Set("p", "n", "r", "m"))
              ),
              ExpectedWorkingScope(
                Ast("(()-[r:R]->()){1, 3}"),
                PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "m"), path = Set("p")),
                Declared(variables = Seq("  SURVEYOR0", "  SURVEYOR1", "r")),
                Referenced(variables = Set("r")),
                Outgoing(variables = Set("r")),
                ExpectedResult.TableResult("r"),
                ExpectedWorkingScope(
                  Ast("()-[r:R]->()"),
                  PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "m")),
                  Declared(variables = Seq("  SURVEYOR0", "r", "  SURVEYOR1")),
                  Outgoing(variables = Set("r")),
                  ExpectedResult.TableResult("r"),
                  ExpectedWorkingScope(
                    Ast("()"),
                    PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "m")),
                    Declared(variables = Seq("  SURVEYOR0")),
                    ExpectedResult.TableResult()
                  ),
                  ExpectedWorkingScope(
                    Ast("-[r:R]->"),
                    PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "m")),
                    Declared(variables = Seq("r")),
                    Outgoing(variables = Set("r")),
                    ExpectedResult.TableResult("r"),
                    ExpectedWorkingScope.constExp("R", Set("n", "r", "m"))
                  ),
                  ExpectedWorkingScope(
                    Ast("()"),
                    PatternIncoming(topology = Set("n", "r"), predicate = Set("n", "r", "m")),
                    Declared(variables = Seq("  SURVEYOR1")),
                    ExpectedResult.TableResult()
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("(m)"),
                PatternIncoming(topology = Set("n", "r"), predicate = Set("n", "r", "m"), path = Set("p")),
                Declared(variables = Seq("m")),
                Outgoing(variables = Set("m")),
                ExpectedResult.TableResult("m")
              )
            )
          )
        )
      )
    )
  }

  test("MATCH p = (n:A {p: 1}) (()-[r:R]->(:X)-[s:S]->() WHERE r.p = s.p){1, 3} (m)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("MATCH p = (n:A {p: 1}) (()-[r:R]->(:X)-[s:S]->() WHERE r.p = s.p){1, 3} (m)"), // query level
        Outgoing(variables = Set("p", "n", "r", "s", "m")),
        ExpectedWorkingScope(
          Ast("MATCH p = (n:A {p: 1}) (()-[r:R]->(:X)-[s:S]->() WHERE r.p = s.p){1, 3} (m)"),
          Declared(variables = Seq("p", "n", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "s", "m")),
          Outgoing(variables = Set("p", "n", "r", "s", "m")),
          ExpectedWorkingScope(
            Ast("p = (n:A {p: 1}) (()-[r:R]->(:X)-[s:S]->() WHERE r.p = s.p){1, 3} (m)"),
            PatternIncoming(predicate = Set("n", "r", "s", "m"), path = Set("p")),
            Declared(variables = Seq("p", "n", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "s", "m")),
            Outgoing(variables = Set("p", "n", "r", "s", "m")),
            ExpectedResult.TableResult("p", "n", "r", "s", "m"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1}) (()-[r:R]->(:X)-[s:S]->() WHERE r.p = s.p){1, 3} (m)"),
              PatternIncoming(predicate = Set("n", "r", "s", "m"), path = Set("p")),
              Declared(variables = Seq("n", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "s", "m")),
              Outgoing(variables = Set("n", "r", "s", "m")),
              ExpectedResult.TableResult("n", "r", "s", "m"),
              ExpectedWorkingScope(
                Ast("(n:A {p: 1})"),
                PatternIncoming(predicate = Set("n", "r", "s", "m"), path = Set("p")),
                Declared(variables = Seq("n")),
                Outgoing(variables = Set("n")),
                ExpectedResult.TableResult("n"),
                ExpectedWorkingScope.constExp("A", Set("p", "n", "r", "s", "m")),
                ExpectedWorkingScope.constExp("{p: 1}", Set("p", "n", "r", "s", "m"))
              ),
              ExpectedWorkingScope(
                Ast("(()-[r:R]->(:X)-[s:S]->() WHERE r.p = s.p){1, 3}"),
                PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "s", "m"), path = Set("p")),
                Declared(variables = Seq("  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "s")),
                Referenced(variables = Set("r", "s")),
                Outgoing(variables = Set("r", "s")),
                ExpectedResult.TableResult("r", "s"),
                ExpectedWorkingScope(
                  Ast("()-[r:R]->(:X)-[s:S]->()"),
                  PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "s", "m")),
                  Declared(variables = Seq("  SURVEYOR0", "r", "  SURVEYOR1", "s", "  SURVEYOR2")),
                  Outgoing(variables = Set("r", "s")),
                  ExpectedResult.TableResult("r", "s"),
                  ExpectedWorkingScope(
                    Ast("()"),
                    PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "s", "m")),
                    Declared(variables = Seq("  SURVEYOR0")),
                    ExpectedResult.TableResult()
                  ),
                  ExpectedWorkingScope(
                    Ast("-[r:R]->"),
                    PatternIncoming(topology = Set("n"), predicate = Set("n", "r", "s", "m")),
                    Declared(variables = Seq("r")),
                    Outgoing(variables = Set("r")),
                    ExpectedResult.TableResult("r"),
                    ExpectedWorkingScope.constExp("R", Set("n", "r", "s", "m"))
                  ),
                  ExpectedWorkingScope(
                    Ast("(:X)"),
                    PatternIncoming(topology = Set("n", "r"), predicate = Set("n", "r", "s", "m")),
                    Declared(variables = Seq("  SURVEYOR1")),
                    ExpectedResult.TableResult(),
                    ExpectedWorkingScope.constExp("X", Set("n", "r", "s", "m"))
                  ),
                  ExpectedWorkingScope(
                    Ast("-[s:S]->"),
                    PatternIncoming(topology = Set("n", "r"), predicate = Set("n", "r", "s", "m")),
                    Declared(variables = Seq("s")),
                    Outgoing(variables = Set("s")),
                    ExpectedResult.TableResult("s"),
                    ExpectedWorkingScope.constExp("S", Set("n", "r", "s", "m"))
                  ),
                  ExpectedWorkingScope(
                    Ast("()"),
                    PatternIncoming(topology = Set("n", "r", "s"), predicate = Set("n", "r", "s", "m")),
                    Declared(variables = Seq("  SURVEYOR2")),
                    ExpectedResult.TableResult()
                  )
                ),
                ExpectedWorkingScope(
                  Ast("r.p = s.p"),
                  Incoming(constants = Set("n", "r", "s", "m")),
                  Referenced(Set("r", "s")),
                  ExpectedWorkingScope.varExp("r", Set("n", "r", "s", "m")),
                  ExpectedWorkingScope.varExp("s", Set("n", "r", "s", "m"))
                )
              ),
              ExpectedWorkingScope(
                Ast("(m)"),
                PatternIncoming(topology = Set("n", "r", "s"), predicate = Set("n", "r", "m", "s"), path = Set("p")),
                Declared(variables = Seq("m")),
                Outgoing(variables = Set("m")),
                ExpectedResult.TableResult("m")
              )
            )
          )
        )
      )
    )
  }

  test(
    """MATCH (n), (m) WHERE n <> m
      |MATCH p = shortestPath((n)-[r:R*1..]->(m))
      |RETURN p, r""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (n), (m) WHERE n <> m
              |MATCH p = shortestPath((n)-[r:R*1..]->(m))
              |RETURN p, r""".stripMargin),
        Outgoing(variables = Set("p", "r")),
        ExpectedResult.TableResult("p", "r"),
        ExpectedWorkingScope(
          Ast("""MATCH (n), (m) WHERE n <> m""".stripMargin),
          Declared(variables = Seq("n", "m")),
          Outgoing(variables = Set("n", "m")),
          ExpectedWorkingScope(
            Ast("(n), (m)"),
            PatternIncoming(predicate = Set("n", "m")),
            Outgoing(variables = Set("n", "m")),
            Declared(variables = Seq("n", "m")),
            ExpectedResult.TableResult("n", "m"),
            ExpectedWorkingScope(
              Ast("(n)"),
              PatternIncoming(predicate = Set("n", "m")),
              Outgoing(variables = Set("n")),
              Declared(variables = Seq("n")),
              ExpectedResult.TableResult("n")
            ),
            ExpectedWorkingScope(
              Ast("(m)"),
              PatternIncoming(topology = Set("n"), predicate = Set("n", "m")),
              Outgoing(variables = Set("m")),
              Declared(variables = Seq("m")),
              ExpectedResult.TableResult("m")
            )
          ),
          ExpectedWorkingScope(
            Ast("n <> m"),
            Incoming(constants = Set("n", "m")),
            Referenced(Set("n", "m")),
            ExpectedWorkingScope.varExp("n", Set("n", "m")),
            ExpectedWorkingScope.varExp("m", Set("n", "m"))
          )
        ),
        ExpectedWorkingScope(
          Ast("""MATCH p = shortestPath((n)-[r:R*1..]->(m))""".stripMargin),
          Incoming(variables = Set("n", "m")),
          Referenced(Set("n", "m")),
          Declared(variables = Seq("p", "r")),
          Outgoing(variables = Set("n", "m", "p", "r")),
          ExpectedWorkingScope(
            Ast("""p = shortestPath((n)-[r:R*1..]->(m))""".stripMargin),
            PatternIncoming(topology = Set("n", "m"), predicate = Set("n", "r", "m"), path = Set("p")),
            Referenced(Set("n", "m")),
            Declared(variables = Seq("p", "r")),
            Outgoing(variables = Set("n", "m", "p", "r")),
            ExpectedResult.TableResult("n", "m", "p", "r"),
            ExpectedWorkingScope(
              Ast("""shortestPath((n)-[r:R*1..]->(m))""".stripMargin),
              PatternIncoming(topology = Set("n", "m"), predicate = Set("n", "r", "m"), path = Set("p")),
              Referenced(Set("n", "m")),
              Declared(variables = Seq("r")),
              Outgoing(variables = Set("n", "r", "m")),
              ExpectedResult.TableResult("n", "r", "m"),
              ExpectedWorkingScope(
                Ast("""(n)""".stripMargin),
                PatternIncoming(topology = Set("n", "m"), predicate = Set("n", "r", "m"), path = Set("p")),
                Referenced(Set("n")),
                Outgoing(variables = Set("n")),
                ExpectedResult.TableResult("n")
              ),
              ExpectedWorkingScope(
                Ast("""-[r:R*1..]->""".stripMargin),
                PatternIncoming(topology = Set("n", "m"), predicate = Set("n", "r", "m"), path = Set("p")),
                Declared(variables = Seq("r")),
                Outgoing(variables = Set("r")),
                ExpectedResult.TableResult("r"),
                ExpectedWorkingScope.constExp("R", Set("n", "m", "p", "r"))
              ),
              ExpectedWorkingScope(
                Ast("""(m)""".stripMargin),
                PatternIncoming(topology = Set("n", "m", "r"), predicate = Set("n", "r", "m"), path = Set("p")),
                Referenced(Set("m")),
                Outgoing(variables = Set("m")),
                ExpectedResult.TableResult("m")
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("""RETURN p, r""".stripMargin),
          Incoming(variables = Set("n", "m", "p", "r")),
          Referenced(Set("p", "r")),
          Outgoing(variables = Set("p", "r")),
          ExpectedResult.TableResult("p", "r"),
          ExpectedWorkingScope.varExp("p", Set("n", "m", "p", "r")),
          ExpectedWorkingScope.varExp("r", Set("n", "m", "p", "r"))
        )
      )
    )
  }

  for {
    (selector, normalizedSelector) <- Seq(
      "ANY SHORTEST PATH" -> "SHORTEST 1 PATHS",
      "ALL SHORTEST PATH" -> "ALL SHORTEST PATHS",
      "ANY 1 PATH" -> "ANY 1 PATHS",
      "SHORTEST 1 PATH GROUP" -> "SHORTEST 1 PATH GROUPS",
      "SHORTEST 1 PATH" -> "SHORTEST 1 PATHS"
    )
  } {
    test(
      s"""MATCH p = $selector (:A)-[r:R]->+(b:B)
         |RETURN p, r""".stripMargin
    ) {
      hasScope(
        ExpectedWorkingScope(
          Ast(s"""MATCH p = $normalizedSelector (:A) (()-[r:R]->())+ (b:B)
                 |RETURN p, r""".stripMargin),
          Outgoing(variables = Set("p", "r")),
          ExpectedResult.TableResult("p", "r"),
          ExpectedWorkingScope(
            Ast(s"MATCH p = $normalizedSelector (:A) (()-[r:R]->())+ (b:B)"),
            Declared(variables = Seq("p", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "b")),
            Outgoing(variables = Set("p", "r", "b")),
            ExpectedWorkingScope(
              Ast(s"p = $normalizedSelector (:A) (()-[r:R]->())+ (b:B)"),
              PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
              Declared(variables = Seq("p", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "b")),
              Outgoing(variables = Set("p", "r", "b")),
              ExpectedResult.TableResult("p", "r", "b"),
              ExpectedWorkingScope(
                Ast("p = (:A) (()-[r:R]->())+ (b:B)"),
                PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
                Declared(variables = Seq("p", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "b")),
                Outgoing(variables = Set("p", "r", "b")),
                ExpectedResult.TableResult("p", "r", "b"),
                ExpectedWorkingScope(
                  Ast("(:A) (()-[r:R]->())+ (b:B)"),
                  PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
                  Declared(variables = Seq("  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "b")),
                  Outgoing(variables = Set("r", "b")),
                  ExpectedResult.TableResult("r", "b"),
                  ExpectedWorkingScope(
                    Ast("(:A)"),
                    PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
                    Declared(variables = Seq("  SURVEYOR0")),
                    ExpectedResult.TableResult(),
                    ExpectedWorkingScope.constExp("A", Set("r", "b", "p"))
                  ),
                  ExpectedWorkingScope(
                    Ast("(()-[r:R]->())+"),
                    PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
                    Declared(variables = Seq("  SURVEYOR1", "  SURVEYOR2", "r")),
                    Referenced(variables = Set("r")),
                    Outgoing(variables = Set("r")),
                    ExpectedResult.TableResult("r"),
                    ExpectedWorkingScope(
                      Ast("()-[r:R]->()"),
                      PatternIncoming(predicate = Set("r", "b")),
                      Declared(variables = Seq("  SURVEYOR1", "r", "  SURVEYOR2")),
                      Outgoing(variables = Set("r")),
                      ExpectedResult.TableResult("r"),
                      ExpectedWorkingScope(
                        Ast("()"),
                        PatternIncoming(predicate = Set("r", "b")),
                        Declared(variables = Seq("  SURVEYOR1")),
                        ExpectedResult.TableResult()
                      ),
                      ExpectedWorkingScope(
                        Ast("-[r:R]->"),
                        PatternIncoming(predicate = Set("r", "b")),
                        Declared(variables = Seq("r")),
                        Outgoing(variables = Set("r")),
                        ExpectedResult.TableResult("r"),
                        ExpectedWorkingScope.constExp("R", Set("r", "b"))
                      ),
                      ExpectedWorkingScope(
                        Ast("()"),
                        PatternIncoming(topology = Set("r"), predicate = Set("r", "b")),
                        Declared(variables = Seq("  SURVEYOR2")),
                        ExpectedResult.TableResult()
                      )
                    )
                  ),
                  ExpectedWorkingScope(
                    Ast("(b:B)"),
                    PatternIncoming(topology = Set("r"), predicate = Set("r", "b"), path = Set("p")),
                    Declared(variables = Seq("b")),
                    Outgoing(variables = Set("b")),
                    ExpectedResult.TableResult("b"),
                    ExpectedWorkingScope.constExp("B", Set("r", "b", "p"))
                  )
                )
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("""RETURN p, r""".stripMargin),
            Incoming(variables = Set("p", "r", "b")),
            Referenced(Set("p", "r")),
            Outgoing(variables = Set("p", "r")),
            ExpectedResult.TableResult("p", "r"),
            ExpectedWorkingScope.varExp("p", Set("p", "r", "b")),
            ExpectedWorkingScope.varExp("r", Set("p", "r", "b"))
          )
        )
      )
    }
  }

  test(
    """MATCH p = ALL PATHS (:A)-[r:R]->+(b:B)
      |RETURN p, r""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH p = (:A) (()-[r:R]->())+ (b:B)
              |RETURN p, r""".stripMargin),
        Outgoing(variables = Set("p", "r")),
        ExpectedResult.TableResult("p", "r"),
        ExpectedWorkingScope(
          Ast("MATCH p = (:A) (()-[r:R]->())+ (b:B)"),
          Declared(variables = Seq("p", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "b")),
          Outgoing(variables = Set("p", "r", "b")),
          ExpectedWorkingScope(
            Ast("p = (:A) (()-[r:R]->())+ (b:B)"),
            PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
            Declared(variables = Seq("p", "  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "b")),
            Outgoing(variables = Set("p", "r", "b")),
            ExpectedResult.TableResult("p", "r", "b"),
            ExpectedWorkingScope(
              Ast("(:A) (()-[r:R]->())+ (b:B)"),
              PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
              Declared(variables = Seq("  SURVEYOR0", "  SURVEYOR1", "  SURVEYOR2", "r", "b")),
              Outgoing(variables = Set("r", "b")),
              ExpectedResult.TableResult("r", "b"),
              ExpectedWorkingScope(
                Ast("(:A)"),
                PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
                Declared(variables = Seq("  SURVEYOR0")),
                ExpectedResult.TableResult(),
                ExpectedWorkingScope.constExp("A", Set("r", "b", "p"))
              ),
              ExpectedWorkingScope(
                Ast("(()-[r:R]->())+"),
                PatternIncoming(predicate = Set("r", "b"), path = Set("p")),
                Declared(variables = Seq("  SURVEYOR1", "  SURVEYOR2", "r")),
                Referenced(variables = Set("r")),
                Outgoing(variables = Set("r")),
                ExpectedResult.TableResult("r"),
                ExpectedWorkingScope(
                  Ast("()-[r:R]->()"),
                  PatternIncoming(predicate = Set("r", "b")),
                  Declared(variables = Seq("  SURVEYOR1", "r", "  SURVEYOR2")),
                  Outgoing(variables = Set("r")),
                  ExpectedResult.TableResult("r"),
                  ExpectedWorkingScope(
                    Ast("()"),
                    PatternIncoming(predicate = Set("r", "b")),
                    Declared(variables = Seq("  SURVEYOR1")),
                    ExpectedResult.TableResult()
                  ),
                  ExpectedWorkingScope(
                    Ast("-[r:R]->"),
                    PatternIncoming(predicate = Set("r", "b")),
                    Declared(variables = Seq("r")),
                    Outgoing(variables = Set("r")),
                    ExpectedResult.TableResult("r"),
                    ExpectedWorkingScope.constExp("R", Set("r", "b"))
                  ),
                  ExpectedWorkingScope(
                    Ast("()"),
                    PatternIncoming(topology = Set("r"), predicate = Set("r", "b")),
                    Declared(variables = Seq("  SURVEYOR2")),
                    ExpectedResult.TableResult()
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("(b:B)"),
                PatternIncoming(topology = Set("r"), predicate = Set("r", "b"), path = Set("p")),
                Declared(variables = Seq("b")),
                Outgoing(variables = Set("b")),
                ExpectedResult.TableResult("b"),
                ExpectedWorkingScope.constExp("B", Set("r", "b", "p"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("""RETURN p, r""".stripMargin),
          Incoming(variables = Set("p", "r", "b")),
          Referenced(Set("p", "r")),
          Outgoing(variables = Set("p", "r")),
          ExpectedResult.TableResult("p", "r"),
          ExpectedWorkingScope.varExp("p", Set("p", "r", "b")),
          ExpectedWorkingScope.varExp("r", Set("p", "r", "b"))
        )
      )
    )
  }

  /*
   * For update
   */
  test("CREATE (n:A {p: 1})") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CREATE (n:A {p: 1})"), // query level
        Outgoing(variables = Set("n")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("CREATE (n:A {p: 1})"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("(n:A {p: 1})"),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A"),
            ExpectedWorkingScope.constExp("{p: 1}")
          )
        )
      )
    )
  }

  test(
    """UNWIND [1, 2, 3] AS x
      |CREATE (n:A {p: x})""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast(
          """UNWIND [1, 2, 3] AS x
            |CREATE (n:A {p: x})""".stripMargin
        ), // query level
        Outgoing(variables = Set("x", "n")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("CREATE (n:A {p: x})"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("x", "n")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("(n:A {p: x})"),
            PatternIncoming(topology = Set("x"), predicate = Set("x")),
            Referenced(Set("x")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope.constExp("A", Set("x")),
            ExpectedWorkingScope(
              Ast("{p: x}"),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          )
        )
      )
    )
  }

  test(
    """UNWIND [1, 2, 3] AS x
      |CREATE (n:$all("A" + x) {p: x})""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast(
          """UNWIND [1, 2, 3] AS x
            |CREATE (n:$all("A" + x) {p: x})""".stripMargin
        ), // query level
        Outgoing(variables = Set("x", "n")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("""CREATE (n:$all("A" + x) {p: x})"""),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("x", "n")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("""(n:$all("A" + x) {p: x})"""),
            PatternIncoming(topology = Set("x"), predicate = Set("x")),
            Referenced(Set("x")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope(
              Ast("""$all("A" + x)"""),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope(
                Ast(""""A" + x"""),
                Incoming(constants = Set("x")),
                Referenced(Set("x")),
                ExpectedWorkingScope.varExp("x", Set("x"))
              )
            ),
            ExpectedWorkingScope(
              Ast("{p: x}"),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          )
        )
      )
    )
  }

  test("CREATE (n:A {p: 1})-[:R]->(n)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CREATE (n:A {p: 1})-[:R]->(n)"), // query level
        Outgoing(variables = Set("n")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("CREATE (n:A {p: 1})-[:R]->(n)"),
          Declared(variables = Seq("n", "  SURVEYOR0")),
          Outgoing(variables = Set("n")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("(n:A {p: 1})-[:R]->(n)"),
            Declared(variables = Seq("n", "  SURVEYOR0")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1})"),
              Declared(variables = Seq("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n"),
              ExpectedWorkingScope.constExp("A"),
              ExpectedWorkingScope.constExp("{p: 1}")
            ),
            ExpectedWorkingScope(
              Ast("-[:R]->"),
              PatternIncoming(topology = Set("n")),
              Declared(variables = Seq("  SURVEYOR0")),
              ExpectedResult.TableResult(),
              ExpectedWorkingScope.constExp("R")
            ),
            ExpectedWorkingScope(
              Ast("(n)"),
              PatternIncoming(topology = Set("n")),
              Referenced(Set("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n")
            )
          )
        )
      )
    )
  }

  test("CREATE (n:A {p: 1})-[r:R]->(m)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CREATE (n:A {p: 1})-[r:R]->(m)"), // query level
        Outgoing(variables = Set("n", "r", "m")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("CREATE (n:A {p: 1})-[r:R]->(m)"),
          Declared(variables = Seq("n", "r", "m")),
          Outgoing(variables = Set("n", "r", "m")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("(n:A {p: 1})-[r:R]->(m)"),
            Declared(variables = Seq("n", "r", "m")),
            Outgoing(variables = Set("n", "r", "m")),
            ExpectedResult.TableResult("n", "r", "m"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1})"),
              Declared(variables = Seq("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n"),
              ExpectedWorkingScope.constExp("A"),
              ExpectedWorkingScope.constExp("{p: 1}")
            ),
            ExpectedWorkingScope(
              Ast("-[r:R]->"),
              PatternIncoming(topology = Set("n")),
              Declared(variables = Seq("r")),
              Outgoing(variables = Set("r")),
              ExpectedWorkingScope.constExp("R"),
              ExpectedResult.TableResult("r")
            ),
            ExpectedWorkingScope(
              Ast("(m)"),
              PatternIncoming(topology = Set("n", "r")),
              Declared(variables = Seq("m")),
              Outgoing(variables = Set("m")),
              ExpectedResult.TableResult("m")
            )
          )
        )
      )
    )
  }

  test("CREATE p = (n:A {p: 1})-[r:R]->(m)") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CREATE p = (n:A {p: 1})-[r:R]->(m)"), // query level
        Outgoing(variables = Set("p", "n", "r", "m")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("CREATE p = (n:A {p: 1})-[r:R]->(m)"),
          Declared(variables = Seq("p", "n", "r", "m")),
          Outgoing(variables = Set("p", "n", "r", "m")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("p = (n:A {p: 1})-[r:R]->(m)"),
            Declared(variables = Seq("p", "n", "r", "m")),
            Outgoing(variables = Set("p", "n", "r", "m")),
            ExpectedResult.TableResult("p", "n", "r", "m"),
            ExpectedWorkingScope(
              Ast("(n:A {p: 1})-[r:R]->(m)"),
              Declared(variables = Seq("n", "r", "m")),
              Outgoing(variables = Set("n", "r", "m")),
              ExpectedResult.TableResult("n", "r", "m"),
              ExpectedWorkingScope(
                Ast("(n:A {p: 1})"),
                Declared(variables = Seq("n")),
                Outgoing(variables = Set("n")),
                ExpectedResult.TableResult("n"),
                ExpectedWorkingScope.constExp("A"),
                ExpectedWorkingScope.constExp("{p: 1}")
              ),
              ExpectedWorkingScope(
                Ast("-[r:R]->"),
                PatternIncoming(topology = Set("n")),
                Declared(variables = Seq("r")),
                Outgoing(variables = Set("r")),
                ExpectedResult.TableResult("r"),
                ExpectedWorkingScope.constExp("R")
              ),
              ExpectedWorkingScope(
                Ast("(m)"),
                PatternIncoming(topology = Set("n", "r")),
                Declared(variables = Seq("m")),
                Outgoing(variables = Set("m")),
                ExpectedResult.TableResult("m")
              )
            )
          )
        )
      )
    )
  }

  test("CREATE (a:A {p: 1}), (b:B {p: 2})") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CREATE (a:A {p: 1}), (b:B {p: 2})"), // query level
        Outgoing(variables = Set("a", "b")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("CREATE (a:A {p: 1}), (b:B {p: 2})"),
          Declared(variables = Seq("a", "b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("(a:A {p: 1}), (b:B {p: 2})"),
            Declared(variables = Seq("a", "b")),
            Outgoing(variables = Set("a", "b")),
            ExpectedResult.TableResult("a", "b"),
            ExpectedWorkingScope(
              Ast("(a:A {p: 1})"),
              Declared(variables = Seq("a")),
              Outgoing(variables = Set("a")),
              ExpectedResult.TableResult("a"),
              ExpectedWorkingScope.constExp("A"),
              ExpectedWorkingScope.constExp("{p: 1}")
            ),
            ExpectedWorkingScope(
              Ast("(b:B {p: 2})"),
              PatternIncoming(topology = Set("a")),
              Declared(variables = Seq("b")),
              Outgoing(variables = Set("b")),
              ExpectedResult.TableResult("b"),
              ExpectedWorkingScope.constExp("B"),
              ExpectedWorkingScope.constExp("{p: 2}")
            )
          )
        )
      )
    )
  }
}
