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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.symbols.CTAny

class InsertParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  private def assertExpectedNodeAst(nodePattern: NodePattern): Unit = {
    parsesTo[Statement](
      singleQuery(
        insert(
          nodePattern
        )
      )
    )
  }

  private def assertExpectedRelAst(
    nodePattern1: NodePattern,
    relName: Option[String],
    relType: LabelExpression,
    relProperties: Option[Expression],
    direction: SemanticDirection,
    nodePattern2: NodePattern
  ): Unit = {
    parsesTo[Statement](
      singleQuery(
        insert(
          relationshipChain(
            nodePattern1,
            relPat(relName, Some(relType), None, relProperties, None, direction),
            nodePattern2
          )
        )
      )
    )
  }

  private val nodeFillers: Seq[(String, NodePattern)] = Seq(
    // No labels
    ("", nodePat()),
    ("n", nodePat(Some("n"))),
    ("{prop: 1}", nodePat(None, None, Some(mapOf(("prop", literalInt(1)))))),
    (
      "x {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        None,
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // IS A
    ("IS A", nodePat(None, Some(labelLeaf("A", containsIs = true)))),
    ("n IS A", nodePat(Some("n"), Some(labelLeaf("A", containsIs = true)))),
    (
      "IS A {prop: 1}",
      nodePat(
        None,
        Some(labelLeaf("A", containsIs = true)),
        Some(mapOf(("prop", literalInt(1))))
      )
    ),
    (
      "x IS A {}",
      nodePat(
        Some("x"),
        Some(labelLeaf("A", containsIs = true)),
        Some(mapOf())
      )
    ),

    // IS A&B
    (
      "IS A&B",
      nodePat(
        None,
        Some(labelConjunction(
          labelLeaf("A", containsIs = true),
          labelLeaf("B", containsIs = true),
          containsIs = true
        ))
      )
    ),
    (
      "n IS A&B",
      nodePat(
        Some("n"),
        Some(labelConjunction(
          labelLeaf("A", containsIs = true),
          labelLeaf("B", containsIs = true),
          containsIs = true
        ))
      )
    ),
    (
      "IS A&B {prop: $value}",
      nodePat(
        None,
        Some(
          labelConjunction(labelLeaf("A", containsIs = true), labelLeaf("B", containsIs = true), containsIs = true)
        ),
        Some(mapOf(("prop", parameter("value", CTAny))))
      )
    ),
    (
      "x IS A&B {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        Some(labelConjunction(
          labelLeaf("A", containsIs = true),
          labelLeaf("B", containsIs = true),
          containsIs = true
        )),
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // :A
    (":A", nodePat(None, Some(labelLeaf("A")))),
    ("n :A", nodePat(Some("n"), Some(labelLeaf("A")))),
    (
      ":A $map",
      nodePat(
        None,
        Some(labelLeaf("A")),
        Some(parameter("map", CTAny))
      )
    ),
    (
      "x :A {prop1: 'a', prop2: 42}",
      nodePat(
        Some("x"),
        Some(labelLeaf("A")),
        Some(mapOf(("prop1", literalString("a")), ("prop2", literalInt(42))))
      )
    ),

    // :A&B
    (
      ":A&B",
      nodePat(
        None,
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B")))
      )
    ),
    (
      "n :A&B",
      nodePat(
        Some("n"),
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B")))
      )
    ),
    (
      ":A&B {prop: duration('P1Y')}",
      nodePat(
        None,
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B"))),
        Some(mapOf(("prop", function("duration", literalString("P1Y")))))
      )
    ),
    (
      "x :A&B {prop1: 'a', prop2: false}",
      nodePat(
        Some("x"),
        Some(labelConjunction(labelLeaf("A"), labelLeaf("B"))),
        Some(mapOf(("prop1", literalString("a")), ("prop2", falseLiteral)))
      )
    )
  )

  for {
    (nodeText1, nodePattern1) <- nodeFillers
  } yield {
    test(s"INSERT ($nodeText1)") {
      assertExpectedNodeAst(nodePattern1)
    }

    for {
      (nodeText2, nodePattern2) <- nodeFillers
    } yield {
      test(s"INSERT ($nodeText1)-[IS A]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A", containsIs = true),
          None,
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[IS A]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A", containsIs = true),
          None,
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)-[:A]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A"),
          None,
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[:A]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A"),
          None,
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)-[r IS A]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A", containsIs = true),
          None,
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[r IS A]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A", containsIs = true),
          None,
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)-[r:A]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A"),
          None,
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[r:A]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A"),
          None,
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)-[IS A {prop: 3}]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A", containsIs = true),
          Some(mapOf(("prop", literalInt(3)))),
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[IS A $$map]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A", containsIs = true),
          Some(parameter("map", CTAny)),
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)-[:A {prop: $$value}]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A"),
          Some(mapOf(("prop", parameter("value", CTAny)))),
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[:A {prop: '1', bool: true}]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          None,
          labelRelTypeLeaf("A"),
          Some(mapOf(("prop", literalString("1")), ("bool", trueLiteral))),
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)-[r IS A{prop:4.5,  prop2: 5}]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A", containsIs = true),
          Some(mapOf(("prop", literalFloat(4.5)), ("prop2", literalInt(5)))),
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[r IS A {prop1: 'prop1', prop2: '', prop3: date('2023-11-29')}]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A", containsIs = true),
          Some(mapOf(
            ("prop1", literalString("prop1")),
            ("prop2", literalString("")),
            ("prop3", function("date", literalString("2023-11-29")))
          )),
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)-[r:A {prop: 1 > 2}]->($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A"),
          Some(mapOf(("prop", greaterThan(literalInt(1), literalInt(2))))),
          SemanticDirection.OUTGOING,
          nodePattern2
        )
      }

      test(s"INSERT ($nodeText1)<-[r:A {}]-($nodeText2)") {
        assertExpectedRelAst(
          nodePattern1,
          Some("r"),
          labelRelTypeLeaf("A"),
          Some(mapOf()),
          SemanticDirection.INCOMING,
          nodePattern2
        )
      }
    }
  }

  // More advanced patterns

  test("INSERT ()-[:R]->(IS B)-[:S {prop:'s'}]->({prop: 42})<-[r IS T]-(n:A)") {
    parsesTo[Statement](
      singleQuery(
        insert(
          relationshipChain(
            nodePat(),
            relPat(labelExpression = Some(labelRelTypeLeaf("R"))),
            nodePat(labelExpression = Some(labelLeaf("B", containsIs = true))),
            relPat(
              labelExpression = Some(labelRelTypeLeaf("S")),
              properties = Some(mapOf(("prop", literalString("s"))))
            ),
            nodePat(properties = Some(mapOf(("prop", literalInt(42))))),
            relPat(
              Some("r"),
              Some(labelRelTypeLeaf("T", containsIs = true)),
              direction = SemanticDirection.INCOMING
            ),
            nodePat(Some("n"), Some(labelLeaf("A")))
          )
        )
      )
    )
  }

  test("INSERT (n)-[:R]->(IS B), (n)-[:S {prop:'s'}]->({prop: 42})") {
    parsesTo[Statement](
      singleQuery(
        insert(
          Seq(
            relationshipChain(
              nodePat(Some("n")),
              relPat(labelExpression = Some(labelRelTypeLeaf("R"))),
              nodePat(labelExpression = Some(labelLeaf("B", containsIs = true)))
            ),
            relationshipChain(
              nodePat(Some("n")),
              relPat(
                labelExpression = Some(labelRelTypeLeaf("S")),
                properties = Some(mapOf(("prop", literalString("s"))))
              ),
              nodePat(properties = Some(mapOf(("prop", literalInt(42)))))
            )
          )
        )
      )
    )
  }

  // Edge cases for IS keyword

  test("INSERT (IS)") {
    assertExpectedNodeAst(nodePat(Some("IS")))
  }

  test("INSERT (IS {prop:42})") {
    assertExpectedNodeAst(nodePat(
      Some("IS"),
      properties = Some(mapOf(("prop", literalInt(42))))
    ))
  }

  test("INSERT (IS IS)") {
    assertExpectedNodeAst(nodePat(labelExpression = Some(labelLeaf("IS", containsIs = true))))
  }

  test("INSERT (:IS)") {
    assertExpectedNodeAst(nodePat(labelExpression = Some(labelLeaf("IS"))))
  }

  test("INSERT (IS IS IS)") {
    assertExpectedNodeAst(nodePat(Some("IS"), Some(labelLeaf("IS", containsIs = true))))
  }

  test("INSERT ()-[IS IS]->()") {
    assertExpectedRelAst(
      nodePat(),
      None,
      labelRelTypeLeaf("IS", containsIs = true),
      None,
      SemanticDirection.OUTGOING,
      nodePat()
    )
  }

  test("INSERT ()-[IS IS IS]->()") {
    assertExpectedRelAst(
      nodePat(),
      Some("IS"),
      labelRelTypeLeaf("IS", containsIs = true),
      None,
      SemanticDirection.OUTGOING,
      nodePat()
    )
  }

  // The following cases will fail parsing for both CREATE and INSERT

  test("INSERT (:A n)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'n'")
      case _ => _.withSyntaxError(
          """Invalid input 'n': expected a parameter, '&', ')', ':' or '{' (line 1, column 12 (offset: 11))
            |"INSERT (:A n)"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ({prop:42} :A)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ':'")
      case _ => _.withSyntaxError(
          """Invalid input ':': expected ')' (line 1, column 19 (offset: 18))
            |"INSERT ({prop:42} :A)"
            |                   ^""".stripMargin
        )
    }
  }

  test("INSERT ()-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected '[' (line 1, column 11 (offset: 10))
            |"INSERT ()-()"
            |           ^""".stripMargin
        )
    }
  }

  test("INSERT ()->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input '>': expected '[' (line 1, column 11 (offset: 10))
            |"INSERT ()->()"
            |           ^""".stripMargin
        )
    }
  }

  test("INSERT ()[]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '['")
      case _ => _.withSyntaxError(
          """Invalid input '[': expected 'FOREACH', ',', '-', '<', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 10 (offset: 9))
            |"INSERT ()[]->()"
            |          ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[]>()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ']'")
      case _ => _.withSyntaxError(
          """Invalid input ']': expected a variable name, ':' or 'IS' (line 1, column 12 (offset: 11))
            |"INSERT ()-[]>()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()-]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input ']': expected '[' (line 1, column 11 (offset: 10))
            |"INSERT ()-]->()"
            |           ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input '-': expected a variable name, ':' or 'IS' (line 1, column 12 (offset: 11))
            |"INSERT ()-[->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[{prop:42} :R]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '{'")
      case _ => _.withSyntaxError(
          """Invalid input '{': expected a variable name, ':' or 'IS' (line 1, column 12 (offset: 11))
            |"INSERT ()-[{prop:42} :R]->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[:R r]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input 'r': expected a parameter, ']' or '{' (line 1, column 15 (offset: 14))
            |"INSERT ()-[:R r]->()"
            |               ^""".stripMargin
        )
    }
  }

  test("INSERT ALL PATHS (n)-[:R]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'ALL'")
      case _ => _.withSyntaxError(
          """Invalid input 'PATHS': expected '=' (line 1, column 12 (offset: 11))
            |"INSERT ALL PATHS (n)-[:R]->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ANY SHORTEST PATHS p = (n)-[:R]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'ANY'")
      case _ => _.withSyntaxError(
          """Invalid input 'SHORTEST': expected '=' (line 1, column 12 (offset: 11))
            |"INSERT ANY SHORTEST PATHS p = (n)-[:R]->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT SHORTEST 2 PATH (n)-[:R]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHORTEST'")
      case _ => _.withSyntaxError(
          """Invalid input '2': expected '=' (line 1, column 17 (offset: 16))
            |"INSERT SHORTEST 2 PATH (n)-[:R]->()"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT SHORTEST 2 GROUPS (n)-[:R]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHORTEST'")
      case _ => _.withSyntaxError(
          """Invalid input '2': expected '=' (line 1, column 17 (offset: 16))
            |"INSERT SHORTEST 2 GROUPS (n)-[:R]->()"
            |                 ^""".stripMargin
        )
    }
  }

  // The following cases will parse but fail in semantic checking for both CREATE and INSERT.

  test("INSERT ()-[:R]-()") {
    assertExpectedRelAst(
      nodePat(),
      None,
      labelRelTypeLeaf("R"),
      None,
      SemanticDirection.BOTH,
      nodePat()
    )
  }

  test("INSERT ()<-[:R]->()") {
    assertExpectedRelAst(
      nodePat(),
      None,
      labelRelTypeLeaf("R"),
      None,
      SemanticDirection.BOTH,
      nodePat()
    )
  }

  // The following cases will parse, but fail in semantic checking for CREATE.
  // For INSERT, they fail in parsing.

  test("INSERT (n:A|B)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '|'")
      case _ => _.withSyntaxError(
          """Invalid input '|': expected a parameter, '&', ')', ':' or '{' (line 1, column 12 (offset: 11))
            |"INSERT (n:A|B)"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT (n:A|:B)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '|'")
      case _ => _.withSyntaxError(
          """Invalid input '|': expected a parameter, '&', ')', ':' or '{' (line 1, column 12 (offset: 11))
            |"INSERT (n:A|:B)"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT (n IS A|B)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '|'")
      case _ => _.withSyntaxError(
          """Invalid input '|': expected a parameter, '&', ')', ':' or '{' (line 1, column 15 (offset: 14))
            |"INSERT (n IS A|B)"
            |               ^""".stripMargin
        )
    }
  }

  test("INSERT (n IS A:B)") {
    failsParsing[Statements].withMessageStart("Colon `:` conjunction is not allowed in INSERT")
  }

  test("INSERT (n IS !(A&B))") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'IS'")
      case _ => _.withSyntaxError(
          """Invalid input '!': expected an identifier (line 1, column 14 (offset: 13))
            |"INSERT (n IS !(A&B))"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT (IS %)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '%'")
      case _ => _.withSyntaxError(
          """Invalid input '%': expected a parameter, an identifier, ')', ':', 'IS' or '{' (line 1, column 12 (offset: 11))
            |"INSERT (IS %)"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT (WHERE true)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'true'")
      case _ => _.withSyntaxError(
          """Invalid input 'true': expected a parameter, ')', ':', 'IS' or '{' (line 1, column 15 (offset: 14))
            |"INSERT (WHERE true)"
            |               ^""".stripMargin
        )
    }
  }

  test("INSERT (n WHERE n.prop = 1)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected a parameter, ')', ':', 'IS' or '{' (line 1, column 11 (offset: 10))
            |"INSERT (n WHERE n.prop = 1)"
            |           ^""".stripMargin
        )
    }
  }

  test("INSERT ({prop:2} WHERE true)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected ')' (line 1, column 18 (offset: 17))
            |"INSERT ({prop:2} WHERE true)"
            |                  ^""".stripMargin
        )
    }
  }

  test("INSERT (n {prop:2} WHERE n.prop = 1)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected ')' (line 1, column 20 (offset: 19))
            |"INSERT (n {prop:2} WHERE n.prop = 1)"
            |                    ^""".stripMargin
        )
    }
  }

  test("INSERT (:A WHERE true)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected a parameter, '&', ')', ':' or '{' (line 1, column 12 (offset: 11))
            |"INSERT (:A WHERE true)"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT (n:A WHERE true)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected a parameter, '&', ')', ':' or '{' (line 1, column 13 (offset: 12))
            |"INSERT (n:A WHERE true)"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT (:A {prop: 2} WHERE true)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected ')' (line 1, column 22 (offset: 21))
            |"INSERT (:A {prop: 2} WHERE true)"
            |                      ^""".stripMargin
        )
    }
  }

  test("INSERT (n:A {prop: 2} WHERE n.prop > 42)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected ')' (line 1, column 23 (offset: 22))
            |"INSERT (n:A {prop: 2} WHERE n.prop > 42)"
            |                       ^""".stripMargin
        )
    }
  }

  test("INSERT ()--()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input '-': expected '[' (line 1, column 11 (offset: 10))
            |"INSERT ()--()"
            |           ^""".stripMargin
        )
    }
  }

  test("INSERT ()-->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input '-': expected '[' (line 1, column 11 (offset: 10))
            |"INSERT ()-->()"
            |           ^""".stripMargin
        )
    }
  }

  test("INSERT ()<--()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input '-': expected '[' (line 1, column 12 (offset: 11))
            |"INSERT ()<--()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '-'")
      case _ => _.withSyntaxError(
          """Invalid input '-': expected '[' (line 1, column 12 (offset: 11))
            |"INSERT ()<-->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[:Rel1|Rel2]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '|'")
      case _ => _.withSyntaxError(
          """Invalid input '|': expected a parameter, ']' or '{' (line 1, column 17 (offset: 16))
            |"INSERT ()-[:Rel1|Rel2]->()"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[:Rel1&Rel2]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '&'")
      case _ => _.withSyntaxError(
          """Invalid input '&': expected a parameter, ']' or '{' (line 1, column 17 (offset: 16))
            |"INSERT ()-[:Rel1&Rel2]->()"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[:!Rel]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '!'")
      case _ => _.withSyntaxError(
          """Invalid input '!': expected an identifier (line 1, column 13 (offset: 12))
            |"INSERT ()-[:!Rel]->()"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ']'")
      case _ => _.withSyntaxError(
          """Invalid input ']': expected a variable name, ':' or 'IS' (line 1, column 12 (offset: 11))
            |"INSERT ()-[]->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input ']': expected ':' or 'IS' (line 1, column 13 (offset: 12))
            |"INSERT ()-[r]->()"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[{prop: 2}]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '{'")
      case _ => _.withSyntaxError(
          """Invalid input '{': expected a variable name, ':' or 'IS' (line 1, column 12 (offset: 11))
            |"INSERT ()-[{prop: 2}]->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[*1..3]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a variable name, ':' or 'IS' (line 1, column 12 (offset: 11))
            |"INSERT ()-[*1..3]->()"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'true': expected ':' or 'IS' (line 1, column 18 (offset: 17))
            |"INSERT ()-[WHERE true]->()"
            |                  ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[r {prop: 2}]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input '{': expected ':' or 'IS' (line 1, column 15 (offset: 14))
            |"INSERT ()<-[r {prop: 2}]-()"
            |               ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[r *1..3]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected ':' or 'IS' (line 1, column 15 (offset: 14))
            |"INSERT ()<-[r *1..3]-()"
            |               ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected ':' or 'IS' (line 1, column 14 (offset: 13))
            |"INSERT ()-[r WHERE true]->()"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[*1..3 {prop:2} ]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a variable name, ':' or 'IS' (line 1, column 13 (offset: 12))
            |"INSERT ()<-[*1..3 {prop:2} ]-()"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[{prop:2} WHERE true]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '{'")
      case _ => _.withSyntaxError(
          """Invalid input '{': expected a variable name, ':' or 'IS' (line 1, column 13 (offset: 12))
            |"INSERT ()<-[{prop:2} WHERE true]-()"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[*1..3 WHERE true]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a variable name, ':' or 'IS' (line 1, column 13 (offset: 12))
            |"INSERT ()<-[*1..3 WHERE true]-()"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r *1..3 {prop:2}]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected ':' or 'IS' (line 1, column 14 (offset: 13))
            |"INSERT ()-[r *1..3 {prop:2}]->()"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r {prop:2} WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input '{': expected ':' or 'IS' (line 1, column 14 (offset: 13))
            |"INSERT ()-[r {prop:2} WHERE true]->()"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r *1..3 WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected ':' or 'IS' (line 1, column 14 (offset: 13))
            |"INSERT ()-[r *1..3 WHERE true]->()"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r *1..3 {prop:2} WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'r'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected ':' or 'IS' (line 1, column 14 (offset: 13))
            |"INSERT ()-[r *1..3 {prop:2} WHERE true]->()"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[:R *1..3]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a parameter, ']' or '{' (line 1, column 15 (offset: 14))
            |"INSERT ()-[:R *1..3]->()"
            |               ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[:R WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected a parameter, ']' or '{' (line 1, column 15 (offset: 14))
            |"INSERT ()-[:R WHERE true]->()"
            |               ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[r :R *1..3]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a parameter, ']' or '{' (line 1, column 18 (offset: 17))
            |"INSERT ()<-[r :R *1..3]-()"
            |                  ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r :R WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected a parameter, ']' or '{' (line 1, column 17 (offset: 16))
            |"INSERT ()-[r :R WHERE true]->()"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[:R *1..3 {prop:2} ]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a parameter, ']' or '{' (line 1, column 16 (offset: 15))
            |"INSERT ()<-[:R *1..3 {prop:2} ]-()"
            |                ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[:R {prop:2} WHERE true]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected ']' (line 1, column 25 (offset: 24))
            |"INSERT ()<-[:R {prop:2} WHERE true]-()"
            |                         ^""".stripMargin
        )
    }
  }

  test("INSERT ()<-[:R *1..3 WHERE true]-()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a parameter, ']' or '{' (line 1, column 16 (offset: 15))
            |"INSERT ()<-[:R *1..3 WHERE true]-()"
            |                ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r :R *1..3 {prop:2}]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a parameter, ']' or '{' (line 1, column 17 (offset: 16))
            |"INSERT ()-[r :R *1..3 {prop:2}]->()"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r :R {prop:2} WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'WHERE'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHERE': expected ']' (line 1, column 26 (offset: 25))
            |"INSERT ()-[r :R {prop:2} WHERE true]->()"
            |                          ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r :R *1..3 WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a parameter, ']' or '{' (line 1, column 17 (offset: 16))
            |"INSERT ()-[r :R *1..3 WHERE true]->()"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT ()-[r :R *1..3 {prop:2} WHERE true]->()") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '*'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected a parameter, ']' or '{' (line 1, column 17 (offset: 16))
            |"INSERT ()-[r :R *1..3 {prop:2} WHERE true]->()"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT shortestPath((a)-[r]->(b))") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'shortestPath'")
      // Not very helpful :(
      case _ => _.withSyntaxError(
          """Invalid input '(': expected '=' (line 1, column 20 (offset: 19))
            |"INSERT shortestPath((a)-[r]->(b))"
            |                    ^""".stripMargin
        )
    }
  }

  test("INSERT allShortestPaths((a)-[r]->(b))") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'allShortestPaths'")
      // Not very helpful :(
      case _ => _.withSyntaxError(
          """Invalid input '(': expected '=' (line 1, column 24 (offset: 23))
            |"INSERT allShortestPaths((a)-[r]->(b))"
            |                        ^""".stripMargin
        )
    }
  }

  test("INSERT (a)-[:R]->(b)(a)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '('")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected 'FOREACH', ',', '-', '<', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 21 (offset: 20))
            |"INSERT (a)-[:R]->(b)(a)"
            |                     ^""".stripMargin
        )
    }
  }

  test("INSERT ((n)-[r]->(m))*") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '('")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected a parameter, a variable name, ')', ':', 'IS' or '{' (line 1, column 9 (offset: 8))
            |"INSERT ((n)-[r]->(m))*"
            |         ^""".stripMargin
        )
    }
  }

  test("INSERT ((a)-->(b) WHERE a.prop > b.prop)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '('")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected a parameter, a variable name, ')', ':', 'IS' or '{' (line 1, column 9 (offset: 8))
            |"INSERT ((a)-->(b) WHERE a.prop > b.prop)"
            |         ^""".stripMargin
        )
    }
  }

  // The following cases will parse and be semantically correct for CREATE.
  // For INSERT, they fail in parsing.

  test("INSERT (:(A&B))") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ':'")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected an identifier (line 1, column 10 (offset: 9))
            |"INSERT (:(A&B))"
            |          ^""".stripMargin
        )
    }
  }

  test("INSERT (IS (A&B)&C)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '('")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected a parameter, an identifier, ')', ':', 'IS' or '{' (line 1, column 12 (offset: 11))
            |"INSERT (IS (A&B)&C)"
            |            ^""".stripMargin
        )
    }
  }

  test("INSERT (:A:B)") {
    failsParsing[Statements].withMessageStart(
      "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead. (line 1, column 11 (offset: 10))"
    )
  }

  test("INSERT (n:A&B:C)") {
    failsParsing[Statements].withMessageStart(
      "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead. (line 1, column 14 (offset: 13))"
    )
  }

  test("INSERT (n:A)-[:R]->(:B:C)") {
    failsParsing[Statements].withMessageStart(
      "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead. (line 1, column 23 (offset: 22))"
    )
  }

  test("INSERT p=()-[:R]->()") {
    failsParsing[Statements].withMessageStart(
      "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name. (line 1, column 8 (offset: 7))"
    )
  }

  test("INSERT (), p=()-[:R]->()") {
    failsParsing[Statements].withMessageStart(
      "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name. (line 1, column 12 (offset: 11))"
    )
  }

  // INSERT should not work as a synonym to CREATE for DDL

  test("INSERT USER foo SET PASSWORD 'password'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'USER'")
      case _ => _.withSyntaxError(
          """Invalid input 'foo': expected '=' (line 1, column 13 (offset: 12))
            |"INSERT USER foo SET PASSWORD 'password'"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT ROLE role") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'ROLE'")
      case _ => _.withSyntaxError(
          """Invalid input 'role': expected '=' (line 1, column 13 (offset: 12))
            |"INSERT ROLE role"
            |             ^""".stripMargin
        )
    }
  }

  test("INSERT DATABASE foo") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'DATABASE'")
      case _ => _.withSyntaxError(
          """Invalid input 'foo': expected '=' (line 1, column 17 (offset: 16))
            |"INSERT DATABASE foo"
            |                 ^""".stripMargin
        )
    }
  }

  test("INSERT COMPOSITE DATABASE name") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'COMPOSITE'")
      // Not very helpful
      case _ => _.withSyntaxError(
          """Invalid input 'DATABASE': expected '=' (line 1, column 18 (offset: 17))
            |"INSERT COMPOSITE DATABASE name"
            |                  ^""".stripMargin
        )
    }
  }

  test("INSERT ALIAS alias FOR DATABASE foo") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'ALIAS'")
      // Not very helpful
      case _ => _.withSyntaxError(
          """Invalid input 'alias': expected '=' (line 1, column 14 (offset: 13))
            |"INSERT ALIAS alias FOR DATABASE foo"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT INDEX FOR (n:Label) ON n.prop") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'INDEX'")
      // Not very helpful
      case _ => _.withSyntaxError(
          """Invalid input 'FOR': expected '=' (line 1, column 14 (offset: 13))
            |"INSERT INDEX FOR (n:Label) ON n.prop"
            |              ^""".stripMargin
        )
    }
  }

  test("INSERT CONSTRAINT FOR (n:Label) REQUIRE n.prop IS NOT NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'CONSTRAINT'")
      // Not very helpful :(
      case _ => _.withMessage(
          """Invalid input 'FOR': expected '=' (line 1, column 19 (offset: 18))
            |"INSERT CONSTRAINT FOR (n:Label) REQUIRE n.prop IS NOT NULL"
            |                   ^""".stripMargin
        )
    }
  }
}
