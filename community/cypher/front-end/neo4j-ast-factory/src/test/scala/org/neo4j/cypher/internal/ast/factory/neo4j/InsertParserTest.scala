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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.symbols.CTAny

class InsertParserTest extends ParserSyntaxTreeBase[Cst.Statement, ast.Statement] {

  implicit private val javaccRule: JavaccRule[Statement] = JavaccRule.Statement
  implicit private val antlrRule: AntlrRule[Cst.Statement] = AntlrRule.Statement

  private def assertExpectedNodeAst(nodePattern: NodePattern): Unit = {
    gives(
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
    gives(
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
    gives(
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
    gives(
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
    failsToParse
  }

  test("INSERT ({prop:42} :A)") {
    failsToParse
  }

  test("INSERT ()-()") {
    failsToParse
  }

  test("INSERT ()->()") {
    failsToParse
  }

  test("INSERT ()[]->()") {
    failsToParse
  }

  test("INSERT ()-[]>()") {
    failsToParse
  }

  test("INSERT ()-]->()") {
    failsToParse
  }

  test("INSERT ()-[->()") {
    failsToParse
  }

  test("INSERT ()-[{prop:42} :R]->()") {
    failsToParse
  }

  test("INSERT ()-[:R r]->()") {
    failsToParse
  }

  test("INSERT ALL PATHS (n)-[:R]->()") {
    failsToParse
  }

  test("INSERT ANY SHORTEST PATHS p = (n)-[:R]->()") {
    failsToParse
  }

  test("INSERT SHORTEST 2 PATH (n)-[:R]->()") {
    failsToParse
  }

  test("INSERT SHORTEST 2 GROUPS (n)-[:R]->()") {
    failsToParse
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
    failsToParse
  }

  test("INSERT (n:A|:B)") {
    failsToParse
  }

  test("INSERT (n IS A|B)") {
    failsToParse
  }

  test("INSERT (n IS A:B)") {
    failsToParse
  }

  test("INSERT (n IS !(A&B))") {
    failsToParse
  }

  test("INSERT (IS %)") {
    failsToParse
  }

  test("INSERT (WHERE true)") {
    failsToParse
  }

  test("INSERT (n WHERE n.prop = 1)") {
    failsToParse
  }

  test("INSERT ({prop:2} WHERE true)") {
    failsToParse
  }

  test("INSERT (n {prop:2} WHERE n.prop = 1)") {
    failsToParse
  }

  test("INSERT (:A WHERE true)") {
    failsToParse
  }

  test("INSERT (n:A WHERE true)") {
    failsToParse
  }

  test("INSERT (:A {prop: 2} WHERE true)") {
    failsToParse
  }

  test("INSERT (n:A {prop: 2} WHERE n.prop > 42)") {
    failsToParse
  }

  test("INSERT ()--()") {
    failsToParse
  }

  test("INSERT ()-->()") {
    failsToParse
  }

  test("INSERT ()<--()") {
    failsToParse
  }

  test("INSERT ()<-->()") {
    failsToParse
  }

  test("INSERT ()-[:Rel1|Rel2]->()") {
    failsToParse
  }

  test("INSERT ()-[:Rel1&Rel2]->()") {
    failsToParse
  }

  test("INSERT ()-[:!Rel]->()") {
    failsToParse
  }

  test("INSERT ()-[]->()") {
    failsToParse
  }

  test("INSERT ()-[r]->()") {
    failsToParse
  }

  test("INSERT ()-[{prop: 2}]->()") {
    failsToParse
  }

  test("INSERT ()-[*1..3]->()") {
    failsToParse
  }

  test("INSERT ()-[WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()<-[r {prop: 2}]-()") {
    failsToParse
  }

  test("INSERT ()<-[r *1..3]-()") {
    failsToParse
  }

  test("INSERT ()-[r WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()<-[*1..3 {prop:2} ]-()") {
    failsToParse
  }

  test("INSERT ()<-[{prop:2} WHERE true]-()") {
    failsToParse
  }

  test("INSERT ()<-[*1..3 WHERE true]-()") {
    failsToParse
  }

  test("INSERT ()-[r *1..3 {prop:2}]->()") {
    failsToParse
  }

  test("INSERT ()-[r {prop:2} WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()-[r *1..3 WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()-[r *1..3 {prop:2} WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()-[:R *1..3]->()") {
    failsToParse
  }

  test("INSERT ()-[:R WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()<-[r :R *1..3]-()") {
    failsToParse
  }

  test("INSERT ()-[r :R WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()<-[:R *1..3 {prop:2} ]-()") {
    failsToParse
  }

  test("INSERT ()<-[:R {prop:2} WHERE true]-()") {
    failsToParse
  }

  test("INSERT ()<-[:R *1..3 WHERE true]-()") {
    failsToParse
  }

  test("INSERT ()-[r :R *1..3 {prop:2}]->()") {
    failsToParse
  }

  test("INSERT ()-[r :R {prop:2} WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()-[r :R *1..3 WHERE true]->()") {
    failsToParse
  }

  test("INSERT ()-[r :R *1..3 {prop:2} WHERE true]->()") {
    failsToParse
  }

  test("INSERT shortestPath((a)-[r]->(b))") {
    failsToParse
  }

  test("INSERT allShortestPaths((a)-[r]->(b))") {
    failsToParse
  }

  test("INSERT (a)-[:R]->(b)(a)") {
    failsToParse
  }

  test("INSERT ((n)-[r]->(m))*") {
    failsToParse
  }

  test("INSERT ((a)-->(b) WHERE a.prop > b.prop)") {
    failsToParse
  }

  // The following cases will parse and be semantically correct for CREATE.
  // For INSERT, they fail in parsing.

  test("INSERT (:(A&B))") {
    failsToParse
  }

  test("INSERT (IS (A&B)&C)") {
    failsToParse
  }

  test("INSERT (:A:B)") {
    assertFailsWithMessage(
      testName,
      "Colon conjunction is not allowed in INSERT. Use `CREATE` instead or conjunction with `&` instead. (line 1, column 11 (offset: 10))"
    )
  }

  test("INSERT (n:A&B:C)") {
    assertFailsWithMessage(
      testName,
      "Colon conjunction is not allowed in INSERT. Use `CREATE` instead or conjunction with `&` instead. (line 1, column 14 (offset: 13))"
    )
  }

  test("INSERT (n:A)-[:R]->(:B:C)") {
    assertFailsWithMessage(
      testName,
      "Colon conjunction is not allowed in INSERT. Use `CREATE` instead or conjunction with `&` instead. (line 1, column 23 (offset: 22))"
    )
  }

  test("INSERT p=()-[:R]->()") {
    assertFailsWithMessage(
      testName,
      "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name. (line 1, column 8 (offset: 7))"
    )
  }

  test("INSERT (), p=()-[:R]->()") {
    assertFailsWithMessage(
      testName,
      "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name. (line 1, column 12 (offset: 11))"
    )
  }

  // INSERT should not work as a synonym to CREATE for DDL

  test("INSERT USER foo SET PASSWORD 'password'") {
    failsToParse
  }

  test("INSERT ROLE role") {
    failsToParse
  }

  test("INSERT DATABASE foo") {
    failsToParse
  }

  test("INSERT COMPOSITE DATABASE name") {
    failsToParse
  }

  test("INSERT ALIAS alias FOR DATABASE foo") {
    failsToParse
  }

  test("INSERT INDEX FOR (n:Label) ON n.prop") {
    failsToParse
  }

  test("INSERT CONSTRAINT FOR (n:Label) REQUIRE n.prop IS NOT NULL") {
    failsToParse
  }
}
