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

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode

class EscapedSymbolicNameParserTest extends ParserSyntaxTreeBase[ParserRuleContext, ASTNode] {

  test("escaped variable name") {
    implicit val javaccRule: JavaccRule[Variable] = JavaccRule.Variable
    implicit val antlrRule: AntlrRule[Cst.Variable] = AntlrRule.Variable

    parsing("`This isn\\'t a common variable`") shouldGive varFor("This isn\\'t a common variable")
    parsing("`a``b`") shouldGive varFor("a`b")
  }

  test("escaped label name") {
    implicit val javaccRule: JavaccRule[NodePattern] = JavaccRule.NodePattern
    implicit val antlrRule: AntlrRule[Cst.NodePattern] = AntlrRule.NodePattern

    parsing("(n:`Label`)") shouldGive nodePat(Some("n"), Some(labelLeaf("Label")))
    parsing("(n:`Label``123`)") shouldGive nodePat(Some("n"), Some(labelLeaf("Label`123")))
    parsing("(n:`````Label```)") shouldGive nodePat(Some("n"), Some(labelLeaf("``Label`")))

    assertFails("(n:`L`abel`)")
  }
}
