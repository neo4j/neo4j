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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.AddTags
import org.neo4j.cypher.internal.ast.AlterUsers
import org.neo4j.cypher.internal.ast.RemoveAllTags
import org.neo4j.cypher.internal.ast.RemoveTags
import org.neo4j.cypher.internal.ast.SetTags
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5

class AlterUsersAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  test("ALTER USERS alice ADD TAGS 'x'") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq(AddTags(literalString("x"))(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice, bob ADD TAGS 'x'") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice"), literalString("bob")),
        ifExists = false,
        Seq(AddTags(literalString("x"))(pos))
      )(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice IF EXISTS ADD TAGS 'x'") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = true, Seq(AddTags(literalString("x"))(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS $user ADD TAGS 'x'") {
    assertAst(
      AlterUsers(Seq(stringParam("user")), ifExists = false, Seq(AddTags(literalString("x"))(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice ADD TAG 'x'") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq(AddTags(literalString("x"))(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice SET TAGS ['a', 'b']") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(SetTags(listOf(literalString("a"), literalString("b")))(pos))
      )(pos),
      supportedInCypher5 = false,
      obfuscator = false // Obfuscation check does not support this query
    )
  }

  test("ALTER USERS alice SET TAGS []") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(SetTags(listOf())(pos))
      )(pos),
      supportedInCypher5 = false,
      obfuscator = false // Obfuscation check does not support this query
    )
  }

  test("ALTER USERS alice ADD TAGS []") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(AddTags(listOf())(pos))
      )(pos),
      supportedInCypher5 = false,
      obfuscator = false // Obfuscation check does not support this query
    )
  }

  test("ALTER USERS alice REMOVE TAGS []") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(RemoveTags(listOf())(pos))
      )(pos),
      supportedInCypher5 = false,
      obfuscator = false // Obfuscation check does not support this query
    )
  }

  test("ALTER USERS alice SET TAGS $param") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq(SetTags(anyParam("param"))(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice REMOVE TAGS ['x']") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(RemoveTags(listOf(literalString("x")))(pos))
      )(pos),
      supportedInCypher5 = false,
      obfuscator = false // Obfuscation check does not support this query
    )
  }

  test("ALTER USERS alice ADD TAGS $param") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq(AddTags(anyParam("param"))(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice REMOVE TAGS $param") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq(RemoveTags(anyParam("param"))(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice REMOVE ALL TAGS") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq(RemoveAllTags()(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice REMOVE ALL TAG") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq(RemoveAllTags()(pos)))(pos),
      supportedInCypher5 = false
    )
  }

  // SET TAGS combined with REMOVE/ADD — parses successfully, caught by semantic check
  test("ALTER USERS alice REMOVE TAGS 'x' SET TAGS ['a']") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(RemoveTags(literalString("x"))(pos), SetTags(listOf(literalString("a")))(pos))
      )(pos),
      supportedInCypher5 = false,
      obfuscator = false // Obfuscation check does not support this query
    )
  }

  test("ALTER USERS alice ADD TAGS 'x' SET TAGS ['a']") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(AddTags(literalString("x"))(pos), SetTags(listOf(literalString("a")))(pos))
      )(pos),
      supportedInCypher5 = false,
      obfuscator = false // Obfuscation check does not support this query
    )
  }

  test("ALTER USERS alice REMOVE TAGS 'x' ADD TAGS 'y'") {
    assertAst(
      AlterUsers(
        Seq(literalString("alice")),
        ifExists = false,
        Seq(RemoveTags(literalString("x"))(pos), AddTags(literalString("y"))(pos))
      )(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice ADD TAGS 'a' ADD TAGS 'b'") {
    parsesIn[Statements] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("Duplicate ADD TAGS clause")
    }
  }

  test("ALTER USERS alice SET TAGS 'a' SET TAGS 'b'") {
    parsesIn[Statements] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("Duplicate SET TAGS clause")
    }
  }

  test("ALTER USERS alice REMOVE TAGS 'a' REMOVE TAGS 'b'") {
    parsesIn[Statements] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("Duplicate REMOVE TAGS clause")
    }
  }

  test("ALTER USERS alice REMOVE ALL TAGS REMOVE TAGS 'x'") {
    parsesIn[Statements] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("Duplicate REMOVE TAGS clause")
    }
  }

  // ALTER USERS with no clause parses successfully at the grammar level;
  // semantic check rejects it with "ALTER USERS requires at least one tag clause"
  test("ALTER USERS alice") {
    assertAst(
      AlterUsers(Seq(literalString("alice")), ifExists = false, Seq.empty)(pos),
      supportedInCypher5 = false
    )
  }

  test("ALTER USERS alice ADD TAGS 'x' REMOVE TAGS 'y'") {
    parsesIn[Statements] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("Invalid input 'REMOVE'")
    }
  }
}
