package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.helpers.Platforms

class StringHelperTest extends CypherFunSuite {

  import StringHelper._

  test("should not fix position when the text contains no line break") {
    val text = "(line 1, column 8 (offset: 7))"

    text.fixPosition should equal("(line 1, column 8 (offset: 7))")
  }

  test("should fix positions on Windows after line breaks") {
    val text = "(line 3, column 8 (offset: 7))"

    if (Platforms.platformIsWindows()) {
      text.fixPosition should equal("(line 3, column 8 (offset: 9))")
    } else {
      text.fixPosition should equal("(line 1, column 8 (offset: 7))")
    }
  }
}
