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
package org.neo4j.cypher.internal.frontend.local_callables

import org.neo4j.gqlstatus.GqlHelper

class LocalCallablesAndCITSemanticAnalysisTest extends LocalCallablesSemanticAnalysisTest {

  test(
    """DEFINE FUNCTION foo.one() = 1
      |
      |CALL {
      |  RETURN foo.one() AS x
      |}
      |RETURN x
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """CALL {
      |  CREATE (:A)
      |} IN TRANSACTIONS
      |RETURN 1 AS x
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """{
      |  DEFINE PROCEDURE foo.bar() {
      |    RETURN 1 AS x
      |  }
      |
      |  CALL foo.bar() YIELD x
      |  RETURN x
      |}
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """DEFINE FUNCTION foo.one() = 1
      |
      |CALL {
      |^
      |  CREATE (:A)
      |} IN TRANSACTIONS
      |RETURN foo.one() AS x
      |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }

  test(
    """DEFINE PROCEDURE foo.p() {
      |  RETURN 1 AS x
      |}
      |
      |CALL {
      |  CALL {
      |  ^
      |    CREATE (:A)
      |  } IN TRANSACTIONS
      |  RETURN 1 AS x
      |}
      |RETURN x
      |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }

  test(
    """CALL () {
      |  DEFINE FUNCTION foo.inner() = 1
      |  RETURN foo.inner() AS x
      |}
      |WITH x
      |CALL {
      |^
      |  CREATE (:A)
      |} IN TRANSACTIONS
      |RETURN x
      |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }

  test(
    """CALL {
      |^
      |  DEFINE FUNCTION foo.inner() = 1
      |  RETURN foo.inner() AS x
      |} IN TRANSACTIONS
      |RETURN 1 AS x
      |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }

  test(
    """{
      |  DEFINE FUNCTION foo.one() = 1
      |  RETURN foo.one() AS x
      |}
      |
      |NEXT
      |
      |CALL {
      |^
      |  CREATE (:A)
      |} IN TRANSACTIONS
      |RETURN 2 AS y
      |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }

  test(
    """WHEN 1 = 1 THEN {
      |  DEFINE FUNCTION foo.one() = 1
      |  RETURN foo.one() AS x
      |}
      |ELSE {
      |  CALL {
      |  ^
      |    CREATE (:A)
      |  } IN TRANSACTIONS
      |  RETURN 2 AS x
      |}
      |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }

  test(
    """{
      |  DEFINE FUNCTION foo.one() = 1
      |  RETURN foo.one() AS x
      |}
      |UNION
      |CALL {
      |^
      |  CREATE (:A)
      |} IN TRANSACTIONS
      |RETURN 2 AS x
      |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }

  test(
    """{
      |  CALL () {
      |    DEFINE FUNCTION foo.one() = 1
      |    RETURN foo.one() AS x
      |  }
      |  CALL {
      |  ^
      |    CREATE (:A)
      |  } IN TRANSACTIONS
      |  RETURN x
      |}
      |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos => GqlHelper.getGql42001_42NAO(pos.offset, pos.line, pos.column),
      msg42NAO()
    )
  }
}
