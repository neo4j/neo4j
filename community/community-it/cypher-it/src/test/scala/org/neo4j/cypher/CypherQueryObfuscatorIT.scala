/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.server.security.auth.AuthProcedures

import scala.jdk.CollectionConverters.MapHasAsJava

class CypherQueryObfuscatorIT extends CypherFunSuite {

  private val obfuscatorFactory = new CypherQueryObfuscatorFactory {
    // required by procedure compiler
    registerComponent(classOf[SecurityContext])
    registerComponent(classOf[Transaction])
    registerComponent(classOf[GraphDatabaseAPI])

    registerProcedure(classOf[AuthProcedures])
  }

  private val passwords = Seq(
    "password",
    "password with \\'quotes\\'",
    "password with\\nnewline"
  )

  for (password <- passwords) {
    val literalTests: Seq[(String, String)] = Seq(
      s"CREATE USER test SET PASSWORD '$password'" ->
        "CREATE USER test SET PASSWORD ******",
      s"CREATE USER test IF NOT EXISTS SET PASSWORD '$password'" ->
        "CREATE USER test IF NOT EXISTS SET PASSWORD ******",
      s"CREATE OR REPLACE USER test SET PASSWORD '$password'" ->
        "CREATE OR REPLACE USER test SET PASSWORD ******",
      s"CREATE USER test SET PASSWORD '$password' CHANGE REQUIRED" ->
        "CREATE USER test SET PASSWORD ****** CHANGE REQUIRED",
      s"ALTER USER test SET PASSWORD '$password'" ->
        "ALTER USER test SET PASSWORD ******",
      s"ALTER USER test SET PASSWORD '$password' CHANGE REQUIRED" ->
        "ALTER USER test SET PASSWORD ****** CHANGE REQUIRED",
      s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO '$password'" ->
        "ALTER CURRENT USER SET PASSWORD FROM ****** TO ******"
    )

    for ((rawText, obfuscatedText) <- literalTests) {
      test(s"$rawText [text]") {
        val ob = obfuscatorFactory.obfuscatorForQuery(rawText)
        ob.obfuscateText(rawText, 0) should equal(obfuscatedText)
      }
    }
  }

  private case class ParameterTest(
    rawText: String,
    obfuscatedText: String,
    rawParameters: Map[String, String],
    obfuscatedParameters: Map[String, String]
  )

  private val parameterTests: Seq[ParameterTest] = Seq(
    ParameterTest(
      "CREATE USER test SET PASSWORD 'password'",
      "CREATE USER test SET PASSWORD ******",
      Map.empty,
      Map.empty
    ),
    ParameterTest(
      "CREATE USER test SET PASSWORD $param",
      "CREATE USER test SET PASSWORD $param",
      Map("param" -> "test"),
      Map("param" -> "******")
    ),
    ParameterTest(
      "ALTER CURRENT USER SET PASSWORD FROM 'test' TO $param",
      "ALTER CURRENT USER SET PASSWORD FROM ****** TO $param",
      Map("param" -> "test"),
      Map("param" -> "******")
    ),
    ParameterTest(
      "ALTER CURRENT USER SET PASSWORD FROM $old TO $new",
      "ALTER CURRENT USER SET PASSWORD FROM $old TO $new",
      Map("old" -> "a", "new" -> "b"),
      Map("old" -> "******", "new" -> "******")
    ),
    ParameterTest(
      "ALTER CURRENT USER SET PASSWORD FROM $old TO 'password'",
      "ALTER CURRENT USER SET PASSWORD FROM $old TO ******",
      Map("old" -> "a", "new" -> "b"),
      Map("old" -> "******", "new" -> "b")
    )
  )

  for (ParameterTest(rawText, obfuscatedText, rawParameters, obfuscatedParameters) <- parameterTests) {
    test(s"$rawText [params]") {
      val params = ValueUtils.asMapValue(rawParameters.asJava)
      val expectedParams = ValueUtils.asMapValue(obfuscatedParameters.asJava)
      val ob = obfuscatorFactory.obfuscatorForQuery(rawText)
      ob.obfuscateText(rawText, 0) should equal(obfuscatedText)
      ob.obfuscateParameters(params) should equal(expectedParams)
    }
  }
}
