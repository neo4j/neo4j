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

import org.neo4j.cypher.CypherITTestSuite
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.options.CypherVersionOption
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.server.security.auth.AuthProcedures

import scala.jdk.CollectionConverters.MapHasAsJava

class CypherQueryObfuscatorIT extends CypherITTestSuite {

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
        "CREATE USER ****** SET PASSWORD ******",
      s"CREATE USER test IF NOT EXISTS SET PASSWORD '$password'" ->
        "CREATE USER ****** IF NOT EXISTS SET PASSWORD ******",
      s"CREATE OR REPLACE USER test SET PASSWORD '$password'" ->
        "CREATE OR REPLACE USER ****** SET PASSWORD ******",
      s"CREATE USER test SET PASSWORD '$password' CHANGE REQUIRED" ->
        "CREATE USER ****** SET PASSWORD ****** CHANGE REQUIRED",
      s"ALTER USER test SET PASSWORD '$password'" ->
        "ALTER USER ****** SET PASSWORD ******",
      s"ALTER USER test SET PASSWORD '$password' CHANGE REQUIRED" ->
        "ALTER USER ****** SET PASSWORD ****** CHANGE REQUIRED",
      s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO '$password'" ->
        "ALTER CURRENT USER SET PASSWORD FROM ****** TO ******"
    )

    for {
      (rawText, obfuscatedText) <- literalTests
      version <- CypherVersionOption.values + CypherVersionOption.default
    } {
      val renderedVersion = if (version == CypherVersionOption.default) "" else "CYPHER " + version.render + " "
      test(s"$renderedVersion$rawText [text]") {
        obfuscatorFactory.obfuscatorForQuery(renderedVersion + rawText, CypherVersion.Legacy.legacyVersion())
          .fullyObfuscatedQuery(rawText, org.neo4j.values.virtual.MapValue.EMPTY, 0).text() should equal(obfuscatedText)
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
      "CREATE USER ****** SET PASSWORD ******",
      Map.empty,
      Map.empty
    ),
    ParameterTest(
      "CREATE USER test SET PASSWORD $param",
      "CREATE USER ****** SET PASSWORD $param",
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

  for {
    ParameterTest(rawText, obfuscatedText, rawParameters, obfuscatedParameters) <- parameterTests
    version <- CypherVersionOption.values
  } {
    val renderedVersion = if (version == CypherVersionOption.default) "" else "CYPHER " + version.render + " "
    test(s"$renderedVersion$rawText [params]") {
      val params = ValueUtils.asMapValue(rawParameters.asJava)
      val expectedParams = ValueUtils.asMapValue(obfuscatedParameters.asJava)
      val ob = obfuscatorFactory.obfuscatorForQuery(renderedVersion + rawText, CypherVersion.Legacy.legacyVersion())
      val result = ob.fullyObfuscatedQuery(rawText, params, 0)
      result.text() should equal(obfuscatedText)
      result.parameters() should equal(expectedParams)
    }
  }

  private val secretCommand = "CREATE USER alice SET PASSWORD 'secretpw'"

  test("sensitive mode redacts the password but keeps ordinary literals visible") {
    val ob = obfuscatorFactory.obfuscatorForQuery(secretCommand, CypherVersion.Legacy.legacyVersion())
    val obfuscated = ob.sensitiveObfuscatedQuery(secretCommand, org.neo4j.values.virtual.MapValue.EMPTY, 0).text()
    obfuscated should not include "secretpw" // password always redacted
    obfuscated should include("alice") // ordinary literal (username) stays visible when obfuscate_literals is off
  }

  test("full mode redacts every literal including ordinary ones") {
    val ob = obfuscatorFactory.obfuscatorForQuery(secretCommand, CypherVersion.Legacy.legacyVersion())
    val obfuscated = ob.fullyObfuscatedQuery(secretCommand, org.neo4j.values.virtual.MapValue.EMPTY, 0).text()
    obfuscated should not include "secretpw"
    obfuscated should not include "alice"
  }

  test("LOAD CSV credential URL is redacted in sensitive mode while ordinary literals stay visible") {
    val query = "LOAD CSV FROM 'ftp://mark:Password1@localhost/images.txt' AS line RETURN 'visible' AS keep"
    val ob = obfuscatorFactory.obfuscatorForQuery(query, CypherVersion.Legacy.legacyVersion())
    val obfuscated = ob.sensitiveObfuscatedQuery(query, org.neo4j.values.virtual.MapValue.EMPTY, 0).text()
    obfuscated should not include "Password1" // credential url redacted even when obfuscate_literals is off
    obfuscated should include("'visible'") // ordinary literal stays visible in sensitive mode
    val fullyObfuscatedText = ob.fullyObfuscatedQuery(query, org.neo4j.values.virtual.MapValue.EMPTY, 0).text()
    fullyObfuscatedText should not include "Password1"
    fullyObfuscatedText should not include "'visible'" // ordinary literal redacted in the all-literals view
  }
}
