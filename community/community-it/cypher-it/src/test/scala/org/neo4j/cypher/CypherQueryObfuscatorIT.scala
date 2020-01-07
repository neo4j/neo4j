/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.cypher.internal.CypherQueryObfuscator
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.phases.{PlannerContext, RewriteProcedureCalls}
import org.neo4j.cypher.internal.logical.plans.{ProcedureSignature, QualifiedName}
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherProcedureSignature
import org.neo4j.cypher.internal.v4_0.frontend.phases.{CompilationPhaseTracer, InitialState, ObfuscationMetadataCollection, Parsing}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.procs.{QualifiedName => Neo4jQualifiedName}
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.impl.GlobalProceduresRegistry
import org.neo4j.server.security.auth.AuthProcedures

class CypherQueryObfuscatorIT extends CypherFunSuite {

  private val passwords = Seq(
    "password",
    "password with \\'quotes\\'",
    "password with\\nnewline",
  )

  for (password <- passwords) {
    val literalTests: Seq[(String, String)] = Seq(
      s"CREATE USER test SET PASSWORD '$password'" ->
        "CREATE USER test SET PASSWORD '******'",
      s"CREATE USER test IF NOT EXISTS SET PASSWORD '$password'" ->
        "CREATE USER test IF NOT EXISTS SET PASSWORD '******'",
      s"CREATE OR REPLACE USER test SET PASSWORD '$password'" ->
        "CREATE OR REPLACE USER test SET PASSWORD '******'",
      s"CREATE USER test SET PASSWORD '$password' CHANGE REQUIRED" ->
        "CREATE USER test SET PASSWORD '******' CHANGE REQUIRED",

      s"ALTER USER test SET PASSWORD '$password'" ->
        "ALTER USER test SET PASSWORD '******'",
      s"ALTER USER test SET PASSWORD '$password' CHANGE REQUIRED" ->
        "ALTER USER test SET PASSWORD '******' CHANGE REQUIRED",

      s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO '$password'" ->
        "ALTER CURRENT USER SET PASSWORD FROM '******' TO '******'",

      s"CALL dbms.security.createUser('user', '$password')" ->
        s"CALL dbms.security.createUser('user', '******')",
      s"CALL dbms.security.createUser('user', '$password', true)" ->
        s"CALL dbms.security.createUser('user', '******', true)",
      s"CALL dbms.security.changePassword('$password')" ->
        s"CALL dbms.security.changePassword('******')",
    )

    for ((rawText, obfuscatedText) <- literalTests) {
      test(s"$rawText [text]") {
        val ob = obfuscatorForQuery(rawText)
        ob.obfuscateText(rawText) should equal(obfuscatedText)
      }
    }
  }

  private case class ParameterTest(rawText: String, obfuscatedText: String, rawParameters: Map[String, String], obfuscatedParameters: Map[String, String])

  private val parameterTests: Seq[ParameterTest] = Seq(
    ParameterTest(
      "CREATE USER test SET PASSWORD 'password'",
      "CREATE USER test SET PASSWORD '******'",
      Map.empty,
      Map.empty,
    ),
    ParameterTest(
      "CREATE USER test SET PASSWORD $param",
      "CREATE USER test SET PASSWORD $param",
      Map("param" -> "test"),
      Map("param" -> "******"),
    ),
    ParameterTest(
      "ALTER CURRENT USER SET PASSWORD FROM 'test' TO $param",
      "ALTER CURRENT USER SET PASSWORD FROM '******' TO $param",
      Map("param" -> "test"),
      Map("param" -> "******"),
    ),
    ParameterTest(
      "ALTER CURRENT USER SET PASSWORD FROM $old TO $new",
      "ALTER CURRENT USER SET PASSWORD FROM $old TO $new",
      Map("old" -> "a", "new" -> "b"),
      Map("old" -> "******", "new" -> "******"),
    ),
    ParameterTest(
      "ALTER CURRENT USER SET PASSWORD FROM $old TO 'password'",
      "ALTER CURRENT USER SET PASSWORD FROM $old TO '******'",
      Map("old" -> "a", "new" -> "b"),
      Map("old" -> "******", "new" -> "b"),
    ),
    ParameterTest(
      "CALL dbms.security.createUser($user, 'password')",
      "CALL dbms.security.createUser($user, '******')",
      Map("user" -> "a", "unused" -> "test"),
      Map("user" -> "a", "unused" -> "test"),
    ),
    ParameterTest(
      "CALL dbms.security.createUser($user, $password)",
      "CALL dbms.security.createUser($user, $password)",
      Map("user" -> "a", "password" -> "b"),
      Map("user" -> "a", "password" -> "******"),
    ),
  )

  for (ParameterTest(rawText, obfuscatedText, rawParameters, obfuscatedParameters) <- parameterTests) {
    test(s"$rawText [params]") {
      import scala.collection.JavaConverters._
      val params = ValueUtils.asMapValue(rawParameters.asJava)
      val expectedParams = ValueUtils.asMapValue(obfuscatedParameters.asJava)
      val ob = obfuscatorForQuery(rawText)
      ob.obfuscateText(rawText) should equal(obfuscatedText)
      ob.obfuscateParameters(params) should equal(expectedParams)
    }
  }

  private def obfuscatorForQuery(query: String): QueryObfuscator = {
    val res = pipeline.transform(InitialState(query, None, null), plannerContext(query))
    CypherQueryObfuscator(res.obfuscationMetadata())
  }

  private val pipeline =
    Parsing andThen
      RewriteProcedureCalls andThen
      ObfuscationMetadataCollection

  private def plannerContext(query: String) =
    new PlannerContext(
      Neo4jCypherExceptionFactory(query, None),
      CompilationPhaseTracer.NO_TRACING,
      null,
      PlanContextWithProceduresRegistry,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null)

  private object PlanContextWithProceduresRegistry extends PlanContext {

    private val procedures = new GlobalProceduresRegistry()

    // required by procedure compiler
    registerComponent(classOf[SecurityContext])
    registerComponent(classOf[Transaction])
    registerComponent(classOf[GraphDatabaseAPI])

    procedures.registerProcedure(classOf[AuthProcedures])

    override def procedureSignature(name: QualifiedName): ProcedureSignature = {
      val handle = procedures.procedure(new Neo4jQualifiedName(name.namespace.toArray, name.name) )
      asCypherProcedureSignature(name, handle.id(), handle.signature())
    }

    private def registerComponent[T](cls: Class[T]): Unit =
      procedures.registerComponent(cls, _ => cls.cast(null), true)

    // unused

    override def indexesGetForLabel(labelId: Int): Nothing = fail()
    override def indexExistsForLabel(labelId: Int): Nothing = fail()
    override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Nothing = fail()
    override def uniqueIndexesGetForLabel(labelId: Int): Nothing = fail()
    override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Nothing = fail()
    override def getPropertiesWithExistenceConstraint(labelName: String): Nothing = fail()
    override def txIdProvider: Nothing = fail()
    override def statistics: Nothing = fail()
    override def notificationLogger(): Nothing = fail()
    override def functionSignature(name: QualifiedName): Nothing = fail()
    override def getLabelName(id: Int): Nothing = fail()
    override def getOptLabelId(labelName: String): Nothing = fail()
    override def getLabelId(labelName: String): Nothing = fail()
    override def getPropertyKeyName(id: Int): Nothing = fail()
    override def getOptPropertyKeyId(propertyKeyName: String): Nothing = fail()
    override def getPropertyKeyId(propertyKeyName: String): Nothing = fail()
    override def getRelTypeName(id: Int): Nothing = fail()
    override def getOptRelTypeId(relType: String): Nothing = fail()
    override def getRelTypeId(relType: String): Nothing = fail()

    private def fail() = throw new IllegalStateException("Should not have been called in this test.")
  }
}
