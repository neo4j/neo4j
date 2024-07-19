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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.initial_default_database
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DefaultSystemGraphComponent
import org.neo4j.dbms.database.DefaultSystemGraphInitializer
import org.neo4j.dbms.database.SystemGraphComponents
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphComponent
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLogProvider
import org.neo4j.server.security.auth.InMemoryUserRepository
import org.neo4j.server.security.systemgraph.SecurityGraphHelper
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent
import org.neo4j.storageengine.api.MetadataProvider
import org.scalatest.OptionValues
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

import java.nio.file.Path
import java.time.Clock
import java.time.ZonedDateTime

class CommunityMultiDatabaseAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase
    with OptionValues {
  private val onlineStatus = DatabaseStatus.Online.stringValue()
  private val defaultConfig = Config.defaults()
  private val accessString = "read-write"
  private val typeString = "standard"
  private val localHostString = "localhost:7687"
  private val dbDefaultMap = Map("db" -> DEFAULT_DATABASE_NAME)
  private val nameDefaultMap = Map("name" -> DEFAULT_DATABASE_NAME)
  private val nameSystemMap = Map("name" -> SYSTEM_DATABASE_NAME)

  test("should fail at startup when config setting for default database name is invalid") {
    // GIVEN
    val startOfError = "Error evaluating value for setting 'initial.dbms.default_database'. "

    // Empty name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(initial_default_database, "")
      // THEN
    } should have message startOfError + "Failed to validate '' for 'initial.dbms.default_database': The provided database name is empty."

    // Starting on invalid character
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(initial_default_database, "_default")
      // THEN
    } should have message startOfError + "Failed to validate '_default' for 'initial.dbms.default_database': Database name '_default' is not starting with an ASCII alphabetic character."

    // Has prefix 'system'
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(initial_default_database, "system-mine")
      // THEN
    } should have message startOfError + "Failed to validate 'system-mine' for 'initial.dbms.default_database': Database name 'system-mine' is invalid, due to the prefix 'system'."

    // Contains invalid characters
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(initial_default_database, "mydbwith_and%")
      // THEN
    } should have message startOfError + "Failed to validate 'mydbwith_and%' for 'initial.dbms.default_database': Database name 'mydbwith_and%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes."

    // Too short name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(initial_default_database, "me")
      // THEN
    } should have message startOfError + "Failed to validate 'me' for 'initial.dbms.default_database': The provided database name must have a length between 3 and 63 characters."

    // Too long name
    val name = "ihaveallooootoflettersclearlymorethanishould-ihaveallooootoflettersclearlymorethanishould"
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(initial_default_database, name)
      // THEN
    } should have message startOfError + "Failed to validate '" + name + "' for 'initial.dbms.default_database': The provided database name must have a length between 3 and 63 characters."
  }

  // Tests for showing databases

  test(s"should show database $DEFAULT_DATABASE_NAME") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    result.toList should be(List(db(DEFAULT_DATABASE_NAME, home = true, default = true)))
  }

  test(s"should show database $DEFAULT_DATABASE_NAME with params") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASE $db", dbDefaultMap)

    // THEN
    result.toList should be(List(db(DEFAULT_DATABASE_NAME, home = true, default = true)))
  }

  test("should give nothing when showing a non-existing database") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List.empty)

    // and an invalid (non-existing) one
    // WHEN
    val result2 = execute("SHOW DATABASE ``")

    // THEN
    result2.toList should be(List.empty)
  }

  test("should fail when showing a database when not on system database") {
    // GIVEN
    setup(defaultConfig)

    // THEN
    assertFailWhenNotOnSystem(s"SHOW DATABASE $DEFAULT_DATABASE_NAME", "SHOW DATABASE")
  }

  test("should show default databases") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, home = true, default = true),
      db(SYSTEM_DATABASE_NAME, dbType = SYSTEM_DATABASE_NAME)
    ))
  }

  test("should fail when showing databases when not on system database") {
    // GIVEN
    setup(defaultConfig)

    // THEN
    assertFailWhenNotOnSystem("SHOW DATABASES", "SHOW DATABASES")
  }

  test("should show default database") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(homeOrDefaultDb(DEFAULT_DATABASE_NAME)))
  }

  test("should show custom default database using show default database command") {
    // GIVEN
    val config = Config.defaults()
    config.set(initial_default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(homeOrDefaultDb("foo")))
  }

  test("should show correct default database for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toSet should be(Set(homeOrDefaultDb(DEFAULT_DATABASE_NAME)))

    // GIVEN
    config.set(initial_default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DEFAULT DATABASE")

    // THEN

    // Required because current acceptance test machinery doesn't actually start foo
    //   but the defaultDb row constructor assumes currentStatus -> started
    val expectedRow = homeOrDefaultDb("foo") ++ Map("currentStatus" -> "unknown")
    result2.toSet should be(Set(expectedRow))
  }

  test("should fail when showing default database when not on system database") {
    // GIVEN
    setup(defaultConfig)

    // THEN
    assertFailWhenNotOnSystem("SHOW DEFAULT DATABASE", "SHOW DEFAULT DATABASE")
  }

  test("should show default database as home database when executing as anonymous user") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW HOME DATABASE")

    // THEN
    result.toList should be(List(homeOrDefaultDb(DEFAULT_DATABASE_NAME)))
  }

  // yield / skip / limit / order by / where

  test("should show database with yield") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASE $db YIELD name, address, role", dbDefaultMap)

    // THEN
    result.toList should be(List(Map(
      "name" -> DEFAULT_DATABASE_NAME,
      "address" -> localHostString,
      "role" -> "primary"
    )))
  }

  test("should show database with yield *") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASE $db YIELD *", dbDefaultMap).toList.head

    // THEN
    val db = managementService.database(DEFAULT_DATABASE_NAME).asInstanceOf[GraphDatabaseAPI]
    val format =
      db.getDependencyResolver.resolveDependency(classOf[MetadataProvider]).getStoreId.getStoreVersionUserString
    result should have size 26
    result should contain.allOf(
      "name" -> DEFAULT_DATABASE_NAME,
      "type" -> "standard",
      "access" -> "read-write",
      "aliases" -> Seq(),
      "address" -> localHostString,
      "role" -> "primary",
      "writer" -> true,
      "requestedStatus" -> "online",
      "currentStatus" -> "online",
      "currentPrimariesCount" -> 1,
      "currentSecondariesCount" -> 0,
      "requestedPrimariesCount" -> null,
      "requestedSecondariesCount" -> null,
      "store" -> format,
      "lastCommittedTxn" -> null,
      "replicationLag" -> 0,
      "constituents" -> Seq.empty,
      "options" -> Map()
    )
  }

  test("should show database with yield and where") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result =
      execute(s"SHOW DATABASE $$db YIELD name, address, role WHERE name = '$DEFAULT_DATABASE_NAME'", dbDefaultMap)

    // THEN
    result.toList should be(List(Map(
      "name" -> DEFAULT_DATABASE_NAME,
      "address" -> localHostString,
      "role" -> "primary"
    )))
  }

  test("should show databases with yield and where") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute(s"SHOW DATABASES YIELD name, address, role WHERE name = '$DEFAULT_DATABASE_NAME'")

    // THEN
    result.toList should be(List(Map(
      "name" -> DEFAULT_DATABASE_NAME,
      "address" -> localHostString,
      "role" -> "primary"
    )))
  }

  test("should show databases with yield and skip") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name SKIP 1")

    // THEN
    result.toList should be(List(nameSystemMap))
  }

  test("should show databases with yield and limit") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name LIMIT 1")

    // THEN
    result.toList should be(List(nameDefaultMap))
  }

  test("should show databases with yield and order by asc") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name ASC")

    // THEN
    result.toList should be(List(nameDefaultMap, nameSystemMap))
  }

  test("should show databases with yield and order by desc") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name DESC")

    // THEN
    result.toList should be(List(nameSystemMap, nameDefaultMap))
  }

  test("should show databases with yield and return") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD name RETURN name")

    // THEN
    result.toSet should be(Set(nameSystemMap, nameDefaultMap))
  }

  test("should count default database with yield and return") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE YIELD name RETURN count(name) as count, name")

    // THEN
    result.toSet should be(Set(Map[String, Any]("count" -> 1, "name" -> DEFAULT_DATABASE_NAME)))
  }

  test("should show databases with yield, return and skip") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD * RETURN name ORDER BY name SKIP 1")

    // THEN
    result.toList should be(List(nameSystemMap))
  }

  test("should show databases with yield, return and limit") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD * RETURN name ORDER BY name LIMIT 1")

    // THEN
    result.toList should be(List(nameDefaultMap))
  }

  test(s"should show database $DEFAULT_DATABASE_NAME with yield and aliasing") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result =
      execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME YIELD name AS foo WHERE foo = '$DEFAULT_DATABASE_NAME' RETURN foo")

    // THEN
    result.toList should be(List(Map("foo" -> DEFAULT_DATABASE_NAME)))
  }

  test("should show default database with all verbose columns") {
    // GIVEN
    setup(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    val dbId = execute(s"CALL db.info() YIELD id").toList.head("id")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    val result = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME YIELD *").toList.head

    // THEN
    result should contain allElementsOf homeOrDefaultDb(DEFAULT_DATABASE_NAME)
    result("replicationLag") shouldBe 0
    result("lastCommittedTxn") shouldBe null
    result("serverID") should beAValidUUID()
    result("databaseID") shouldBe dbId
    result("creationTime") shouldBe a[ZonedDateTime]
    (ZonedDateTime.now().toEpochSecond - result("creationTime").asInstanceOf[
      ZonedDateTime
    ].toEpochSecond) should be < 300L
    result("lastStartTime") shouldBe a[ZonedDateTime]
    (ZonedDateTime.now().toEpochSecond - result("lastStartTime").asInstanceOf[
      ZonedDateTime
    ].toEpochSecond) should be < 300L
    result("lastStopTime") shouldBe null
    result("currentPrimariesCount") shouldBe 1
    result("currentSecondariesCount") shouldBe 0
    result("requestedPrimariesCount") shouldBe null
    result("requestedSecondariesCount") shouldBe null
  }

  test("should show database with yield verbose columns should produce verbose but not polled columns") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD name, databaseID, serverID")

    // THEN
    result.toList.foreach { map =>
      map should have size 3
      map.get("name") should contain.oneOf("neo4j", "system")

      // Lookup the real store id from db.info()
      selectDatabase(map("name").asInstanceOf[String])
      val dbId = execute(s"CALL db.info() YIELD id").toList.head("id")

      map.get("databaseID").value shouldBe dbId
      map.get("serverID").value should beAValidUUID()
    }
  }

  test("should show database and yield only verbose columns") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES YIELD lastCommittedTxn, serverID").toList

    // THEN
    result.foreach { map =>
      map should have size 2
      map.get("serverID").value should beAValidUUID()
      map.get("lastCommittedTxn").value shouldBe null
    }
  }

  test("should show default database with yield and return with aliasing") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute(s"SHOW DEFAULT DATABASE YIELD name WHERE name = '$DEFAULT_DATABASE_NAME' RETURN name as foo")

    // THEN
    result.toList should be(List(Map("foo" -> DEFAULT_DATABASE_NAME)))
  }

  test("should show default database as home database with YIELD") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW HOME DATABASE YIELD name")

    // THEN
    result.toList should be(List(Map("name" -> DEFAULT_DATABASE_NAME)))
  }

  test("should not show database with invalid yield") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASE $db YIELD foo, bar, baz", dbDefaultMap)
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 25 (offset: 24))")
  }

  test("should not show database with invalid where") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASE $db WHERE foo = 'bar'", dbDefaultMap)
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 25 (offset: 24))")
  }

  test("should not show database with yield and invalid where") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASE $db YIELD name, address, role WHERE foo = 'bar'", dbDefaultMap)
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 51 (offset: 50))")
  }

  test("should not show databases with yield and invalid skip") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASES YIELD name ORDER BY name SKIP -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 46 (offset: 45))")
  }

  test("should not show databases with yield and invalid limit") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASES YIELD name ORDER BY name LIMIT -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 47 (offset: 46))")
  }

  test("should not show default database with invalid order by") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val exceptionBar = the[SyntaxException] thrownBy {
      execute("SHOW DEFAULT DATABASE YIELD name ORDER BY bar")
    }

    // THEN
    exceptionBar.getMessage should startWith("Variable `bar` not defined")
    exceptionBar.getMessage should include("(line 1, column 43 (offset: 42))")

    // WHEN
    val exceptionRole = the[SyntaxException] thrownBy {
      // 'role' is a valid column but not yielded
      execute("SHOW DEFAULT DATABASE YIELD name ORDER BY role")
    }

    // THEN
    exceptionRole.getMessage should startWith("Variable `role` not defined")
    exceptionRole.getMessage should include("(line 1, column 43 (offset: 42))")
  }

  // Test for non-valid community commands

  test("should fail on creating database from community") {
    // GIVEN
    setup(defaultConfig)

    // THEN

    assertFailure("CREATE DATABASE foo", "Unsupported administration command: CREATE DATABASE foo")

    assertFailure("CREATE DATABASE $foo", "Unsupported administration command: CREATE DATABASE $foo")

    assertFailure(
      s"CREATE DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: CREATE DATABASE $DEFAULT_DATABASE_NAME"
    )

    assertFailure(
      s"CREATE DATABASE $DEFAULT_DATABASE_NAME IF NOT EXISTS",
      s"Unsupported administration command: CREATE DATABASE $DEFAULT_DATABASE_NAME IF NOT EXISTS"
    )

    assertFailure(
      s"CREATE OR REPLACE DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: CREATE OR REPLACE DATABASE $DEFAULT_DATABASE_NAME"
    )

    assertFailure(
      s"CREATE DATABASE $DEFAULT_DATABASE_NAME OPTIONS {existingData: 'use', existingDataSeedInstance: '1'}",
      s"Unsupported administration command: CREATE DATABASE $DEFAULT_DATABASE_NAME OPTIONS {existingData: 'use', existingDataSeedInstance: '1'}"
    )
  }

  test("should fail on dropping database from community") {
    // GIVEN
    setup(defaultConfig)

    // THEN

    assertFailure("DROP DATABASE foo", "Unsupported administration command: DROP DATABASE foo")

    assertFailure("DROP DATABASE $foo", "Unsupported administration command: DROP DATABASE $foo")

    assertFailure(
      s"DROP DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: DROP DATABASE $DEFAULT_DATABASE_NAME"
    )

    assertFailure(
      s"DROP DATABASE $DEFAULT_DATABASE_NAME IF EXISTS",
      s"Unsupported administration command: DROP DATABASE $DEFAULT_DATABASE_NAME IF EXISTS"
    )
  }

  test("should fail on altering database from community") {
    // GIVEN
    setup(defaultConfig)

    // THEN
    assertFailure(
      "ALTER DATABASE foo SET ACCESS READ ONLY",
      "Unsupported administration command: ALTER DATABASE foo SET ACCESS READ ONLY"
    )

    assertFailure(
      s"ALTER DATABASE $DEFAULT_DATABASE_NAME SET ACCESS READ WRITE",
      s"Unsupported administration command: ALTER DATABASE $DEFAULT_DATABASE_NAME SET ACCESS READ WRITE"
    )
  }

  test("should fail on starting database from community") {
    // GIVEN
    setup(defaultConfig)

    // THEN

    assertFailure("START DATABASE foo", "Unsupported administration command: START DATABASE foo")

    assertFailure("START DATABASE $foo", "Unsupported administration command: START DATABASE $foo")

    assertFailure(
      s"START DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: START DATABASE $DEFAULT_DATABASE_NAME"
    )
  }

  test("should fail on stopping database from community") {
    // GIVEN
    setup(defaultConfig)

    // THEN

    assertFailure("STOP DATABASE foo", "Unsupported administration command: STOP DATABASE foo")

    assertFailure("STOP DATABASE $foo", "Unsupported administration command: STOP DATABASE $foo")

    assertFailure(
      s"STOP DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: STOP DATABASE $DEFAULT_DATABASE_NAME"
    )
  }

  test("should fail on creating composite database from community") {
    // GIVEN
    setup(defaultConfig)

    // THEN

    assertFailure("CREATE COMPOSITE DATABASE foo", "Unsupported administration command: CREATE COMPOSITE DATABASE foo")

    assertFailure(
      "CREATE COMPOSITE DATABASE $foo",
      "Unsupported administration command: CREATE COMPOSITE DATABASE $foo"
    )

    assertFailure(
      s"CREATE COMPOSITE DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: CREATE COMPOSITE DATABASE $DEFAULT_DATABASE_NAME"
    )

    assertFailure(
      s"CREATE COMPOSITE DATABASE $DEFAULT_DATABASE_NAME IF NOT EXISTS",
      s"Unsupported administration command: CREATE COMPOSITE DATABASE $DEFAULT_DATABASE_NAME IF NOT EXISTS"
    )

    assertFailure(
      s"CREATE OR REPLACE COMPOSITE DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: CREATE OR REPLACE COMPOSITE DATABASE $DEFAULT_DATABASE_NAME"
    )
  }

  test("should fail on dropping composite database from community") {
    // GIVEN
    setup(defaultConfig)

    // THEN

    assertFailure("DROP COMPOSITE DATABASE foo", "Unsupported administration command: DROP COMPOSITE DATABASE foo")

    assertFailure("DROP COMPOSITE DATABASE $foo", "Unsupported administration command: DROP COMPOSITE DATABASE $foo")

    assertFailure(
      s"DROP COMPOSITE DATABASE $DEFAULT_DATABASE_NAME",
      s"Unsupported administration command: DROP COMPOSITE DATABASE $DEFAULT_DATABASE_NAME"
    )

    assertFailure(
      s"DROP COMPOSITE DATABASE foo IF EXISTS",
      s"Unsupported administration command: DROP COMPOSITE DATABASE foo IF EXISTS"
    )
  }

  // Helper methods

  private def db(
    name: String,
    dbType: String = typeString,
    home: Boolean = false,
    default: Boolean = false
  ): Map[String, Any] =
    Map(
      "name" -> name,
      "type" -> dbType,
      "aliases" -> Seq.empty,
      "access" -> accessString,
      "address" -> localHostString,
      "role" -> "primary",
      "writer" -> true,
      "requestedStatus" -> onlineStatus,
      "currentStatus" -> onlineStatus,
      "statusMessage" -> "",
      "default" -> default,
      "home" -> home,
      "constituents" -> List()
    )

  private def homeOrDefaultDb(name: String): Map[String, Any] =
    Map(
      "name" -> name,
      "type" -> typeString,
      "aliases" -> Seq.empty,
      "access" -> accessString,
      "address" -> localHostString,
      "role" -> "primary",
      "writer" -> true,
      "requestedStatus" -> onlineStatus,
      "currentStatus" -> onlineStatus,
      "statusMessage" -> "",
      "constituents" -> List()
    )

  // Disable normal database creation because we need different settings on each test
  override protected def beforeEach(): Unit = {}

  private def setup(config: Config): Unit = {
    managementService = graphDatabaseFactory(Path.of("test")).impermanent().setConfig(
      Config.newBuilder().fromConfig(config).build()
    ).setInternalLogProvider(logProvider).build()
    graphOps = managementService.database(SYSTEM_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)

    initSystemGraph(config)
  }

  private def initSystemGraph(config: Config): Unit = {
    val systemGraphComponentsBuilder = new SystemGraphComponents.DefaultBuilder()
    systemGraphComponentsBuilder.register(new DefaultSystemGraphComponent(config, Clock.systemUTC))
    systemGraphComponentsBuilder.register(new CommunityTopologyGraphComponent(config, NullLogProvider.getInstance()))
    systemGraphComponentsBuilder.register(new UserSecurityGraphComponent(
      new InMemoryUserRepository,
      config,
      NullLogProvider.getInstance(),
      CommunitySecurityLog.NULL_LOG
    ))

    val databaseContextProvider =
      graph.getDependencyResolver.resolveDependency(classOf[DatabaseContextProvider[DatabaseContext]])
    val systemSupplier = SecurityGraphHelper.makeSystemSupplier(databaseContextProvider)
    val systemGraphInitializer = new DefaultSystemGraphInitializer(
      systemSupplier,
      systemGraphComponentsBuilder.build()
    )
    systemGraphInitializer.start()

    selectDatabase(SYSTEM_DATABASE_NAME)
  }
}
