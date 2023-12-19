/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package cypher.features

class TestConfig(
  val blacklist: Option[String],
  val executionPrefix: String
)

case object DefaultTestConfig extends TestConfig(Some("default.txt"),"")

case object CostSlottedTestConfig extends TestConfig(Some("cost-slotted.txt"),"CYPHER planner=cost runtime=slotted")

case object CostCompiledTestConfig extends TestConfig(Some("cost-compiled.txt"),
  "CYPHER planner=cost runtime=compiled debug=generate_java_source")

case object CostInterpretedTestConfig extends TestConfig(Some("cost-interpreted.txt"),"CYPHER planner=cost runtime=interpreted")

case object Compatibility33TestConfig extends TestConfig(Some("compatibility-33.txt"),"CYPHER 3.3")

case object Compatibility31TestConfig extends TestConfig(Some("compatibility-31.txt"),"CYPHER 3.1")

case object Compatibility23TestConfig extends TestConfig(Some("compatibility-23.txt"),"CYPHER 2.3")
