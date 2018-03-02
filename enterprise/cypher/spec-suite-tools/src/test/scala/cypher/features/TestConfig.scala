/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.features

class TestConfig(
  val blacklist: Option[String],
  val executionPrefix: String
)

case object DefaultTestConfig extends TestConfig(Some("default.txt"),"")

case object CostSlottedTestConfig extends TestConfig(Some("cost-slotted.txt"),"CYPHER planner=cost runtime=slotted")

case object CostMorselTestConfig extends TestConfig(Some("cost-morsel.txt"),"CYPHER planner=cost runtime=morsel")

case object CostCompiledTestConfig extends TestConfig(Some("cost-compiled.txt"),
  "CYPHER planner=cost runtime=compiled debug=generate_java_source")

case object CostInterpretedTestConfig extends TestConfig(Some("cost-interpreted.txt"),"CYPHER planner=cost runtime=interpreted")

case object Compatibility33TestConfig extends TestConfig(Some("compatibility-33.txt"),"CYPHER 3.3")

case object Compatibility31TestConfig extends TestConfig(Some("compatibility-31.txt"),"CYPHER 3.1")

case object Compatibility23TestConfig extends TestConfig(Some("compatibility-23.txt"),"CYPHER 2.3")
