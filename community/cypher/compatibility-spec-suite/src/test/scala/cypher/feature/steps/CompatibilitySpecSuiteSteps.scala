/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package cypher.feature.steps

import cypher.cucumber.db.{GraphArchiveLibrary, GraphFileRepository}
import cypher.{CompatibilitySpecSuiteTest, CompatibilitySpecSuiteResources}

import scala.reflect.io.Path

class CompatibilitySpecSuiteSteps extends SpecSuiteSteps {

  override val requiredScenarioName: String = CompatibilitySpecSuiteTest.SCENARIO_NAME_REQUIRED.trim.toLowerCase

  override val unsupportedScenarios = Set("Fail when adding new label predicate on already bound node 5",
    "Fail when trying to compare strings and numbers",
    "Handling property access on the Any type",
    "Failing when performing property access on a non-map 1",
    "`toInt()` handling Any type",
    "`toInt()` failing on invalid arguments",
    "`toFloat()` handling Any type",
    "`toFloat()` failing on invalid arguments",
    "`type()` handling Any type",
    "`type()` failing on invalid arguments",
    "Concatenating lists of different type",
    "Appending to a list of different type",
    "Matching relationships into a list and matching variable length using the list", // only broken in rule planner TODO: separate this list between configurations
    "Aggregation with `min()`",
    // TODO: Align TCK w Cypher changes
    "Comparing nodes to properties",
    "Fail when comparing nodes to parameters",
    "Fail when comparing parameters to nodes",
    "Fail when comparing relationships to nodes",
    "Generate the movie graph correctly", // quoting issue in string
    "Fail when using property access on primitive type", // change error detail
    "Fail when comparing nodes to relationships",
    "Many CREATE clauses", // stack overflow
    "Null-setting one property with ON CREATE", // wrong output format (?)
    "Copying properties from node with ON CREATE", // wrong output format (?)
    "Copying properties from node with ON MATCH", // wrong output format (?)
    "Copying properties from literal map with ON CREATE", // wrong output format (?)
    "Copying properties from literal map with ON MATCH" // wrong output format (?)
  )

  override val graphArchiveLibrary = new GraphArchiveLibrary(new GraphFileRepository(Path(CompatibilitySpecSuiteResources.targetDirectory("graphs"))))
}


