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
package org.neo4j.cypher.cucumber.synthesise

import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.synthesise.generator.AddIndex
import org.neo4j.cypher.cucumber.synthesise.generator.CachingParser
import org.neo4j.cypher.cucumber.synthesise.generator.CombineUncommitted
import org.neo4j.cypher.cucumber.synthesise.generator.CompositeWrap
import org.neo4j.cypher.cucumber.synthesise.generator.Namespacing
import org.neo4j.cypher.cucumber.synthesise.generator.ObfuscateExplain
import org.neo4j.cypher.cucumber.synthesise.generator.Paginate
import org.neo4j.cypher.cucumber.synthesise.generator.ScenarioGenerator
import org.neo4j.cypher.cucumber.synthesise.generator.Uncommitted
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.test.RandomSupport

import java.nio.file.Path

object CucumberSalad {

  /** Transform scenarios to run in an open transaction. */
  def uncommitted(args: Ingredients): ScenarioGenerator = new Uncommitted(args)

  /** Combine scenarios and run in an open transaction. */
  def combinedUncommitted(args: Ingredients): ScenarioGenerator = new CombineUncommitted(args)

  /** Add indexes to existing scenarios without index. */
  def addIndex(args: Ingredients): ScenarioGenerator = new AddIndex(args)

  /** Create pagination queries (SKIP + LIMIT) based on existing scenarios. */
  def pagination(args: Ingredients): ScenarioGenerator = new Paginate(args)

  /** Wrap queries with `USE comp.data` so they run through a composite database's remote fragment. */
  def compositeWrap(args: Ingredients): ScenarioGenerator = new CompositeWrap(args)

  /** Wrap the query under test so every variable also lives in a second scope, stressing the Namespacer. */
  def namespacing(args: Ingredients): ScenarioGenerator = new Namespacing(args)

  /**
   * Rewrite test queries to EXPLAIN over their typed-obfuscated text, so obfuscated query-log
   * output must stay plannable.
   */
  def obfuscateExplain(newRenderer: () => QueryObfuscator.ObfuscatedLiteralRenderer)(
    args: Ingredients
  ): ScenarioGenerator = new ObfuscateExplain(args, newRenderer)

  case class Ingredients(
    source: Seq[RecordedScenario],
    rand: RandomSupport,
    exportDirectory: Path,
    targetConf: TestConf
  ) {

    lazy val cypherVersion: CypherVersion = CypherVersion.values()
      .find(version => targetConf.expectFailureTags.contains(s"@fails:cypher-${version.versionName}"))
      .getOrElse(CypherVersion.Legacy.legacyVersion())
    lazy val parser = new CachingParser(cypherVersion)
  }
}
