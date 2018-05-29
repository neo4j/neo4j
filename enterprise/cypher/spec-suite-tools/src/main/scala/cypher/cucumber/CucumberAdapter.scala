/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.cucumber

import java.util

import gherkin.formatter.model._
import gherkin.formatter.{Formatter, Reporter}

trait CucumberAdapter extends Formatter with Reporter {
  override def endOfScenarioLifeCycle(scenario: Scenario){}
  override def scenario(scenario: Scenario){}
  override def startOfScenarioLifeCycle(scenario: Scenario){}
  override def uri(uri: String){}
  override def done(){}
  override def background(background: Background){}
  override def scenarioOutline(scenarioOutline: ScenarioOutline){}
  override def close(){}
  override def feature(feature: Feature){}
  override def step(step: Step){}
  override def eof(){}
  override def examples(examples: Examples){}
  override def syntaxError(state: String, event: String, legalEvents: util.List[String], uri: String, line: Integer){}
  override def result(result: Result){}
  override def `match`(`match`: Match){}
  override def after(`match`: Match, result: Result){}
  override def embedding(mimeType: String, data: Array[Byte]){}
  override def write(text: String){}
  override def before(`match`: Match, result: Result){}
}
