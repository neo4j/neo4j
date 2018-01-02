/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package cypher.cucumber

import java.util

import gherkin.formatter.model.{Result, Background, Examples, Feature, Match, Scenario, ScenarioOutline, Step}
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
