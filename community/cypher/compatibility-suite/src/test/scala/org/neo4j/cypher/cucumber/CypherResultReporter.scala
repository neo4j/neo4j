/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.cucumber

import java.io.{File, PrintStream}
import java.net.URL
import java.util

import gherkin.formatter.model.{Background, Examples, Feature, Match, Result, Scenario, ScenarioOutline, Step}
import gherkin.formatter.{Formatter, Reporter}

import scala.util.matching.Regex

object CypherResultReporter {
  def createPrintStream(path: String, filename: String): PrintStream = {
    val pathFile = new File(path)
    pathFile.mkdirs()
    new PrintStream(new File(pathFile, filename))
  }
}

class CypherResultReporter(producer: OutputProducer, jsonWriter: PrintStream) extends Formatter with Reporter {

  def this(reportDir: URL) = {
    this(producer = JsonProducer, jsonWriter = CypherResultReporter.createPrintStream(reportDir.getFile, "compact.json") )
  }

  private var query: String = null
  private var status: String = Result.PASSED
  private val pattern: Regex = "running: (.*)".r

  override def done(): Unit = {
    jsonWriter.println(producer.dump())
  }

  override def close(): Unit = {
    jsonWriter.flush()
    jsonWriter.close()
  }

  override def step(step: Step) {
    if(step.getKeyword.trim == "When") {
      val pattern(q) = step.getName
      query = q
    }
  }

  override def result(result: Result): Unit = {
    val resultStatus = result.getStatus
    if (status == Result.PASSED) {
      status = resultStatus
    }
    else if (!status.contains(resultStatus)) {
      status = s"$status;$resultStatus"
    }
  }

  override def after(`match`: Match, result: Result): Unit = {
    producer.complete(query, Outcome.from(status))
    query = null
    status = Result.PASSED
  }

  override def endOfScenarioLifeCycle(scenario: Scenario){}
  override def scenario(scenario: Scenario){}
  override def startOfScenarioLifeCycle(scenario: Scenario){}
  override def uri(s: String) {}
  override def scenarioOutline(scenarioOutline: ScenarioOutline){}
  override def background(background: Background) {}
  override def feature(feature: Feature) {}
  override def examples(examples: Examples) {}
  override def eof() {}
  override def syntaxError(s: String, s1: String, list: util.List[String], s2: String, integer: Integer) {}
  override def `match`(`match`: Match) {}
  override def embedding(mimeType: String, data: Array[Byte]) {}
  override def write(text: String) {}
  override def before(`match`: Match, result: Result) {}
}
