/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher.feature.reporting

import java.io.{File, PrintStream}

import cypher.cucumber.CucumberAdapter
import cypher.feature.parser.reporting.{CombinationChartWriter, CoverageChartWriter}
import gherkin.formatter.model.{Match, Result, Step}
import org.opencypher.tools.tck.constants.TCKStepDefinitions

import scala.util.matching.Regex

object CypherResultReporter {

  def createPrintStream(path: File, filename: String): PrintStream = {
    path.mkdirs()
    new PrintStream(new File(path, filename))
  }
}

class CypherResultReporter(producer: OutputProducer, jsonWriter: PrintStream, chartWriter: CoverageChartWriter,
                           combinationChartWriter: CombinationChartWriter)
  extends CucumberAdapter {

  def this(reportDir: File) = {
    this(producer = JsonProducer,
         jsonWriter = CypherResultReporter.createPrintStream(reportDir, "compact.json"),
         chartWriter = new CoverageChartWriter(reportDir, "tags"),
         combinationChartWriter = new CombinationChartWriter(reportDir, "tagCombinations"))
  }

  private var query: String = null
  private var status: String = Result.PASSED
  private val execQRegex: Regex = TCKStepDefinitions.EXECUTING_QUERY.r
  private val controlQRegex = TCKStepDefinitions.EXECUTING_CONTROL_QUERY.r

  override def done(): Unit = {
    jsonWriter.println(producer.dump())
    chartWriter.dumpSVG(producer.dumpTagStats)
    chartWriter.dumpPNG(producer.dumpTagStats)
    val stats = producer.dumpTagCombinationStats
    combinationChartWriter.dumpHTML(stats._1, stats._2)
  }

  override def close(): Unit = {
    jsonWriter.flush()
    jsonWriter.close()
  }

  override def step(step: Step) {
    if (step.getKeyword.trim == "When") {
      step.getName match {
        case execQRegex() => query = step.getDocString.getValue
        case controlQRegex() => // do nothing
        case _ => throw new IllegalStateException("An illegal 'When' step was encountered: " + step.getName)
      }
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
}
