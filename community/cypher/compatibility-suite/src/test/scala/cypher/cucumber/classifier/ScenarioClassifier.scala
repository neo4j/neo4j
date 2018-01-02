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
package cypher.cucumber.classifier

import cucumber.api.DataTable
import cucumber.runtime.io.MultiLoader
import cucumber.runtime.model.CucumberScenario
import cucumber.runtime.scala.ScalaBackend
import cucumber.runtime.table.TableConverter
import cucumber.runtime.xstream.LocalizedXStreams
import cucumber.runtime.{RuntimeGlue, RuntimeOptionsFactory, UndefinedStepsTracker}
import cypher.cucumber.DataTableConverter._
import gherkin.formatter.model.DocString
import org.neo4j.cypher.internal.compiler.v2_3.ast.QueryTagger

import scala.collection.JavaConverters._

class ScenarioClassifier {

  import cypher.GlueSteps._

  def classify(clazz: Class[_]): Seq[Scenario] = {
    // initialize options
    val options = new RuntimeOptionsFactory(clazz).create()
    val loader = new MultiLoader(clazz.getClassLoader)
    val streams = new LocalizedXStreams(clazz.getClassLoader)
    // create glue
    val glue = new RuntimeGlue(new UndefinedStepsTracker, streams)
    new ScalaBackend(loader).loadGlue(glue, options.getGlue)

    options.cucumberFeatures(loader).asScala.flatMap { feature =>
      val i18n = feature.getI18n
      val featurePath = feature.getPath
      val featureName = feature.getGherkinFeature.getName
      feature.getFeatureElements.asScala.map { element =>
        val scenario = element.asInstanceOf[CucumberScenario]
        val attributes: Seq[Attribute] = scenario.getSteps.asScala.map { step =>
          val definition = glue.stepDefinitionMatch(featurePath, step, i18n)
          val docString = parseDocString(step.getDocString)
          definition.getPattern match {
            case INIT_DB =>
              val query = definition.getArguments.get(0).getVal
              Init(query, docString)
            case USING_DB =>
              val databaseName = definition.getArguments.get(0).getVal
              Using(databaseName, docString)
            case RUNNING_QUERY =>
              val query = definition.getArguments.get(0).getVal
              val tags = QueryTagger(query)
              Run(query, tags, None, docString)
            case RUNNING_PARAMETRIZED_QUERY =>
              val query = definition.getArguments.get(0).getVal
              val converter = new TableConverter(streams.get(i18n.getLocale), null)
              val table = new DataTable(step.getRows, converter)
              val params = table.asScala[AnyRef].map { m => Map(m.toList: _*) }
              assert(params.size == 1)
              val tags = QueryTagger(query)
              Run(query, tags, Some(params.head), docString)
            case RESULT =>
              val converter = new TableConverter(streams.get(i18n.getLocale), null)
              val table = new DataTable(step.getRows, converter)
              val data = table.asScala[String].map { m => Map(m.toList: _*) }
              Result(data, docString)
          }
        }

        Scenario(featureName, scenario.getGherkinModel.getName, attributes)
      }
    }
  }

  private def parseDocString(comment: DocString): Option[String] =
    if (comment == null) None
    else {
      val str = comment.getValue
      if (str == null || str == "") None else Some(str)
    }
}
