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

import org.neo4j.graphdb.{QueryExecutionException, Result}
import org.opencypher.tools.tck.constants.TCKErrorDetails._
import org.opencypher.tools.tck.constants.{TCKErrorPhases, TCKErrorTypes}
import org.scalatest.{Assertions, Matchers}

import scala.util.{Failure, Success, Try}

case class TCKErrorHandler(typ: String, phase: String, detail: String) extends Matchers with Assertions {

  def check(result: Try[Result]) = {
    phase match {
      case TCKErrorPhases.COMPILE_TIME => checkError(result, typ, phase, detail)
      case TCKErrorPhases.RUNTIME => result match {
        case Success(triedResult) =>
          // might need to exhaust result to provoke error
          val consumedResult = Try {
            while (triedResult.hasNext) {
              triedResult.next()
            }
            triedResult
          }
          checkError(consumedResult, typ, phase, detail)
        case x => checkError(x, typ, phase, detail)
      }
      case _ => fail(s"Unknown phase $phase specified. Supported values are '${TCKErrorPhases.COMPILE_TIME}' and '${TCKErrorPhases.RUNTIME}'.")
    }
  }

  private def checkError(result: Try[Result], typ: String, phase: String, detail: String) = {
    val statusType = if (typ == TCKErrorTypes.CONSTRAINT_VALIDATION_FAILED) "Schema" else "Statement"
    result match {
      case Failure(e: QueryExecutionException) =>
        s"Neo.ClientError.$statusType.$typ" should equal(e.getStatusCode)

        // Compile time errors
        if (e.getMessage.matches("Invalid input .+ is not a valid value, must be a positive integer[\\s.\\S]+"))
          detail should equal(NEGATIVE_INTEGER_ARGUMENT)
        else if (e.getMessage.matches("Can't use aggregate functions inside of aggregate functions\\."))
          detail should equal(NESTED_AGGREGATION)
        else if (e.getMessage.matches("Can't create node `(\\w+)` with labels or properties here. The variable is already declared in this context"))
          detail should equal(VARIABLE_ALREADY_BOUND)
        else if (e.getMessage.matches("Can't create node `\\w+` with labels or properties here. The variable is already declared in this context"))
          detail should equal(VARIABLE_ALREADY_BOUND)

        // Runtime errors
        else if (e.getMessage.matches("Expected .+ to be a java.lang.String, but it was a .+"))
          detail should equal(MAP_ELEMENT_ACCESS_BY_NON_STRING)
        else if (e.getMessage.matches("Expected .+ to be a java.lang.Number, but it was a .+"))
          detail should equal(LIST_ELEMENT_ACCESS_BY_NON_INTEGER)
        else if (e.getMessage.matches(".+ is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key .+"))
          detail should equal(INVALID_ELEMENT_ACCESS)
        else if (e.getMessage.matches(".+ can not create a new node due to conflicts with( both)? existing( and missing)? unique nodes.*"))
          detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
        else if (e.getMessage.matches("Node [0-9]+ already exists with label .+ and property \".+\"=\\[.+\\]"))
          detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
        else if (e.getMessage.matches("Cannot delete node\\<\\d+\\>, because it still has relationships. To delete this node, you must first delete its relationships."))
          detail should equal(DELETE_CONNECTED_NODE)
        else if (e.getMessage.matches("Don't know how to compare that\\..+"))
          detail should equal(INCOMPARABLE_VALUES)

        else fail(s"Unknown $phase error: $e", e)

      case Failure(e) =>
        fail(s"Unknown $phase error: $e", e)

      case _: Success[_] =>
        fail(s"No $phase error was raised")
    }

  }
}
