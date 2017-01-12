/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.graphdb.{ConstraintViolationException, QueryExecutionException, Result, Transaction}
import org.opencypher.tools.tck.constants.TCKErrorDetails._
import org.opencypher.tools.tck.constants.{TCKErrorPhases, TCKErrorTypes}
import org.scalatest.{Assertions, Matchers}

import scala.util.{Failure, Success, Try}

case class SpecSuiteErrorHandler(typ: String, phase: String, detail: String) extends Matchers with Assertions {

  def check(result: Try[Result], tx: Transaction) = {
    phase match {
      case TCKErrorPhases.COMPILE_TIME => checkError(result, compileTimeError)
      case TCKErrorPhases.RUNTIME => result match {
        case Success(triedResult) =>
          // might need to exhaust result to provoke error
          val consumedResult = Try {
            while (triedResult.hasNext) {
              triedResult.next()
            }
            tx.success()
            tx.close()
            triedResult
          }
          checkError(consumedResult, runtimeError)
        case x => checkError(x, runtimeError)
      }
      case _ => fail(s"Unknown phase $phase specified. Supported values are '${TCKErrorPhases.COMPILE_TIME}' and '${TCKErrorPhases.RUNTIME}'.")
    }
  }

  private val DOTALL = "(?s)"

  private def checkError(result: Try[Result], msgHandler: String => Boolean) = {
    val statusType =
      if (typ == TCKErrorTypes.CONSTRAINT_VALIDATION_FAILED) "Schema"
      else if (typ == "ProcedureError") "Procedure"
      else "Statement"

    // TODO: Bloody hack
    val statusDetail = if (typ == "ProcedureError") "ProcedureNotFound" else typ

    result match {
      case Failure(e: QueryExecutionException) =>
        withClue(e.getMessage) {
          s"Neo.ClientError.$statusType.$statusDetail" should equal(e.getStatusCode)
        }

        if (!msgHandler(e.getMessage)) fail(s"Unknown $phase error: $e", e)

      case Failure(e: ConstraintViolationException) =>
        // Due to the explicit tx management (which is necessary due to how results are streamed back from the 2.3 iterators),
        // some exceptions aren't coming from within Cypher, but from the kernel at the tx commit phase.
        // We could work around this by caching all results, but that has implications for error handling in general.

        if (!msgHandler(e.getMessage)) fail(s"Unknown $phase error: $e", e)

      case Failure(e) =>
        fail(s"Unknown $phase error: $e", e)

      case Success(r) =>
        r.close()
        fail(s"No $phase error was raised")
    }
  }

  private def compileTimeError(msg: String): Boolean = {
    var r = true

    if (msg.matches("Invalid input '-(\\d)+' is not a valid value, must be a positive integer[\\s.\\S]+"))
      detail should equal(NEGATIVE_INTEGER_ARGUMENT)
    else if (msg.matches("Invalid input '.+' is not a valid value, must be a positive integer[\\s.\\S]+"))
      detail should equal(INVALID_ARGUMENT_TYPE)
    else if (msg.matches("Can't use aggregate functions inside of aggregate functions\\."))
      detail should equal(NESTED_AGGREGATION)
    else if (msg.matches("Can't create node `(\\w+)` with labels or properties here. The variable is already declared in this context"))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches("Can't create node `(\\w+)` with labels or properties here. It already exists in this context"))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches("Can't create `\\w+` with properties or labels here. The variable is already declared in this context"))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches("Can't create `\\w+` with properties or labels here. It already exists in this context"))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches("Can't create `(\\w+)` with labels or properties here. It already exists in this context"))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches(semanticError("\\w+ already declared")))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches(semanticError("Only directed relationships are supported in ((CREATE)|(MERGE))")))
      detail should equal(REQUIRES_DIRECTED_RELATIONSHIP)
    else if (msg.matches(s"${DOTALL}Type mismatch: expected .+ but was .+"))
      detail should equal(INVALID_ARGUMENT_TYPE)
    else if (msg.matches(semanticError("Variable `.+` not defined")))
      detail should equal(UNDEFINED_VARIABLE)
    else if (msg.matches(semanticError(".+ not defined")))
      detail should equal(UNDEFINED_VARIABLE)
    else if (msg.matches(semanticError("Type mismatch: .+ already defined with conflicting type .+ \\(expected .+\\)")))
      detail should equal(VARIABLE_TYPE_CONFLICT)
    else if (msg.matches(semanticError("Cannot use the same relationship variable '.+' for multiple patterns")))
      detail should equal(RELATIONSHIP_UNIQUENESS_VIOLATION)
    else if (msg.matches(semanticError("Cannot use the same relationship identifier '.+' for multiple patterns")))
      detail should equal(RELATIONSHIP_UNIQUENESS_VIOLATION)
    else if (msg.matches(semanticError("Variable length relationships cannot be used in ((CREATE)|(MERGE))")))
      detail should equal(CREATING_VAR_LENGTH)
    else if (msg.matches(semanticError("Parameter maps cannot be used in ((MATCH)|(MERGE)) patterns \\(use a literal map instead, eg. \"\\{id: \\{param\\}\\.id\\}\"\\)")))
      detail should equal(INVALID_PARAMETER_USE)
    else if (msg.matches(semanticError("Variable `.+` already declared")))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches(semanticError("MATCH cannot follow OPTIONAL MATCH \\(perhaps use a WITH clause between them\\)")))
      detail should equal(INVALID_CLAUSE_COMPOSITION)
    else if (msg.matches(semanticError("Invalid combination of UNION and UNION ALL")))
      detail should equal(INVALID_CLAUSE_COMPOSITION)
    else if (msg.matches(semanticError("floating point number is too large")))
      detail should equal(FLOATING_POINT_OVERFLOW)
    else if (msg.matches(semanticError("Argument to exists\\(\\.\\.\\.\\) is not a property or pattern")))
      detail should equal(INVALID_ARGUMENT_EXPRESSION)
    else if (msg.startsWith("Invalid input '—':"))
      detail should equal(INVALID_UNICODE_CHARACTER)
    else if (msg.matches(semanticError("Can't use aggregating expressions inside of expressions executing over lists")))
      detail should equal(INVALID_AGGREGATION)
    else if (msg.matches(semanticError("Can't use aggregating expressions inside of expressions executing over collections")))
      detail should equal(INVALID_AGGREGATION)
    else if (msg.matches(semanticError("It is not allowed to refer to variables in ((SKIP)|(LIMIT))")))
      detail should equal(NON_CONSTANT_EXPRESSION)
    else if (msg.matches(semanticError("It is not allowed to refer to identifiers in ((SKIP)|(LIMIT))")))
      detail should equal(NON_CONSTANT_EXPRESSION)
    else if (msg.matches("Can't use non-deterministic \\(random\\) functions inside of aggregate functions\\."))
      detail should equal(NON_CONSTANT_EXPRESSION)
    else if (msg.matches(semanticError("A single relationship type must be specified for ((CREATE)|(MERGE))")))
      detail should equal(NO_SINGLE_RELATIONSHIP_TYPE)
    else if (msg.matches(s"${DOTALL}Invalid input '.*': expected an identifier character, whitespace, '\\|', a length specification, a property map or '\\]' \\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*"))
      detail should equal(INVALID_RELATIONSHIP_PATTERN)
    else if (msg.matches(s"${DOTALL}Invalid input '.*': expected whitespace, RangeLiteral, a property map or '\\]' \\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*"))
      detail should equal(INVALID_RELATIONSHIP_PATTERN)
    else if (msg.matches(semanticError("invalid literal number")))
      detail should equal(INVALID_NUMBER_LITERAL)
    else if (msg.matches(semanticError("Unknown function '.+'")))
      detail should equal(UNKNOWN_FUNCTION)
    else if (msg.matches(semanticError("Invalid input '.+': expected four hexadecimal digits specifying a unicode character")))
      detail should equal(INVALID_UNICODE_LITERAL)
    else if (msg.matches("Cannot merge ((relationship)|(node)) using null property value for .+"))
      detail should equal(MERGE_READ_OWN_WRITES)
    else if (msg.matches(semanticError("Invalid use of aggregating function count\\(\\.\\.\\.\\) in this context")))
      detail should equal(INVALID_AGGREGATION)
    else if (msg.matches(semanticError("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding ((RETURN)|(WITH))")))
      detail should equal(INVALID_AGGREGATION)
    else if (msg.matches(semanticError("Expression in WITH must be aliased \\(use AS\\)")))
      detail should equal(NO_EXPRESSION_ALIAS)
    else if (msg.matches(semanticError("All sub queries in an UNION must have the same column names")))
      detail should equal(DIFFERENT_COLUMNS_IN_UNION)
    else if (msg.matches(semanticError("DELETE doesn't support removing labels from a node. Try REMOVE.")))
      detail should equal(INVALID_DELETE)
    else if (msg.matches("Property values can only be of primitive types or arrays thereof"))
      detail should equal(INVALID_PROPERTY_TYPE)
    else if (msg.matches(semanticError("Multiple result columns with the same name are not supported")))
      detail should equal(COLUMN_NAME_CONFLICT)
    else if (msg.matches(semanticError("RETURN \\* is not allowed when there are no variables in scope")))
      detail should equal(NO_VARIABLES_IN_SCOPE)
    else if (msg.matches(semanticError("RETURN \\* is not allowed when there are no identifiers in scope")))
      detail should equal(NO_VARIABLES_IN_SCOPE)
    else if (msg.matches(semanticError("Procedure call does not provide the required number of arguments.+")))
      detail should equal("InvalidNumberOfArguments")
    else if (msg.matches("Expected a parameter named .+"))
      detail should equal("MissingParameter")
    else if (msg.startsWith("Procedure call cannot take an aggregating function as argument, please add a 'WITH' to your statement."))
      detail should equal("InvalidAggregation")
    else if (msg.startsWith("Procedure call inside a query does not support passing arguments implicitly (pass explicitly after procedure name instead)"))
      detail should equal("InvalidArgumentPassingMode")
    else if (msg.matches("There is no procedure with the name `.+` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed."))
      detail should equal("ProcedureNotFound")
    else r = false

    r
  }

  private def runtimeError(msg: String): Boolean = {
    var r = true

    if (msg.matches("Type mismatch: expected a map but was .+"))
      detail should equal(PROPERTY_ACCESS_ON_NON_MAP)
    else if (msg.matches("Expected .+ to be a java.lang.String, but it was a .+"))
      detail should equal(MAP_ELEMENT_ACCESS_BY_NON_STRING)
    else if (msg.matches("Expected .+ to be a java.lang.Number, but it was a .+"))
      detail should equal(LIST_ELEMENT_ACCESS_BY_NON_INTEGER)
    else if (msg.matches(".+ is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key .+"))
      detail should equal(INVALID_ELEMENT_ACCESS)
    else if (msg.matches(s"\nElement access is only possible by performing a collection lookup using an integer index,\nor by performing a map lookup using a string key .+"))
      detail should equal(INVALID_ELEMENT_ACCESS)
    else if (msg.matches(".+ can not create a new node due to conflicts with( both)? existing( and missing)? unique nodes.*"))
      detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
    else if (msg.matches("Node [0-9]+ already exists with label .+ and property \".+\"=\\[.+\\]"))
      detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
    else if (msg.matches("Cannot delete node\\<\\d+\\>, because it still has relationships. To delete this node, you must first delete its relationships."))
      detail should equal(DELETE_CONNECTED_NODE)
    else if (msg.matches("Don't know how to compare that\\..+"))
      detail should equal(INCOMPARABLE_VALUES)
    else if (msg.matches("Invalid input '.+' is not a valid argument, must be a number in the range 0.0 to 1.0"))
      detail should equal(NUMBER_OUT_OF_RANGE)
    else if (msg.matches("step argument to range\\(\\) cannot be zero"))
      detail should equal(NUMBER_OUT_OF_RANGE)
    else if (msg.matches("Expected a (.+), got: (.*)"))
      detail should equal(INVALID_ARGUMENT_VALUE)
    else if (msg.matches("The expression .+ should have been a node or a relationship, but got .+"))
      detail should equal(REQUIRES_DIRECTED_RELATIONSHIP)
    else if (msg.matches("((Node)|(Relationship)) with id 0 has been deleted in this transaction"))
      detail should equal(DELETED_ENTITY_ACCESS)
    else r = false

    r
  }

  private val POSITION_PATTERN = " \\(line .+, column .+ \\(offset: .+\\)\\).*"

  private def semanticError(pattern: String): String = DOTALL + pattern + POSITION_PATTERN
}
