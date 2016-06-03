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
      case TCKErrorPhases.COMPILE_TIME => checkError(result, typ, phase, detail, compileTimeError)
      case TCKErrorPhases.RUNTIME => result match {
        case Success(triedResult) =>
          // might need to exhaust result to provoke error
          val consumedResult = Try {
            while (triedResult.hasNext) {
              triedResult.next()
            }
            triedResult
          }
          checkError(consumedResult, typ, phase, detail, runtimeError)
        case x => checkError(x, typ, phase, detail, runtimeError)
      }
      case _ => fail(s"Unknown phase $phase specified. Supported values are '${TCKErrorPhases.COMPILE_TIME}' and '${TCKErrorPhases.RUNTIME}'.")
    }
  }

  private val DOTALL = "(?s)"

  private def checkError(result: Try[Result], typ: String, phase: String, detail: String, msgHandler: String => Boolean) = {
    val statusType = if (typ == TCKErrorTypes.CONSTRAINT_VALIDATION_FAILED) "Schema" else "Statement"
    result match {
      case Failure(e: QueryExecutionException) =>
        s"Neo.ClientError.$statusType.$typ" should equal(e.getStatusCode)

        if (!msgHandler(e.getMessage)) fail(s"Unknown $phase error: $e", e)

      case Failure(e) =>
        fail(s"Unknown $phase error: $e", e)

      case _: Success[_] =>
        fail(s"No $phase error was raised")
    }
  }

  private def compileTimeError(msg: String): Boolean = {
    var r = true

    if (msg.matches("Invalid input '-(\\d)+' is not a valid value, must be a positive integer[\\s.\\S]+"))
      detail should equal(NEGATIVE_INTEGER_ARGUMENT)
    else if (msg.matches("Invalid input '.+' is not a valid value, must be a positive integer[\\s.\\S]+"))
      detail should equal("InvalidArgumentType")
    else if (msg.matches("Can't use aggregate functions inside of aggregate functions\\."))
      detail should equal(NESTED_AGGREGATION)
    else if (msg.matches("Can't create node `(\\w+)` with labels or properties here. The variable is already declared in this context"))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches("Can't create `\\w+` with properties or labels here. The variable is already declared in this context"))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches(semanticError("Only directed relationships are supported in ((CREATE)|(MERGE))")))
      detail should equal(REQUIRES_DIRECTED_RELATIONSHIP)
    else if (msg.matches(s"${DOTALL}Type mismatch: expected .+ but was .+"))
      detail should equal("InvalidArgumentType")
    else if (msg.matches(semanticError("Variable `.+` not defined")))
      detail should equal("UndefinedVariable")
    else if (msg.matches(semanticError("Type mismatch: .+ already defined with conflicting type .+ \\(expected .+\\)")))
      detail should equal("VariableTypeConflict")
    else if (msg.matches(semanticError("Cannot use the same relationship variable '.+' for multiple patterns")))
      detail should equal("RelationshipUniquenessViolation")
    else if (msg.matches(semanticError("Variable length relationships cannot be used in ((CREATE)|(MERGE))")))
      detail should equal("CreatingVarLength")
    else if (msg.matches(semanticError("Parameter maps cannot be used in ((MATCH)|(MERGE)) patterns \\(use a literal map instead, eg. \"\\{id: \\{param\\}\\.id\\}\"\\)")))
      detail should equal("InvalidParameterUse")
    else if (msg.matches(semanticError("Variable `.+` already declared")))
      detail should equal(VARIABLE_ALREADY_BOUND)
    else if (msg.matches(semanticError("MATCH cannot follow OPTIONAL MATCH \\(perhaps use a WITH clause between them\\)")))
      detail should equal("InvalidClauseComposition")
    else if (msg.matches(semanticError("Invalid combination of UNION and UNION ALL")))
      detail should equal("InvalidClauseComposition")
    else if (msg.matches(semanticError("floating point number is too large")))
      detail should equal("FloatingPointOverflow")
    else if (msg.matches(semanticError("Argument to exists\\(\\.\\.\\.\\) is not a property or pattern")))
      detail should equal("InvalidArgumentExpression")
    else if (msg.startsWith("Invalid input '—':"))
      detail should equal("InvalidUnicode")
    else if (msg.matches(semanticError("Can't use aggregating expressions inside of expressions executing over lists")))
      detail should equal("InvalidAggregation")
    else if (msg.matches(semanticError("It is not allowed to refer to variables in ((SKIP)|(LIMIT))")))
      detail should equal("NonConstantExpression")
    else if (msg.matches("Can't use non-deterministic \\(random\\) functions inside of aggregate functions\\."))
      detail should equal("NonConstantExpression")
    else if (msg.matches(semanticError("A single relationship type must be specified for ((CREATE)|(MERGE))")))
      detail should equal("NoSingleRelationshipType")
    else if (msg.matches(s"${DOTALL}Invalid input '.*': expected an identifier character, whitespace, '\\|', a length specification, a property map or '\\]' \\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*"))
      detail should equal("InvalidRelationshipPattern")
    else if (msg.matches(s"${DOTALL}Invalid input '.*': expected whitespace, RangeLiteral, a property map or '\\]' \\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*"))
      detail should equal("InvalidRelationshipPattern")
    else if (msg.matches(semanticError("invalid literal number")))
      detail should equal("InvalidNumberLiteral")
    else if (msg.matches(semanticError("Unknown function '.+'")))
      detail should equal("UnknownFunction")
    else if (msg.matches(semanticError("Invalid input '.+': expected four hexadecimal digits specifying a unicode character")))
      detail should equal("InvalidUnicodeLiteral")
    else if (msg.matches("Cannot merge ((relationship)|(node)) using null property value for .+"))
      detail should equal("MergeReadOwnWrites")
    else if (msg.matches(semanticError("Invalid use of aggregating function count\\(\\.\\.\\.\\) in this context")))
      detail should equal("InvalidAggregation")
    else if (msg.matches(semanticError("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding ((RETURN)|(WITH))")))
      detail should equal("InvalidAggregation")
    else if (msg.matches(semanticError("Expression in WITH must be aliased \\(use AS\\)")))
      detail should equal("NoExpressionAlias")
    else if (msg.matches(semanticError("All sub queries in an UNION must have the same column names")))
      detail should equal("DifferentColumnsInUnion")
    else if (msg.matches(semanticError("DELETE doesn't support removing labels from a node. Try REMOVE.")))
      detail should equal("InvalidDelete")
    else if (msg.matches("Property values can only be of primitive types or arrays thereof"))
      detail should equal("InvalidPropertyType")
    else if (msg.matches(semanticError("Multiple result columns with the same name are not supported")))
      detail should equal("ColumnNameConflict")
    else if (msg.matches(semanticError("RETURN \\* is not allowed when there are no variables in scope")))
      detail should equal("NoVariablesInScope")
    else r = false

    r
  }

  private def runtimeError(msg: String): Boolean = {
    var r = true

    if (msg.matches("Type mismatch: expected a map but was .+"))
      detail should equal("PropertyAccessOnNonMap")
    else if (msg.matches("Expected .+ to be a java.lang.String, but it was a .+"))
      detail should equal(MAP_ELEMENT_ACCESS_BY_NON_STRING)
    else if (msg.matches("Expected .+ to be a java.lang.Number, but it was a .+"))
      detail should equal(LIST_ELEMENT_ACCESS_BY_NON_INTEGER)
    else if (msg.matches(".+ is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key .+"))
      detail should equal(INVALID_ELEMENT_ACCESS)
    else if (msg.matches(".+ can not create a new node due to conflicts with( both)? existing( and missing)? unique nodes.*"))
      detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
    else if (msg.matches("Node [0-9]+ already exists with label .+ and property \".+\"=\\[.+\\]"))
      detail should equal(CREATE_BLOCKED_BY_CONSTRAINT)
    else if (msg.matches("Cannot delete node\\<\\d+\\>, because it still has relationships. To delete this node, you must first delete its relationships."))
      detail should equal(DELETE_CONNECTED_NODE)
    else if (msg.matches("Don't know how to compare that\\..+"))
      detail should equal(INCOMPARABLE_VALUES)
    else if (msg.startsWith("It is not allowed to refer to variables in"))
      detail should equal(VARIABLE_USE_NOT_ALLOWED)
    else if (msg.matches("Invalid input '.+' is not a valid argument, must be a number in the range 0.0 to 1.0"))
      detail should equal("NumberOutOfRange")
    else if (msg.matches("step argument to range\\(\\) cannot be zero"))
      detail should equal("NumberOutOfRange")
    else if (msg.matches("Expected a String, Number or Boolean, got: .+"))
      detail should equal("InvalidArgumentValue")
    else if (msg.matches("The expression .+ should have been a node or a relationship, but got .+"))
      detail should equal("RequiresNodeOrRelationship")
    else r = false

    r
  }

  private val POSITION_PATTERN = " \\(line .+, column .+ \\(offset: .+\\)\\).*"

  private def semanticError(pattern: String): String = DOTALL + pattern + POSITION_PATTERN
}
