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
package cypher.features

import org.neo4j.kernel.api.exceptions.Status
import org.opencypher.tools.tck.api.ExecutionFailed
import org.opencypher.tools.tck.constants.TCKErrorDetails.AMBIGUOUS_AGGREGATION_EXPRESSION
import org.opencypher.tools.tck.constants.TCKErrorDetails.COLUMN_NAME_CONFLICT
import org.opencypher.tools.tck.constants.TCKErrorDetails.CREATING_VAR_LENGTH
import org.opencypher.tools.tck.constants.TCKErrorDetails.DELETED_ENTITY_ACCESS
import org.opencypher.tools.tck.constants.TCKErrorDetails.DELETE_CONNECTED_NODE
import org.opencypher.tools.tck.constants.TCKErrorDetails.DIFFERENT_COLUMNS_IN_UNION
import org.opencypher.tools.tck.constants.TCKErrorDetails.FLOATING_POINT_OVERFLOW
import org.opencypher.tools.tck.constants.TCKErrorDetails.INTEGER_OVERFLOW
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_AGGREGATION
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_ARGUMENT_EXPRESSION
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_ARGUMENT_PASSING_MODE
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_ARGUMENT_TYPE
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_ARGUMENT_VALUE
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_CLAUSE_COMPOSITION
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_DELETE
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_ELEMENT_ACCESS
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_NUMBER_LITERAL
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_NUMBER_OF_ARGUMENTS
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_PARAMETER_USE
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_PROPERTY_TYPE
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_RELATIONSHIP_PATTERN
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_UNICODE_CHARACTER
import org.opencypher.tools.tck.constants.TCKErrorDetails.INVALID_UNICODE_LITERAL
import org.opencypher.tools.tck.constants.TCKErrorDetails.LIST_ELEMENT_ACCESS_BY_NON_INTEGER
import org.opencypher.tools.tck.constants.TCKErrorDetails.MAP_ELEMENT_ACCESS_BY_NON_STRING
import org.opencypher.tools.tck.constants.TCKErrorDetails.MERGE_READ_OWN_WRITES
import org.opencypher.tools.tck.constants.TCKErrorDetails.MISSING_PARAMETER
import org.opencypher.tools.tck.constants.TCKErrorDetails.NEGATIVE_INTEGER_ARGUMENT
import org.opencypher.tools.tck.constants.TCKErrorDetails.NESTED_AGGREGATION
import org.opencypher.tools.tck.constants.TCKErrorDetails.NON_CONSTANT_EXPRESSION
import org.opencypher.tools.tck.constants.TCKErrorDetails.NO_EXPRESSION_ALIAS
import org.opencypher.tools.tck.constants.TCKErrorDetails.NO_SINGLE_RELATIONSHIP_TYPE
import org.opencypher.tools.tck.constants.TCKErrorDetails.NO_VARIABLES_IN_SCOPE
import org.opencypher.tools.tck.constants.TCKErrorDetails.NUMBER_OUT_OF_RANGE
import org.opencypher.tools.tck.constants.TCKErrorDetails.PROCEDURE_NOT_FOUND
import org.opencypher.tools.tck.constants.TCKErrorDetails.PROPERTY_ACCESS_ON_NON_MAP
import org.opencypher.tools.tck.constants.TCKErrorDetails.RELATIONSHIP_UNIQUENESS_VIOLATION
import org.opencypher.tools.tck.constants.TCKErrorDetails.REQUIRES_DIRECTED_RELATIONSHIP
import org.opencypher.tools.tck.constants.TCKErrorDetails.UNDEFINED_VARIABLE
import org.opencypher.tools.tck.constants.TCKErrorDetails.UNEXPECTED_SYNTAX
import org.opencypher.tools.tck.constants.TCKErrorDetails.UNKNOWN_FUNCTION
import org.opencypher.tools.tck.constants.TCKErrorDetails.VARIABLE_ALREADY_BOUND
import org.opencypher.tools.tck.constants.TCKErrorDetails.VARIABLE_TYPE_CONFLICT

object Phase {
  val runtime = "runtime"
  val compile = "compile time"
}

case class Neo4jExecutionFailed(errorType: String, phase: String, detail: String, cause: Throwable)
    extends Exception(cause)

object Neo4jExceptionToExecutionFailed {

  def convert(phase: String, t: Throwable): ExecutionFailed = {
    val neo4jException = t match {
      case re: RuntimeException => re
      case _                    => throw t
    }
    val errorType = Status.statusCodeOf(neo4jException)
    val errorTypeStr =
      if (errorType != null) errorTypeMapping(errorType)
      else ""
    val msg = neo4jException.getMessage
    val detail = phase match {
      case Phase.compile => compileTimeDetail(msg)
      case Phase.runtime => runtimeDetail(msg)
      case x             => throw new InternalError(s"Expected ${Phase.compile} or ${Phase.runtime} but got $x")
    }
    val neo4jexception = Neo4jExecutionFailed(errorTypeStr, phase, detail, t)
    ExecutionFailed(errorTypeStr, phase, detail, Some(neo4jexception))
  }

  private def errorTypeMapping(errorType: Status): String = {
    if (errorType == Status.Procedure.ProcedureNotFound) {
      // TCK uses a different name
      "ProcedureError"
    } else {
      errorType.toString
    }
  }

  private def runtimeDetail(msg: String): String = {
    if (msg == null)
      ""
    else if (
      msg.matches(
        "((SKIP: )|(LIMIT: )|(OF \\.\\.\\. ROWS: ))?Invalid input. ('-.+' is not a valid value|Got a negative integer)\\. Must be a ((non-negative)|(positive)) integer\\.[\\s.\\S]*"
      )
    )
      NEGATIVE_INTEGER_ARGUMENT
    else if (
      msg.matches(
        "((SKIP: )|(LIMIT: )|(OF \\.\\.\\. ROWS: ))?Invalid input. ('.+' is not a valid value|Got a floating-point number)\\. Must be a ((non-negative)|(positive)) integer\\.[\\s.\\S]*"
      )
    )
      INVALID_ARGUMENT_TYPE
    else if (msg.matches("Type mismatch: expected a map but was .+"))
      PROPERTY_ACCESS_ON_NON_MAP
    else if (
      msg.matches(
        "Cannot access a map 'Map\\{.+\\}' by key '.+': Expected .+ to be a ((java.lang.String)|(org.neo4j.values.storable.TextValue)), but it was a .+"
      )
    )
      MAP_ELEMENT_ACCESS_BY_NON_STRING
    else if (
      msg.matches(
        "Cannot access a list 'List\\{.+\\}' using a non-number index, got .+: Expected .+ to be a ((java.lang.Number)|(org.neo4j.values.storable.NumberValue)), but it was a .+"
      )
    )
      LIST_ELEMENT_ACCESS_BY_NON_INTEGER
    else if (
      msg.matches(
        ".+ is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key .+"
      )
    )
      INVALID_ELEMENT_ACCESS
    else if (
      msg.matches(
        s"\nElement access is only possible by performing a collection lookup using an integer index,\nor by performing a map lookup using a string key .+"
      )
    )
      INVALID_ELEMENT_ACCESS
    else if (
      msg.matches(".+ can not create a new node due to conflicts with( both)? existing( and missing)? unique nodes.*")
    )
      "CreateBlockedByConstraint"
    else if (msg.matches("Node\\(\\d+\\) already exists with label `.+` and property `.+` = .+"))
      "CreateBlockedByConstraint"
    else if (
      msg.matches(
        "Cannot delete node\\<\\d+\\>, because it still has relationships. To delete this node, you must first delete its relationships."
      )
    )
      DELETE_CONNECTED_NODE
    else if (msg.matches("Don't know how to compare that\\..+"))
      "IncomparableValues"
    else if (msg.matches("Cannot perform .+ on mixed types\\..+"))
      "IncomparableValues"
    else if (msg.matches("Invalid input '.+' is not a valid argument, must be a number in the range 0.0 to 1.0"))
      NUMBER_OUT_OF_RANGE
    else if (msg.matches("Step argument to 'range\\(\\)' cannot be zero"))
      NUMBER_OUT_OF_RANGE
    else if (msg.matches("Invalid input for function '.+': Expected a (.+), got: (.*)"))
      INVALID_ARGUMENT_VALUE
    else if (msg.matches("The expression .+ should have been a node or a relationship, but got .+"))
      REQUIRES_DIRECTED_RELATIONSHIP
    else if (msg.matches("((Node)|(Relationship)) with id \\d+ has been deleted in this transaction"))
      DELETED_ENTITY_ACCESS
    else if (msg.matches("Expected parameter\\(s\\): .+"))
      MISSING_PARAMETER
    else if (msg.matches("Cannot merge the following ((relationship)|(node)) because of null property value for .+"))
      MERGE_READ_OWN_WRITES
    else if (msg.startsWith("Property values can only be of primitive types or arrays thereof"))
      INVALID_PROPERTY_TYPE
    else
      msg
  }

  private def compileTimeDetail(msg: String): String = {
    if (msg == null)
      ""
    else if (
      msg.matches(
        "Invalid input. '-.+' is not a valid value. Must be a ((non-negative)|(positive)) integer\\.[\\s.\\S]*"
      )
    )
      NEGATIVE_INTEGER_ARGUMENT
    else if (
      msg.matches(
        "Invalid input. '.+' is not a valid value. Must be a ((non-negative)|(positive)) integer\\.[\\s.\\S]*"
      )
    )
      INVALID_ARGUMENT_TYPE
    else if (
      msg.matches("Coercion of list to boolean is not allowed. Please use `NOT isEmpty\\(...\\)` instead.[\\s.\\S]*")
    )
      INVALID_ARGUMENT_TYPE
    else if (msg.matches(semanticError("Can't use aggregate functions inside of aggregate functions\\.")))
      NESTED_AGGREGATION
    else if (
      msg.matches(
        "Can't create node `(\\w+)` with labels or properties here. The variable is already declared in this context"
      )
    )
      VARIABLE_ALREADY_BOUND
    else if (
      msg.matches(
        "Can't create `\\w+` with properties or labels here. The variable is already declared in this context"
      )
    )
      VARIABLE_ALREADY_BOUND
    else if (msg.matches(semanticError("\\w+ already declared")))
      VARIABLE_ALREADY_BOUND
    else if (
      msg.matches(
        semanticError("The variable `\\w+` occurs in multiple quantified path patterns and needs to be renamed.")
      )
    )
      VARIABLE_ALREADY_BOUND
    else if (
      msg.matches(semanticError(
        "The variable `\\w+` occurs both inside and outside a quantified path pattern and needs to be renamed."
      ))
    )
      VARIABLE_ALREADY_BOUND
    else if (msg.matches(semanticError("Only directed relationships are supported in ((CREATE)|(MERGE))")))
      REQUIRES_DIRECTED_RELATIONSHIP
    else if (msg.matches(s"${DOTALL}Type mismatch: map key must be given as String, but was .+"))
      MAP_ELEMENT_ACCESS_BY_NON_STRING
    else if (msg.matches(s"${DOTALL}Type mismatch: expected .+ but was .+"))
      INVALID_ARGUMENT_TYPE
    else if (msg.matches(semanticError("Variable `.+` not defined")))
      UNDEFINED_VARIABLE
    else if (msg.matches(semanticError(".+ not defined")))
      UNDEFINED_VARIABLE
    else if (
      msg.matches(semanticError(
        "Aggregation column contains implicit grouping expressions. .+"
      ))
    ) AMBIGUOUS_AGGREGATION_EXPRESSION
    else if (msg.matches(semanticError("PatternExpressions are not allowed to introduce new variables: .+")))
      UNDEFINED_VARIABLE
    else if (msg.matches(semanticError("Type mismatch: .+ defined with conflicting type .+ \\(expected .+\\)")))
      VARIABLE_TYPE_CONFLICT
    else if (msg.matches(semanticError("Cannot use the same relationship variable '.+' for multiple relationships")))
      RELATIONSHIP_UNIQUENESS_VIOLATION
    else if (msg.matches(semanticError("Cannot use the same relationship identifier '.+' for multiple relationships")))
      RELATIONSHIP_UNIQUENESS_VIOLATION
    else if (msg.matches(semanticError("Variable length relationships cannot be used in ((CREATE)|(MERGE))")))
      CREATING_VAR_LENGTH
    else if (
      msg.matches(semanticError(
        "Parameter maps cannot be used in `((MATCH)|(MERGE))` patterns \\(use a literal map instead, e.g. `\\{id: \\$.+\\.id\\}`\\)"
      ))
    )
      INVALID_PARAMETER_USE
    else if (msg.matches(semanticError("Variable `.+` already declared")))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches(semanticError("Invalid combination of UNION and UNION ALL")))
      INVALID_CLAUSE_COMPOSITION
    else if (msg.matches(semanticError("floating point number is too large")))
      FLOATING_POINT_OVERFLOW
    else if (msg.matches(semanticError("integer is too large")))
      INTEGER_OVERFLOW
    else if (msg.matches(semanticError("Argument to exists\\(\\.\\.\\.\\) is not a property or pattern")))
      INVALID_ARGUMENT_EXPRESSION
    else if (msg.startsWith("Invalid input '—':"))
      INVALID_UNICODE_CHARACTER
    else if (msg.matches(semanticError("Can't use aggregating expressions inside of expressions executing over lists")))
      INVALID_AGGREGATION
    else if (
      msg.matches(semanticError("Can't use aggregating expressions inside of expressions executing over collections"))
    )
      INVALID_AGGREGATION
    else if (
      msg.matches(semanticError("It is not allowed to refer to variables in ((SKIP)|(LIMIT)|(OF \\.\\.\\. ROWS)).*"))
    )
      NON_CONSTANT_EXPRESSION
    else if (
      msg.matches(
        semanticError("It is not allowed to use patterns in the expression for ((SKIP)|(LIMIT)|(OF \\.\\.\\. ROWS)).*")
      )
    )
      NON_CONSTANT_EXPRESSION
    else if (
      msg.matches(semanticError("It is not allowed to refer to identifiers in ((SKIP)|(LIMIT)|(OF \\.\\.\\. ROWS))"))
    )
      NON_CONSTANT_EXPRESSION
    else if (msg.matches("Can't use non-deterministic \\(random\\) functions inside of aggregate functions\\."))
      NON_CONSTANT_EXPRESSION
    else if (
      msg.matches(
        semanticError("A single (plain )?relationship type (like :\\\\w+)?must be specified for ((CREATE)|(MERGE))\\")
      ) ||
      msg.matches(semanticError("Exactly one relationship type must be specified for ((CREATE)|(MERGE))\\. " +
        "Did you forget to prefix your relationship type with a \\'\\:\\'\\?"))
    )
      NO_SINGLE_RELATIONSHIP_TYPE
    else if (
      msg.matches(s"${DOTALL}Invalid input '.*': expected.*\\].*\\{.*\\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*")
    )
      INVALID_RELATIONSHIP_PATTERN
    else if (
      msg.matches(s"${DOTALL}Invalid input '.*': expected.*or.*\\].*\\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*")
    )
      INVALID_RELATIONSHIP_PATTERN
    else if (msg.matches(semanticError("invalid literal number")))
      INVALID_NUMBER_LITERAL
    else if (msg.matches(semanticError("Unknown function '.+'")))
      UNKNOWN_FUNCTION
    else if (
      msg.matches(semanticError("Invalid input '.+': expected four hexadecimal digits specifying a unicode character"))
    )
      INVALID_UNICODE_LITERAL
    else if (msg.matches(semanticError("Invalid use of aggregating function count\\(\\.\\.\\.\\) in this context")))
      INVALID_AGGREGATION
    else if (
      msg.matches(semanticError(
        "Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding ((RETURN)|(WITH))"
      ))
    )
      INVALID_AGGREGATION
    else if (msg.matches(semanticError("Expression in .* must be aliased \\(use AS\\)")))
      NO_EXPRESSION_ALIAS
    else if (msg.matches(semanticError("All sub queries in an UNION must have the same return column names")))
      DIFFERENT_COLUMNS_IN_UNION
    else if (msg.matches(semanticError("DELETE doesn't support removing labels from a node. Try REMOVE.")))
      INVALID_DELETE
    else if (msg.matches(semanticError("Multiple result columns with the same name are not supported")))
      COLUMN_NAME_CONFLICT
    else if (msg.matches(semanticError("RETURN \\* is not allowed when there are no variables in scope")))
      NO_VARIABLES_IN_SCOPE
    else if (msg.matches(semanticError("RETURN \\* is not allowed when there are no identifiers in scope")))
      NO_VARIABLES_IN_SCOPE
    else if (msg.matches(semanticError("Procedure call does not provide the required number of arguments.+")))
      INVALID_NUMBER_OF_ARGUMENTS
    else if (msg.matches(semanticError("Procedure call provides too many arguments.+")))
      INVALID_NUMBER_OF_ARGUMENTS
    else if (msg.matches("Expected a parameter named .+"))
      MISSING_PARAMETER
    else if (
      msg.startsWith(
        "Procedure call cannot take an aggregating function as argument, please add a 'WITH' to your statement."
      )
    )
      INVALID_AGGREGATION
    else if (
      msg.startsWith(
        "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN"
      )
    )
      UNDEFINED_VARIABLE
    else if (msg.startsWith("Illegal aggregation expression(s) in order by"))
      AMBIGUOUS_AGGREGATION_EXPRESSION
    else if (msg.startsWith("Procedure call inside a query does not support passing arguments implicitly"))
      INVALID_ARGUMENT_PASSING_MODE
    else if (
      msg.matches(
        "There is no procedure with the name `.+` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed."
      )
    )
      PROCEDURE_NOT_FOUND
    else if (msg.startsWith("Type mismatch for parameter"))
      INVALID_ARGUMENT_TYPE
    else if (msg.matches(semanticError("Cannot use `YIELD \\*` outside standalone call")))
      UNEXPECTED_SYNTAX
    else if (msg.startsWith("A pattern expression should only be used in order to test the existence of a pattern"))
      UNEXPECTED_SYNTAX
    else if (msg.startsWith("Invalid input"))
      UNEXPECTED_SYNTAX
    else if (msg.startsWith("Query cannot conclude with"))
      INVALID_CLAUSE_COMPOSITION
    else if (msg.startsWith("An Exists Expression cannot contain any updates"))
      INVALID_CLAUSE_COMPOSITION
    else if (msg.startsWith("Only directed relationships are supported in INSERT"))
      REQUIRES_DIRECTED_RELATIONSHIP
    else
      msg.replaceAll(DOTALL + POSITION_PATTERN, "")
  }

  /**
   * This is a modifier to match newlines with `.` in a Regex.
   */
  private val DOTALL = "(?s)"

  private val POSITION_PATTERN = " \\(line .+, column .+ \\(offset: .+\\)\\).*"

  private def semanticError(pattern: String): String = DOTALL + pattern + POSITION_PATTERN

}
