/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package cypher.features

import org.neo4j.kernel.api.exceptions.Status
import org.opencypher.tools.tck.api.ExecutionFailed
import org.opencypher.tools.tck.constants._

object Phase {
  val runtime = "runtime"
  val compile = "compile time"
}

object Neo4jExceptionToExecutionFailed {

  def convert(phase: String, t: Throwable): ExecutionFailed = {
    val neo4jException = t match {
      case re: RuntimeException => re
      case _ => throw t
    }
    val errorType = Status.statusCodeOf(neo4jException)
    val msg = neo4jException.getMessage
    val detail = phase match {
      case Phase.compile => compileTimeDetail(msg)
      case Phase.runtime => runtimeDetail(msg)
    }
    ExecutionFailed(if(errorType != null) errorType.toString else "", phase, detail)
  }

  private def runtimeDetail(msg: String): String = {
    import TCKErrorDetails._
    if (msg == null)
      ""
    else if (msg.matches("Type mismatch: expected a map but was .+"))
      PROPERTY_ACCESS_ON_NON_MAP
    else if (msg.matches("Expected .+ to be a ((java.lang.String)|(org.neo4j.values.storable.TextValue)), but it was a .+"))
      MAP_ELEMENT_ACCESS_BY_NON_STRING
    else if (msg.matches("Expected .+ to be a ((java.lang.Number)|(org.neo4j.values.storable.NumberValue)), but it was a .+"))
      LIST_ELEMENT_ACCESS_BY_NON_INTEGER
    else if (msg.matches(".+ is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key .+"))
      INVALID_ELEMENT_ACCESS
    else if (msg.matches(s"\nElement access is only possible by performing a collection lookup using an integer index,\nor by performing a map lookup using a string key .+"))
      INVALID_ELEMENT_ACCESS
    else if (msg.matches(".+ can not create a new node due to conflicts with( both)? existing( and missing)? unique nodes.*"))
      "CreateBlockedByConstraint"
    else if (msg.matches("Node\\(\\d+\\) already exists with label `.+` and property `.+` = .+"))
      "CreateBlockedByConstraint"
    else if (msg.matches("Cannot delete node\\<\\d+\\>, because it still has relationships. To delete this node, you must first delete its relationships."))
      DELETE_CONNECTED_NODE
    else if (msg.matches("Don't know how to compare that\\..+"))
      "IncomparableValues"
    else if (msg.matches("Cannot perform .+ on mixed types\\..+"))
      "IncomparableValues"
    else if (msg.matches("Invalid input '.+' is not a valid argument, must be a number in the range 0.0 to 1.0"))
      NUMBER_OUT_OF_RANGE
    else if (msg.matches("step argument to range\\(\\) cannot be zero"))
      NUMBER_OUT_OF_RANGE
    else if (msg.matches("Expected a (.+), got: (.*)"))
      INVALID_ARGUMENT_VALUE
    else if (msg.matches("The expression .+ should have been a node or a relationship, but got .+"))
      REQUIRES_DIRECTED_RELATIONSHIP
    else if (msg.matches("((Node)|(Relationship)) with id \\d+ has been deleted in this transaction"))
      DELETED_ENTITY_ACCESS
    else
      msg
  }

  private def compileTimeDetail(msg: String): String = {
    import TCKErrorDetails._
    if (msg.matches("Invalid input '-(\\d)+' is not a valid value, must be a positive integer[\\s.\\S]+"))
      NEGATIVE_INTEGER_ARGUMENT
    else if (msg.matches("Invalid input '.+' is not a valid value, must be a positive integer[\\s.\\S]+"))
      INVALID_ARGUMENT_TYPE
    else if (msg.matches("Can't use aggregate functions inside of aggregate functions\\."))
      NESTED_AGGREGATION
    else if (msg.matches("Can't create node `(\\w+)` with labels or properties here. The variable is already declared in this context"))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches("Can't create node `(\\w+)` with labels or properties here. It already exists in this context"))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches("Can't create `\\w+` with properties or labels here. The variable is already declared in this context"))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches("Can't create `\\w+` with properties or labels here. It already exists in this context"))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches("Can't create `(\\w+)` with labels or properties here. It already exists in this context"))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches(semanticError("\\w+ already declared")))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches(semanticError("Only directed relationships are supported in ((CREATE)|(MERGE))")))
      REQUIRES_DIRECTED_RELATIONSHIP
    else if (msg.matches(s"${DOTALL}Type mismatch: expected .+ but was .+"))
      INVALID_ARGUMENT_TYPE
    else if (msg.matches(semanticError("Variable `.+` not defined")))
      UNDEFINED_VARIABLE
    else if (msg.matches(semanticError(".+ not defined")))
      UNDEFINED_VARIABLE
    else if (msg.matches(semanticError("Type mismatch: .+ already defined with conflicting type .+ \\(expected .+\\)")))
      VARIABLE_TYPE_CONFLICT
    else if (msg.matches(semanticError("Cannot use the same relationship variable '.+' for multiple patterns")))
      RELATIONSHIP_UNIQUENESS_VIOLATION
    else if (msg.matches(semanticError("Cannot use the same relationship identifier '.+' for multiple patterns")))
      RELATIONSHIP_UNIQUENESS_VIOLATION
    else if (msg.matches(semanticError("Variable length relationships cannot be used in ((CREATE)|(MERGE))")))
      CREATING_VAR_LENGTH
    else if (msg.matches(semanticError("Parameter maps cannot be used in ((MATCH)|(MERGE)) patterns \\(use a literal map instead, eg. \"\\{id: \\{param\\}\\.id\\}\"\\)")))
      INVALID_PARAMETER_USE
    else if (msg.matches(semanticError("Variable `.+` already declared")))
      VARIABLE_ALREADY_BOUND
    else if (msg.matches(semanticError("MATCH cannot follow OPTIONAL MATCH \\(perhaps use a WITH clause between them\\)")))
      INVALID_CLAUSE_COMPOSITION
    else if (msg.matches(semanticError("Invalid combination of UNION and UNION ALL")))
      INVALID_CLAUSE_COMPOSITION
    else if (msg.matches(semanticError("floating point number is too large")))
      FLOATING_POINT_OVERFLOW
    else if (msg.matches(semanticError("Argument to exists\\(\\.\\.\\.\\) is not a property or pattern")))
      INVALID_ARGUMENT_EXPRESSION
    else if (msg.startsWith("Invalid input '—':"))
      INVALID_UNICODE_CHARACTER
    else if (msg.matches(semanticError("Can't use aggregating expressions inside of expressions executing over lists")))
      INVALID_AGGREGATION
    else if (msg.matches(semanticError("Can't use aggregating expressions inside of expressions executing over collections")))
      INVALID_AGGREGATION
    else if (msg.matches(semanticError("It is not allowed to refer to variables in ((SKIP)|(LIMIT))")))
      NON_CONSTANT_EXPRESSION
    else if (msg.matches(semanticError("It is not allowed to refer to identifiers in ((SKIP)|(LIMIT))")))
      NON_CONSTANT_EXPRESSION
    else if (msg.matches("Can't use non-deterministic \\(random\\) functions inside of aggregate functions\\."))
      NON_CONSTANT_EXPRESSION
    else if (msg.matches(semanticError("A single relationship type must be specified for ((CREATE)|(MERGE))\\")) ||
      msg.matches(semanticError("Exactly one relationship type must be specified for ((CREATE)|(MERGE))\\. " +
        "Did you forget to prefix your relationship type with a \\'\\:\\'\\?")))
      NO_SINGLE_RELATIONSHIP_TYPE
    else if (msg.matches(s"${DOTALL}Invalid input '.*': expected an identifier character, whitespace, '\\|', a length specification, a property map or '\\]' \\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*"))
      INVALID_RELATIONSHIP_PATTERN
    else if (msg.matches(s"${DOTALL}Invalid input '.*': expected whitespace, RangeLiteral, a property map or '\\]' \\(line \\d+, column \\d+ \\(offset: \\d+\\)\\).*"))
      INVALID_RELATIONSHIP_PATTERN
    else if (msg.matches(semanticError("invalid literal number")))
      INVALID_NUMBER_LITERAL
    else if (msg.matches(semanticError("Unknown function '.+'")))
      UNKNOWN_FUNCTION
    else if (msg.matches(semanticError("Invalid input '.+': expected four hexadecimal digits specifying a unicode character")))
      INVALID_UNICODE_LITERAL
    else if (msg.matches("Cannot merge ((relationship)|(node)) using null property value for .+"))
      MERGE_READ_OWN_WRITES
    else if (msg.matches(semanticError("Invalid use of aggregating function count\\(\\.\\.\\.\\) in this context")))
      INVALID_AGGREGATION
    else if (msg.matches(semanticError("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding ((RETURN)|(WITH))")))
      INVALID_AGGREGATION
    else if (msg.matches(semanticError("Expression in WITH must be aliased \\(use AS\\)")))
      NO_EXPRESSION_ALIAS
    else if (msg.matches(semanticError("All sub queries in an UNION must have the same column names")))
      DIFFERENT_COLUMNS_IN_UNION
    else if (msg.matches(semanticError("DELETE doesn't support removing labels from a node. Try REMOVE.")))
      INVALID_DELETE
    else if (msg.matches("Property values can only be of primitive types or arrays thereof"))
      INVALID_PROPERTY_TYPE
    else if (msg.matches(semanticError("Multiple result columns with the same name are not supported")))
      COLUMN_NAME_CONFLICT
    else if (msg.matches(semanticError("RETURN \\* is not allowed when there are no variables in scope")))
      NO_VARIABLES_IN_SCOPE
    else if (msg.matches(semanticError("RETURN \\* is not allowed when there are no identifiers in scope")))
      NO_VARIABLES_IN_SCOPE
    else if (msg.matches(semanticError("Procedure call does not provide the required number of arguments.+")))
      "InvalidNumberOfArguments"
    else if (msg.matches("Expected a parameter named .+"))
      "MissingParameter"
    else if (msg.startsWith("Procedure call cannot take an aggregating function as argument, please add a 'WITH' to your statement."))
      "InvalidAggregation"
    else if (msg.startsWith("Procedure call inside a query does not support passing arguments implicitly (pass explicitly after procedure name instead)"))
      "InvalidArgumentPassingMode"
    else if (msg.matches("There is no procedure with the name `.+` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed."))
      "ProcedureNotFound"
    else
      msg
  }

  private val DOTALL = "(?s)"

  private val POSITION_PATTERN = " \\(line .+, column .+ \\(offset: .+\\)\\).*"

  private def semanticError(pattern: String): String = DOTALL + pattern + POSITION_PATTERN

}
