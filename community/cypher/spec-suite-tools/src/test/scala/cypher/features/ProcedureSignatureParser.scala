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

import cypher.features
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.SyntaxException

/**
 * This parses procedure signatures as specified by the Cypher type system and returned
 * by SHOW PROCEDURES
 */
class ProcedureSignatureParser {

  @throws(classOf[SyntaxException])
  def parse(signatureText: String): ProcedureSignature = {
    val signature = signatureText.trim

    // Extract everything before the first parentheses = the full procedure name
    val parts = signature.split("(?=\\()", 2)
    if (parts.size < 2) {
      throw new SyntaxException("Error parsing procedure signature: expected '(' after procedure name")
    }

    // Split full procedure name into namespace and name by splitting on '.'
    val nameParts = parts.head.split("\\.")

    // Split procedure input and output fields. They should be on the format (input) :: (output)
    val (inputPart, outputPart) = extractInputAndOutputParts(parts.last)

    val inputs = extractProcedureInputFields(inputPart)
    val outputs = extractProcedureOutputFields(outputPart)

    features.ProcedureSignature(nameParts.dropRight(1), nameParts.last, inputs, outputs)
  }

  private def extractInputAndOutputParts(inputAndOutputString: String): (String, String) = {

    // Check that the first and last char are parenthesis
    if (inputAndOutputString.head != '(' || inputAndOutputString.last != ')') {
      throw new SyntaxException(
        "Error parsing procedure signature: expected input fields to be on the format '(input) :: (output)'"
      )
    }

    // Check there is exactly one ') :: (' and split on it
    val inputAndOutput = inputAndOutputString.split("\\) :: \\(")
    if (inputAndOutput.size != 2) {
      throw new SyntaxException(
        "Error parsing procedure signature: expected exactly one ') :: (' between input and output."
      )
    }

    // Remove the first and last parenthesis
    (inputAndOutput.head.drop(1), inputAndOutput.last.dropRight(1))
  }

  private def extractProcedureInputFields(inputString: String): Seq[(String, CypherType)] = {
    if (inputString.isEmpty) {
      Seq()
    } else {
      // Check that the rest is a comma-separated list
      val inputFields = inputString.split(",")

      // Check that each field in the list has valid format
      inputFields.map(x => extractProcedureField(x)).toSeq
    }
  }

  private def extractProcedureOutputFields(outputString: String): Option[Seq[(String, CypherType)]] = {
    if (outputString.isEmpty || outputString.trim.equals("VOID")) {
      None
    } else {
      // Check that the rest is a comma-separated list
      val outputFields = outputString.split(",")

      // Check that each field in the list has valid format
      Some(outputFields.map(x => extractProcedureField(x)).toSeq)
    }
  }

  private def extractProcedureField(procedureField: String): (String, CypherType) = {
    // Expected format 'name :: type'
    val fieldParts = procedureField.split("::")

    if (fieldParts.size != 2) {
      throw new SyntaxException(
        "Error parsing procedure signature: expected exactly one '::' between procedure field parts."
      )
    }

    val fieldName = fieldParts.head.trim
    val cypherType = extractCypherType(fieldParts.last.trim)
    (fieldName, cypherType)
  }

  private def extractCypherType(typeString: String): CypherType = {
    typeString match {
      case "ANY?"                         => CTAny
      case "MAP?"                         => CTMap
      case "NODE?"                        => CTNode
      case "RELATIONSHIP?"                => CTRelationship
      case "POINT?"                       => CTPoint
      case "PATH?"                        => CTPath
      case "STRING?"                      => CTString
      case "BOOLEAN?"                     => CTBoolean
      case "NUMBER?"                      => CTNumber
      case "INTEGER?"                     => CTInteger
      case "FLOAT?"                       => CTFloat
      case s if s.startsWith("LIST? OF ") => CTList(extractCypherType(s.stripPrefix("LIST? OF ")))
      case unexpected =>
        throw new SyntaxException(s"Error parsing procedure signature: unexpected Cypher type $unexpected")
    }
  }
}
