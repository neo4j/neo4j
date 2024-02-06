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
package org.neo4j.fabric.util

import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.semantics.FeatureError
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.planning.Use
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

/**
 * The errors in this file are in 2 categories: "Open Cypher" and "Neo4j".
 * <p>
 * The main feature of "Neo4j" errors is that they have status codes. Any error without a status code that occurs during cypher statement execution
 * and makes to Bolt server will be mapped to a generic [[org.neo4j.kernel.api.exceptions.Status.Statement.ExecutionFailed]] that is an equivalent of HTTP 500
 * and indicates a generic failure on the server side.
 * <p>
 * "Open Cypher" errors can be used, but they need to be mapped to "Neo4j" ones unless "Statement Execution Failed" is a desirable outcome.
 */
object Errors {

  trait HasErrors extends Throwable {
    def update(upd: SemanticErrorDef => SemanticErrorDef): HasErrors
  }

  case class EvaluationFailedException(errors: Seq[SemanticErrorDef]) extends RuntimeException(
        s"Evaluation failed\n${errors.map(e => s"- ${e.msg} [at ${e.position}]").mkString("\n")}"
      ) with HasErrors {
    override def update(upd: SemanticErrorDef => SemanticErrorDef): EvaluationFailedException = copy(errors.map(upd))
  }
  def openCypherSemantic(msg: String, node: ASTNode): SemanticError = SemanticError(msg, node.position)

  def openCypherFailure(errors: Seq[SemanticErrorDef]): Nothing = throw EvaluationFailedException(errors)

  def openCypherFailure(error: SemanticErrorDef): Nothing = openCypherFailure(Seq(error))

  def wrongType(exp: String, got: String): Nothing =
    throw new CypherTypeException(s"Wrong type. Expected $exp, got $got")

  def wrongArity(exp: Int, got: Int): Nothing =
    syntax(s"Wrong arity. Expected $exp argument(s), got $got argument(s)")

  def syntax(msg: String): Nothing = throw new SyntaxException(msg)

  def syntax(msg: String, query: String, pos: InputPosition): Nothing =
    throw new SyntaxException(msg, query, pos.offset)

  def semantic(message: String) = throw new InvalidSemanticsException(message)

  def entityNotFound(kind: String, needle: String): Nothing =
    throw new EntityNotFoundException(s"$kind not found: $needle")

  /** Attaches position and query info to exceptions, if it is missing */
  def errorContext[T](query: String, node: ASTNode)(block: => T): T =
    try block
    catch {
      case e: HasErrors => throw e.update {
          case SemanticError(msg, InputPosition.NONE)   => syntax(msg, query, node.position)
          case SemanticError(msg, pos)                  => syntax(msg, query, pos)
          case FeatureError(msg, _, InputPosition.NONE) => syntax(msg, query, node.position)
          case FeatureError(msg, _, pos)                => syntax(msg, query, pos)
          case o                                        => o
        }
    }

  def show(n: CatalogName): String = n.parts.mkString(".")

  def show(av: AnyValue): String = av match {
    case v: Value => v.prettyPrint()
    case x        => x.getTypeName
  }

  def show(a: Catalog.Arg[_]) = s"${a.name}: ${a.tpe.getSimpleName}"

  def show(seq: Seq[_]): String = seq.map {
    case v: AnyValue       => show(v)
    case a: Catalog.Arg[_] => show(a)
  }.mkString(",")
}
