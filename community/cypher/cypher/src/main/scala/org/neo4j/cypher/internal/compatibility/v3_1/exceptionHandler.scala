package org.neo4j.cypher.internal.compatibility.v3_1

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compatibility.{ExceptionHandler, _}
import org.neo4j.cypher.internal.frontend.v3_1.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.frontend.v3_1.{CypherException => InternalCypherException}

object exceptionHandler extends MapToPublicExceptions[CypherException] {
  def syntaxException(message: String, query: String, offset: Option[Int], cause: Throwable) = new SyntaxException(message, query, offset, cause)

  def arithmeticException(message: String, cause: Throwable) = new ArithmeticException(message, cause)

  def profilerStatisticsNotReadyException(cause: Throwable) = {
    throw new ProfilerStatisticsNotReadyException(cause)
  }

  def incomparableValuesException(lhs: String, rhs: String, cause: Throwable) = new IncomparableValuesException(lhs, rhs, cause)


  def patternException(message: String, cause: Throwable) = new PatternException(message, cause)

  def invalidArgumentException(message: String, cause: Throwable) = new InvalidArgumentException(message, cause)

  def mergeConstraintConflictException(message: String, cause: Throwable) = new MergeConstraintConflictException(message, cause)

  def internalException(message: String, cause: Exception) = new InternalException(message, cause)

  def loadCsvStatusWrapCypherException(extraInfo: String, cause: InternalCypherException) =
    new LoadCsvStatusWrapCypherException(extraInfo, cause.mapToPublic(exceptionHandler))

  def loadExternalResourceException(message: String, cause: Throwable) = throw new LoadExternalResourceException(message, cause)

  def parameterNotFoundException(message: String, cause: Throwable) = throw new ParameterNotFoundException(message, cause)

  def uniquePathNotUniqueException(message: String, cause: Throwable) = throw new UniquePathNotUniqueException(message, cause)

  def entityNotFoundException(message: String, cause: Throwable) = throw new EntityNotFoundException(message, cause)


  def cypherTypeException(message: String, cause: Throwable) = throw new CypherTypeException(message, cause)

  def cypherExecutionException(message: String, cause: Throwable) = throw new CypherExecutionException(message, cause)

  override def shortestPathFallbackDisableRuntimeException(message: String, cause: Throwable): CypherException =
    throw new ExhaustiveShortestPathForbiddenException(message, cause)

  def invalidSemanticException(message: String, cause: Throwable) = throw new InvalidSemanticsException(message, cause)

  def parameterWrongTypeException(message: String, cause: Throwable) = throw new ParameterWrongTypeException(message, cause)

  def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable) = throw new NodeStillHasRelationshipsException(nodeId, cause)

  def indexHintException(variable: String, label: String, property: String, message: String, cause: Throwable) =
    throw new IndexHintException(variable, label, property, message, cause)

  def joinHintException(variable: String, message: String, cause: Throwable) = throw new JoinHintException(variable, message, cause)

  def periodicCommitInOpenTransactionException(cause: Throwable) = throw new PeriodicCommitInOpenTransactionException(cause)

  def failedIndexException(indexName: String, cause: Throwable): CypherException = throw new FailedIndexException(indexName, cause)

  object runSafely extends RunSafely {
    override def apply[T](body: => T)(implicit f: ExceptionHandler = ExceptionHandler.default) = {
      try {
        body
      }
      catch {
        case e: InternalCypherException =>
          f(e)
          throw e.mapToPublic(exceptionHandler)
        case e: Throwable =>
          f(e)
          throw e
      }
    }
  }
}
