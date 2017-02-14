package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, InternalException, PlannerName, SemanticState, SemanticTable}

trait BaseState {
  def queryText: String
  def startPosition: Option[InputPosition]
  def plannerName: PlannerName
  def maybeStatement: Option[Statement]
  def maybeSemantics: Option[SemanticState]
  def maybeExtractedParams: Option[Map[String, Any]]
  def maybeSemanticTable: Option[SemanticTable]

  def accumulatedConditions: Set[Condition]

  def isPeriodicCommit(): Boolean = statement() match {
    case Query(Some(_), _) => true
    case _ => false
  }

  def statement(): Statement = maybeStatement getOrElse fail("Statement")
  def semantics(): SemanticState = maybeSemantics getOrElse fail("Semantics")
  def extractedParams(): Map[String, Any] = maybeExtractedParams getOrElse fail("Extracted parameters")
  def semanticTable(): SemanticTable = maybeSemanticTable getOrElse fail("Semantic table")

  protected def fail(what: String) = {
    throw new InternalException(s"$what not yet initialised")
  }
}
