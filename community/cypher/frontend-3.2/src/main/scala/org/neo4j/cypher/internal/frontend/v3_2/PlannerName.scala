package org.neo4j.cypher.internal.frontend.v3_2

/**
 * This class defines the query planners used by cyphers.
 **/
trait PlannerName {
  def name: String
  def toTextOutput: String
}
