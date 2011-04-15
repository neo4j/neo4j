package org.neo4j.lab.cypher

import util.parsing.combinator.testing.Number

case class Query(select: Select, from: From, where: Option[Where])

case class Select(selectItems: List[SelectItem])

abstract sealed class SelectItem

case class From(fromItems: List[VariableAssignment])

case class VariableAssignment(variable:Variable, fromitem:FromItem)

case class Variable(name:String)


abstract sealed class FromItem

case class NodeById(id:Int*) extends FromItem
case class RelationshipById(id:Int*) extends FromItem
case class NodeByIndex(idxName:String, value:Any) extends FromItem
case class RelationshipByIndex(idxName:String, value:Any) extends FromItem


case class Where(clauses:Clause*)



abstract class Clause {
  def and(otherField: Clause): Clause = And(this, otherField)
  def or(otherField: Clause): Clause = Or(this, otherField)
}

case class StringEquals(f: String, value: String) extends Clause
case class NumberEquals(f: String, value: Number) extends Clause
case class BooleanEquals(f: String, value: Boolean) extends Clause
case class In(field: String, values: String*) extends Clause
case class And(lClause:Clause, rClause:Clause) extends Clause
case class Or(lClause:Clause, rClause:Clause) extends Clause