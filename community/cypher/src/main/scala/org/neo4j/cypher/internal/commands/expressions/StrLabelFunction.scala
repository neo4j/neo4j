package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.CypherTypeException

/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
case class StrLabelFunction(labelStr: Expression) extends NullInNullOutExpression(labelStr) {

  def compute(value: Any, m: ExecutionContext) = value match {
    case s: String => LabelValue(s)
    case _ => throw new CypherTypeException("Cannot convert value that is not a string to a label")
  }

  def rewrite(f: (Expression) => Expression) = f(StrLabelFunction(labelStr.rewrite(f)))

  def children = Seq(labelStr)

  def identifierDependencies(expectedType: CypherType) = null

  def calculateType(symbols: SymbolTable) = {
    labelStr.evaluateType(StringType(), symbols)
    LabelType()
  }

  def symbolTableDependencies = labelStr.symbolTableDependencies
}