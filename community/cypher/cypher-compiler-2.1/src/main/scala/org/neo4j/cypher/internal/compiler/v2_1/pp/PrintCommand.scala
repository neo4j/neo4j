package org.neo4j.cypher.internal.compiler.v2_1.pp

sealed abstract class PrintCommand
case class PrintText(value: String) extends PrintCommand
case class PrintNewLine(indent: Int) extends PrintCommand




