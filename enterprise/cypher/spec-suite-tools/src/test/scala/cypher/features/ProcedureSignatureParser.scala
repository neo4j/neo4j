/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package cypher.features

import cypher.features
import org.neo4j.cypher.internal.frontend.v3_4.parser.{Base, Expressions, Literals}
import org.neo4j.cypher.internal.util.v3_4.SyntaxException
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.parboiled.scala._

/**
  * This parses procedure signatures as specified by the Cypher type system and returned
  * by dbms.procedures()
  */
class ProcedureSignatureParser extends Parser with Base with Expressions with Literals {

  @throws(classOf[SyntaxException])
  def parse(signatureText: String): ProcedureSignature = {
    val parsingResults = ReportingParseRunner(ProcedureSignature).run(signatureText.trim)
    parsingResults.result match {
      case Some(signature) =>
        signature
      case None =>
        val errors = parsingResults.parseErrors
        throw new SyntaxException(s"Errors parsing procedure signature: ${errors.mkString(", ")}")
    }
  }

  private def ProcedureSignature: Rule1[ProcedureSignature] = rule("procedure signature") {
    ProcedureSignatureNameParts ~ ProcedureSignatureInputs ~~ "::" ~~ ProcedureSignatureOutputs ~~> {
      (nameParts: Seq[String], inputs: Seq[(String, CypherType)], outputs: Option[Seq[(String, CypherType)]]) =>
        features.ProcedureSignature(nameParts.dropRight(1), nameParts.last, inputs, outputs)
    }
  }

  private def ProcedureSignatureNameParts: Rule1[Seq[String]] = rule("procedure signature name parts") {
    oneOrMore(SymbolicNameString, separator=".")
  }

  private def ProcedureSignatureInputs: Rule1[Seq[(String, CypherType)]] = rule("procedure signature inputs") {
    ProcedureSignatureFields
  }

  private def ProcedureSignatureOutputs: Rule1[Option[Seq[(String, CypherType)]]] = rule("procedure signature outputs") {
    group(ProcedureSignatureFields ~~> { outputs => if (outputs.isEmpty) None else Some(outputs) }) | VoidProcedure
  }

  private def ProcedureSignatureFields: Rule1[Seq[(String, CypherType)]] = rule("procedure signature columns") {
    "(" ~~ zeroOrMore(ProcedureSignatureField ~ WS, separator = "," ~ WS) ~ ")"
  }

  private def ProcedureSignatureField: Rule1[(String, CypherType)] = rule("procedure signature column") {
    group(SymbolicNameString ~~ "::" ~~ ProcedureFieldType) ~~> { (name: String, tpe: CypherType) => name -> tpe }
  }

  private def VoidProcedure: Rule1[Option[Nothing]] = rule {
    "VOID" ~ push(None)
  }

  private def ProcedureFieldType: Rule1[CypherType] = rule("cypher type") {
    group("ANY?" ~ push(CTAny)) |
    group("MAP?" ~ push(CTMap)) |
    group("NODE?" ~ push(CTNode)) |
    group("RELATIONSHIP?" ~ push(CTRelationship)) |
    group("POINT?" ~ push(CTPoint)) |
    group("PATH?" ~ push(CTPath)) |
    group("LIST?" ~~ "OF" ~~ ProcedureFieldType ~~> { (tpe: CypherType) => CTList(tpe) }) |
    group("STRING?" ~ push(CTString)) |
    group("BOOLEAN?" ~ push(CTBoolean)) |
    group("NUMBER?" ~ push(CTNumber)) |
    group("INTEGER?" ~ push(CTInteger)) |
    group("FLOAT?" ~ push(CTFloat))
  }
}





