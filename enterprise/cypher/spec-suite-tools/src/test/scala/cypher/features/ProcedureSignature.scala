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

import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType

case class ProcedureSignature(namespace: Seq[String],
                              name: String,
                              inputs: Seq[(String, CypherType)],
                              outputs: Option[Seq[(String, CypherType)]]) {

  val fields: List[String] =
    (inputs ++ outputs.getOrElse(Seq.empty)).map { case (fieldName, _) => fieldName }.toList
}
