/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.functions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.neo4j.cypher.internal.v3_4.{functions => astFunctions}

object functionConverter {

  def apply(fcn: ast.FunctionInvocation, callback: ast.Expression => CodeGenExpression)
           (implicit context: CodeGenContext): CodeGenExpression = fcn.function match {

    // id(n)
    case astFunctions.Id =>
      assert(fcn.args.size == 1)
      IdCodeGenFunction(callback(fcn.args(0)))

    // type(r)
    case astFunctions.Type =>
      assert(fcn.args.size == 1)
      TypeCodeGenFunction(callback(fcn.args(0)))

    case other => throw new CantCompileQueryException(s"Function $other not yet supported")
  }
}



