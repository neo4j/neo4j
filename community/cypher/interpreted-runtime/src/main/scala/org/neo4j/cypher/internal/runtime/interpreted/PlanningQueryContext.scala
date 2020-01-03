/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.v3_5.logical.plans.QualifiedName
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.AnyValue
import org.neo4j.cypher.internal.v3_5.util.InternalException

/**
  * In planning we need the ability to evaluate some expressions, e.g. LIMIT org.foo.computeLimit(). In order to
  * do that we need some functionality of a full QueryContext. This class provides that.
  */
case class PlanningQueryContext(transaction: TransactionalContext) extends BaseQueryContext {

  override def notSupported(): Nothing = throw new InternalException("Operation not supported during planning")

  override def callFunction(id: Int, args: Seq[AnyValue],
                            allowed: Array[String]): AnyValue = CallSupport.callFunction(transaction, id, args, allowed)


  override def callFunction(name: QualifiedName,
                            args: Seq[AnyValue],
                            allowed: Array[String]): AnyValue = CallSupport.callFunction(transaction, name, args, allowed)
}
