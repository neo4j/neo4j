/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

package org.neo4j.kernel.impl

import org.neo4j.blob.utils.ContextMap
import org.neo4j.blob.utils.ReflectUtils._
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.store.id.RenewableBatchIdSequence
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyRecord}
import org.neo4j.kernel.impl.store.{CommonAbstractStore, StandardDynamicRecordAllocator}
import org.neo4j.kernel.impl.transaction.state.RecordAccess

import scala.collection.mutable.{Map => MMap}

/**
  * Created by bluejoe on 2019/4/16.
  */
object InstanceContext {
  val none: ContextMap = new ContextMap();

  def of(o: AnyRef, path: String): ContextMap = of(o._get(path));

  def of(o: AnyRef): ContextMap = o match {
    case x: StandardDynamicRecordAllocator =>
      of(x._get("idGenerator"));

    case x: CommonAbstractStore[_, _] =>
      x._get("configuration").asInstanceOf[Config].getInstanceContext;

    case x: RecordAccess[PropertyRecord, PrimitiveRecord] =>
      x._get("loader.val$store.configuration").asInstanceOf[Config].getInstanceContext;

    case x: RenewableBatchIdSequence =>
      of(x._get("source"));

    case x: GraphDatabaseFacade =>
      x._get("config").asInstanceOf[Config].getInstanceContext;

    case _ =>
      throw new FaileToGetInstanceContextException(o);
  }

}

class FaileToGetInstanceContextException(o: AnyRef) extends RuntimeException {

}