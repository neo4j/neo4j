/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package cypher.cucumber

import java.util

import cucumber.api.DataTable

import scala.collection.JavaConverters._
import scala.collection.mutable

object DataTableConverter {

  implicit class RighDataTable(dataTable: DataTable) {

    def asScala[T](implicit manifest: Manifest[T]): List[mutable.Map[String, T]] =
      toList[T].map(_.asScala)

    def toList[T](implicit manifest: Manifest[T]): List[util.Map[String, T]] =
      dataTable.asMaps(classOf[String], manifest.runtimeClass.asInstanceOf[Class[T]]).asScala.toList
  }

}
