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
package org.neo4j.blob

import java.util.Properties

import eu.medsea.mimeutil.MimeUtil
import org.apache.commons.io.IOUtils

import scala.collection.JavaConversions._

/**
  * Created by bluejoe on 2019/4/18.
  */
object MimeTypeFactory {
  MimeUtil.registerMimeDetector("eu.medsea.mimeutil.detector.MagicMimeMimeDetector");

  val properties = new Properties();
  properties.load(this.getClass.getClassLoader.getResourceAsStream("mime.properties"));
  val code2Types = properties.map(x => (x._1.toLong, x._2.toLowerCase())).toMap;
  val type2Codes = code2Types.map(x => (x._2, x._1)).toMap;

  def fromText(text: String): MimeType = {
    val lc = text.toLowerCase();
    new MimeType(type2Codes.get(lc).getOrElse(throw new UnknownMimeTypeException(lc)), lc);
  }

  def fromCode(code: Long) = new MimeType(code, code2Types(code));

  def guessMimeType(iss: InputStreamSource): MimeType = {
    val mimeTypes = iss.offerStream { is =>
      MimeUtil.getMimeTypes(IOUtils.toByteArray(is))
    }.toList;

    mimeTypes.headOption.map(mt => fromText(mt.toString)).getOrElse(fromCode(-1));
  }
}

class UnknownMimeTypeException(mtype: String) extends RuntimeException(s"unknown mime-type: $mtype") {

}