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

package org.neo4j.blob.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}

/**
  * Created by bluejoe on 2018/11/3.
  */

object StreamUtils {
  def covertLong2ByteArray(value: Long): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    outputStream2Ex(baos).writeLong(value);
    baos.toByteArray;
  }

  def convertLongArray2ByteArray(values: Array[Long]): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    values.foreach(outputStream2Ex(baos).writeLong(_));
    baos.toByteArray;
  }

  def convertByteArray2LongArray(value: Array[Byte]): Array[Long] = {
    val baos = inputStream2Ex(new ByteArrayInputStream(value));
    (1 to value.length / 8).map(x => baos.readLong()).toArray
  }

  def convertByteArray2Long(value: Array[Byte]): Long = {
    val baos = inputStream2Ex(new ByteArrayInputStream(value));
    baos.readLong()
  }

  implicit def inputStream2Ex(is: InputStream) = new InputStreamEx(is);

  implicit def outputStream2Ex(os: OutputStream) = new OutputStreamEx(os);
}

class InputStreamEx(is: InputStream) {

  def readLong(): Long = {
    val bytes = readBytes(8);

    val longValue = 0L |
      ((bytes(0) & 0xff).toLong << 56) |
      ((bytes(1) & 0xff).toLong << 48) |
      ((bytes(2) & 0xff).toLong << 40) |
      ((bytes(3) & 0xff).toLong << 32) |
      ((bytes(4) & 0xff).toLong << 24) |
      ((bytes(5) & 0xff).toLong << 16) |
      ((bytes(6) & 0xff).toLong << 8) |
      ((bytes(7) & 0xff).toLong << 0);

    longValue;
  }

  def readBytes(n: Int): Array[Byte] = {
    val bytes: Array[Byte] = new Array[Byte](n).map(x => 0.toByte);
    val nread = is.read(bytes);

    if (nread != n)
      throw new InsufficientBytesException(n, nread);

    bytes;
  }
}

class OutputStreamEx(os: OutputStream) {
  def writeLong(value: Long): Unit = {
    val bytes = Array(
      (value >> 56).toByte,
      (value >> 48).toByte,
      (value >> 40).toByte,
      (value >> 32).toByte,
      (value >> 24).toByte,
      (value >> 16).toByte,
      (value >> 8).toByte,
      (value >> 0).toByte
    );

    os.write(bytes);
  }
}

class InsufficientBytesException(expected: Int, actual: Int) extends
  RuntimeException(s"required $expected bytes, actual $actual") {
}