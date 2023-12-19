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
package org.neo4j.cypher.internal.queryReduction

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

sealed trait OracleResult

// Bug reproduced
case object Reproduced extends OracleResult

// Bug gone
case object NotReproduced extends OracleResult

object DDmin {
  type Oracle[I] = (I => OracleResult)

  private val cache = mutable.Map[Seq[Int], OracleResult]()

  def apply[I](input: DDInput[I])(test: Oracle[I]): I = {
    cache.clear()
    ddmin2(input, 2, test).getCurrentCode
  }

  private def ddmin2[I](input: DDInput[I], n: Int, test: Oracle[I]): DDInput[I] = {
    def runWithCache: OracleResult = {
      val key = input.activeTokens
      if (cache.contains(key)) {
        cache(key)
      } else {
        // No cached value available
        val triedCode = Try {
          input.getCurrentCode
        }

        val res = triedCode match {
          case Success(code) => test(code)
          case Failure(_) => NotReproduced
        }
        // Cache the result
        cache(key) = res
        res
      }
    }

    val originalLength = input.length
    if (originalLength <= 1) {
      // No further minimization possible
      return input
    }

    // Set the granularity on the input
    input.setGranularity(n)

    // Try reducing to subset
    for (i <- 0 until n) {
      input.setSubset(i)
      val result = runWithCache

      if (result == Reproduced && input.length < originalLength) {
        // Subset is smaller
        return ddmin2(input, 2, test)
      } else {
        input.unset()
      }
    }

    // Try reducing to complement
    for (i <- 0 until n) {
      // Obtain subset
      input.setComplement(i)
      val result = runWithCache

      if (result == Reproduced && input.length < originalLength) {
        // Complement is smaller
        return ddmin2(input, Math.max(n - 1, 2), test)
      } else {
        input.unset()
      }
    }

    if (n < originalLength) {
      // Increase granularity
      return ddmin2(input, Math.min(originalLength, 2 * n), test)
    }

    // Otherwise done
    input
  }
}

abstract class DDInput[I](originalLength : Int) {
  val allTokens : Array[Int] = (0 until originalLength).toArray
  // Initially all tokens are active
  var activeTokens : Array[Int] = allTokens
  var chunks : mutable.Buffer[Array[Int]] = _

  def setGranularity(n : Int): Unit = {
    // The maximum size a chunk can have
    val maxChunkSize = Math.ceil(length.toDouble / n).toInt
    // Number of chunks with the maximum length
    var maxLengthChunks = n
    // Some chunks must be shorter if there is no clean division
    if(length % n != 0) {
      maxLengthChunks = length % n
    }

    // Split the activeTokens into n chunks
    var index = 0
    chunks = mutable.Buffer()
    for (i <- 0 until n) {
      var chunkSize = if (i < maxLengthChunks)  maxChunkSize else maxChunkSize - 1
      val chunk = activeTokens.slice(index, index + chunkSize)
      chunks.append(chunk)
      index += chunkSize
    }
  }

  private def getComplementChunks(num: Int) = {
    val complement : Array[Int] = new Array(length - chunks(num).length)
    var index = 0
    for (i <- chunks.indices) {
      // Skip the num-th entry
      if(i != num) {
        for (j <- chunks(i).indices) {
          complement(index) = chunks(i)(j)
          index = index + 1
        }
      }
    }
    complement
  }

  def setSubset(num: Int): Unit = {
    activeTokens = chunks(num)
  }

  def setComplement(num: Int): Unit = {
    activeTokens = getComplementChunks(num)
  }

  def unset(): Unit = {
    activeTokens = chunks.flatten.toArray
  }

  def length: Int = activeTokens.length

  @throws(classOf[IllegalSyntaxException])
  def getCurrentCode : I
}

class IllegalSyntaxException(message: String = "") extends Exception(message)
