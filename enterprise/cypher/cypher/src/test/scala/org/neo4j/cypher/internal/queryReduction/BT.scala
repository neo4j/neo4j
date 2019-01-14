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
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.queryReduction.BT.BTConfiguration
import org.neo4j.cypher.internal.queryReduction.DDmin.Oracle

import scala.collection.mutable

object BT {
  type BTConfiguration = Array[Int]

  private val cache = mutable.Map[Seq[Int], OracleResult]()

  def apply[I, O](input: BTInput[I, O])(test: Oracle[I]): I = {
    // Create initial configuration
    cache.clear()
    val conf: BTConfiguration = new Array[Int](input.length)

    def runWithCache: OracleResult = {
      val key = conf.clone()
      if (cache.contains(key)) {
        cache(key)
      } else {
        // No cached value available
        val res = test(input.toInput(conf))
        // Cache the result
        cache(key) = res
        res
      }
    }

    var improvementFound = false
    do {
      improvementFound = false

      // Try to maximize the variables one by one
      for (i <- input.domains.indices) {

        var currentAssignment: Int = conf(i)
        // Check each assignment that is right of the current assignment (by starting with the last one)
        var j = input.domains(i).length - 1
        while(j > currentAssignment) {
          // Only check the assignment if its gain is better
          if (input.domains(i).assignments(j).gain > input.domains(i).assignments(currentAssignment).gain) {
            // Assign that value
            conf(i) = j
            // Test the current assignment
            val result = runWithCache

            if (result == Reproduced) {
              // If it is valid, we mark that an improvement was found
              improvementFound = true
              // Save the new current assignment
              currentAssignment = conf(i)

              // See if new assignments became available
              val newAssignments = input.getNewAssignments(input.domains(i).assignments(j))
              if (newAssignments.nonEmpty) {
                // Insert the new assignments after the last entry
                input.domains(i).assignments = input.domains(i).assignments ++ newAssignments

                // We have to start over to test the new assignments
                // Set to after last element, so that after the loop decrement it will point to
                // the last of the newly inserted elements
                j = input.domains(i).length
              }
            } else {
              // If not, we revert to the original assignment
              conf(i) = currentAssignment
            }
          }
          j = j - 1
        }
      }
    } while (improvementFound)

    // Return the final assignment
    input.toInput(conf)
  }


}

abstract class BTInput[I, O] {

  val domains: Array[BTDomain[O]]

  def length: Int = domains.length

  def toObjects(config: BTConfiguration): Seq[O] = {
    domains.zipWithIndex.map{
      case (dom, index) => dom.assignments(config(index)).obj
    }
  }

  def toInput(config: BTConfiguration): I = {
    convertToInput(toObjects(config))
  }

  def convertToInput(objects: Seq[O]): I

  def getNewAssignments(assignment: BTAssignment[O]) : Seq[BTAssignment[O]]

}

class BTDomain[O](var assignments: Array[BTAssignment[O]]) {
  def length: Int = assignments.length
}

case class BTAssignment[O](obj: O, gain: Int)
