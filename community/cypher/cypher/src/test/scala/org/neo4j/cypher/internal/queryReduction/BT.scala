/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.queryReduction.BT.BTConfiguration
import org.neo4j.cypher.internal.queryReduction.DDmin.Oracle


object BT {
  type BTConfiguration = Array[Int]

  def apply[I, O](input: BTInput[I, O])(test: Oracle[I]): I = {
    // Create initial configuration
    val conf: BTConfiguration = new Array[Int](input.length)

    var improvementFound = false
    do {
      improvementFound = false

      // Try to maximize the variables one by one
      for (i <- 0 until input.domains.length) {

        var currentAssignment: Int = conf(i);
        // Check each assignment that is right of the current assignment (by starting with the last one)
        var j = input.domains(i).length - 1
        while(j > currentAssignment) {
          // Only check the assignment if its gain is better
          if (input.domains(i).assignments(j).gain > input.domains(i).assignments(currentAssignment).gain) {
            // Assign that value
            conf(i) = j
            // Test the current assignment
            val result = test(input.toInput(conf))

            if (result == Reproduced) {
              // If it is valid, we mark that an improvement was found
              improvementFound = true
              // Save the new current assignment
              currentAssignment = conf(i)

              // See if new assignments became available
              val newAssignments = input.getNewAssignments(input.domains(i).assignments(j))
              if (!newAssignments.isEmpty) {
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

  def length = domains.length

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
  def length = assignments.length
}

case class BTAssignment[O](obj: O, gain: Int)
