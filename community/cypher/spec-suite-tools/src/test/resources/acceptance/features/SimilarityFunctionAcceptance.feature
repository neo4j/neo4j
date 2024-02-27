#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#encoding: utf-8

Feature: SimilarityFunctionAcceptance
  Scenario: Testing cosine with literals of known similarity
    When executing query:
      """
      RETURN vector.similarity.cosine([1.0, 0.0], [0.0, 1.0]) AS floats,
             vector.similarity.cosine([1, 0], [0, 1]) AS integers,
             vector.similarity.cosine([1.0, 0], [0, 1.0]) AS mixed,
             vector.similarity.cosine(NULL, [0, 1.0]) AS leftNull,
             vector.similarity.cosine([1.0, 0], NULL) AS rightNull
      """
    Then the result should be, in any order:
      | floats | integers | mixed | leftNull | rightNull |
      | 0.5    | 0.5      | 0.5   | null     | null      |
    And no side effects

  Scenario: Testing cosine with property values of known similarity
    Given an empty graph
    And having executed:
    """
    CREATE(:Cosine {floatVector: [0.0, 1.0], intVector: [0, 1]})
    """
    When executing query:
      """
      MATCH (n:Cosine)
      RETURN vector.similarity.cosine([1.0, 0.0], n.floatVector) AS floats,
             vector.similarity.cosine([1.0, 0.0], n.intVector) AS integers
      """
    Then the result should be, in any order:
      | floats | integers |
      | 0.5    | 0.5      |
    And no side effects

  Scenario: Testing Euclidean with literals of known similarity
    When executing query:
      """
      RETURN vector.similarity.euclidean([0.0, 2.0], [1.0, 2.0]) AS floats,
             vector.similarity.euclidean([2, 4], [3, 4]) AS integers,
             vector.similarity.euclidean([2.5, 1], [1.5, 1.0]) AS mixed,
             vector.similarity.euclidean(NULL, [1.5, 1.0]) AS leftNull,
             vector.similarity.euclidean([2.5, 1], NULL) AS rightNull
      """
    Then the result should be, in any order:
      | floats | integers | mixed | leftNull | rightNull |
      | 0.5    | 0.5      | 0.5   | null     | null      |
    And no side effects

  Scenario: Testing Euclidean with property values of known similarity
    Given an empty graph
    And having executed:
    """
    CREATE
      (:Euclidean {floatVector: [1.0, 0.0], intVector: [1, 0]})
    """
    When executing query:
      """
      MATCH (n:Euclidean)
      RETURN vector.similarity.euclidean([0.0, 0.0], n.floatVector) AS floats,
             vector.similarity.euclidean([0.0, 0.0], n.intVector) AS integers
      """
    Then the result should be, in any order:
      | floats | integers |
      | 0.5    | 0.5      |
    And no side effects

  Scenario: Testing cosine nearest neighbor search
    Given an empty graph
    And having executed:
    """
    CREATE
      (:Neighbor { id: 1, vector: [1.0, 0.0] }),
      (:Neighbor { id: 2, vector: [-1.0, 0.0] }),
      (:Neighbor { id: 3, vector: [0.0, 1.0] })
    """
    When executing query:
      """
      MATCH (n:Neighbor)
      RETURN n.id AS id,
             vector.similarity.cosine([1.0, 0.0], n.vector) AS similarity
      ORDER BY similarity DESCENDING
      """
    Then the result should be, in order:
      | id | similarity |
      | 1  | 1.0        |
      | 3  | 0.5        |
      | 2  | 0.0        |
    And no side effects

  Scenario: Testing Euclidean nearest neighbor search
    Given an empty graph
    And having executed:
    """
    CREATE
      (:Neighbor { id: 1, vector: [1.0, 0.0] }),
      (:Neighbor { id: 2, vector: [-1.0, 0.0] }),
      (:Neighbor { id: 3, vector: [0.0, 1.0] })
    """
    When executing query:
      """
      MATCH (n:Neighbor)
      WITH n.id AS id,
             vector.similarity.euclidean([1.0, 0.0], n.vector) AS similarity
      ORDER BY similarity DESCENDING
      RETURN id
      """
    Then the result should be, in order:
      | id |
      | 1  |
      | 3  |
      | 2  |
    And no side effects
