#
# Copyright 2016 "Neo Technology",
# Network Engine for Objects in Lund AB (http://neotechnology.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

Feature: OrderByAcceptance

  Background:
    Given any graph

  Scenario: ORDER BY nodes should return null results last in ascending order
    And having executed:
      """
      CREATE (:A)-[:REL]->(:B),
             (:A)
      """
    When executing query:
      """
      MATCH (a:A)
      OPTIONAL MATCH (a)-[:REL]->(b:B)
      RETURN b
      ORDER BY b
      """
    Then the result should be, in order:
      | b    |
      | (:B) |
      | null |
    And no side effects
