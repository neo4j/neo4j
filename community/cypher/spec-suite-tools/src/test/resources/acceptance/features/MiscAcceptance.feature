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

Feature: MiscAcceptance

  Scenario: Github issue #13165
    Given an empty graph
    And having executed:
      """
      CREATE (n0:channel {id: "channel::0"})
      CREATE (n1:channel_state {id: "channel_state::0"})
      CREATE (n2:channel {id: "channel::1"})
      CREATE (n3:channel_state {id: "channel_state::1"})
      CREATE (n4:channel {id: "channel::2"})
      CREATE (n5:channel_state {id: "channel_state::2"})
      CREATE (n6:channel {id: "channel::3"})
      CREATE (n7:channel_state {id: "channel_state::3"})
      CREATE (n8:channel {id: "channel::4"})
      CREATE (n9:channel_state {id: "channel_state::4"})
      CREATE (n10:channel {id: "channel::5"})
      CREATE (n11:channel_state {id: "channel_state::5"})
      CREATE (n12:channel {id: "channel::6"})
      CREATE (n13:channel_state {id: "channel_state::6"})
      CREATE (n14:channel {id: "channel::7"})
      CREATE (n15:channel_state {id: "channel_state::7"})
      CREATE (n16:user {id: "user::1"})
      CREATE (n17:user_state {id: "user_state::1"})
      CREATE (n0)-[:METADATA_STATE {id: 'metastate::1', to: datetime('+275760-09-13T00:00Z')}]->(n1)
      CREATE (n2)-[:METADATA_STATE {id: 'metastate::2', to: datetime('+275760-09-13T00:00Z')}]->(n3)
      CREATE (n0)-[:BENEFICIARY {id: 'beneficiary::1', to: datetime('+275760-09-13T00:00Z')}]->(n2)
      CREATE (n4)-[:METADATA_STATE {id: 'metastate::3', to: datetime('+275760-09-13T00:00Z')}]->(n5)
      CREATE (n2)-[:BENEFICIARY {id: 'beneficiary::2', to: datetime('+275760-09-13T00:00Z')}]->(n4)
      CREATE (n6)-[:METADATA_STATE {id: 'metastate::4', to: datetime('+275760-09-13T00:00Z')}]->(n7)
      CREATE (n4)-[:BENEFICIARY {id: 'beneficiary::3', to: datetime('+275760-09-13T00:00Z')}]->(n6)
      CREATE (n8)-[:METADATA_STATE {id: 'metastate::5', to: datetime('+275760-09-13T00:00Z')}]->(n9)
      CREATE (n6)-[:BENEFICIARY {id: 'beneficiary::4', to: datetime('+275760-09-13T00:00Z')}]->(n8)
      CREATE (n10)-[:METADATA_STATE {id: 'metastate::6', to: datetime('+275760-09-13T00:00Z')}]->(n11)
      CREATE (n8)-[:BENEFICIARY {id: 'beneficiary::5', to: datetime('+275760-09-13T00:00Z')}]->(n10)
      CREATE (n12)-[:METADATA_STATE {id: 'metastate::7', to: datetime('+275760-09-13T00:00Z')}]->(n13)
      CREATE (n10)-[:BENEFICIARY {id: 'beneficiary::6', to: datetime('+275760-09-13T00:00Z')}]->(n12)
      CREATE (n14)-[:METADATA_STATE {id: 'metastate::8', to: datetime('+275760-09-13T00:00Z')}]->(n15)
      CREATE (n12)-[:BENEFICIARY {id: 'beneficiary::7', to: datetime('2023-04-19T08:58:51.105Z')}]->(n14)
      CREATE (n16)-[:METADATA_STATE {id: 'metastate::9', to: datetime('+275760-09-13T00:00Z')}]->(n17)
      CREATE (n14)-[:SETTLEMENT {id: 'settlement::1', to: datetime('+275760-09-13T00:00Z')}]->(n16)
      """
    When executing query:
      """
      MATCH p=({id: 'channel::0'})-[:BENEFICIARY|SETTLEMENT*]->(n)
      WHERE all(r in relationships(p) WHERE r.to = datetime({ epochMillis: 8640000000000000 }))
      WITH *, last(relationships(p)) as edge
      MATCH (n)-[me:METADATA_STATE]->(mn)
      WHERE me.to = datetime({ epochMillis: 8640000000000000 })
      OPTIONAL MATCH (pn)-[edge]->(n)
      RETURN pn.id, n.id, edge.id, me.id, mn.id
      """
    Then the result should be, in any order:
      | pn.id        | n.id         | edge.id          | me.id          | mn.id              |
      | 'channel::0' | 'channel::1' | 'beneficiary::1' | 'metastate::2' | 'channel_state::1' |
      | 'channel::1' | 'channel::2' | 'beneficiary::2' | 'metastate::3' | 'channel_state::2' |
      | 'channel::2' | 'channel::3' | 'beneficiary::3' | 'metastate::4' | 'channel_state::3' |
      | 'channel::3' | 'channel::4' | 'beneficiary::4' | 'metastate::5' | 'channel_state::4' |
      | 'channel::4' | 'channel::5' | 'beneficiary::5' | 'metastate::6' | 'channel_state::5' |
      | 'channel::5' | 'channel::6' | 'beneficiary::6' | 'metastate::7' | 'channel_state::6' |
    And no side effects

  Scenario: GitHub Issue #13169
    Given an empty graph
    When executing query:
      """
      CALL {
        MERGE ()
      }
      RETURN null AS n0
      UNION ALL
      MATCH ()
      MATCH ()<-[:((!A&B)&(C|D))]-()
      RETURN null AS n0
      """
    Then the result should be, in order:
      | n0   |
      | null |
    And the side effects should be:
      | +nodes      | 1 |

  Scenario: GitHub Issue #13169 variant
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:B]->(), ()-[:C]->()
      """
    When executing query:
      """
      CALL {
        MERGE ()
      }
      RETURN null AS n0
      UNION ALL
      MATCH ()
      MATCH ()<-[:((!A&B)&(C|D))]-()
      RETURN null AS n0
      """
    Then the result should be, in order:
      | n0   |
      | null |
    And no side effects

  Scenario: GitHub Issue #13190
    Given an empty graph
    When executing query:
      """
      RETURN [()-[]-()|1][count{()}] AS result
      """
    Then the result should be, in order:
      | result   |
      | null     |
    And no side effects

  Scenario: GitHub Issue #13190 variant
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:B]->()
      """
    When executing query:
      """
      RETURN [()-[]-()|2][count{()} - 2] AS result
      """
    Then the result should be, in order:
      | result |
      | 2      |
    And no side effects

  Scenario: Should not discard re-used variable names
    Given an empty graph
    When executing query:
      """
        UNWIND [1,2,3,4,5] AS a
        WITH a, a*2 AS b
        WITH a
        WITH a, -a AS b
        RETURN a
        ORDER BY b
      """
    Then the result should be, in order:
      | a |
      | 5 |
      | 4 |
      | 3 |
      | 2 |
      | 1 |
    And no side effects

    Scenario: Github issue number 13432 query 1
      Given an empty graph
      When executing query:
      """
        RETURN (CASE toBoolean(all(n0 IN [1,2] WHERE n0 < 0)) WHEN false THEN 0 END) AS r
      """
      Then the result should be, in order:
        | r |
        | 0 |
      And no side effects

  Scenario: Github issue number 13432 query 2
    Given an empty graph
    When executing query:
      """
        RETURN toBoolean(all(n0 IN [1, 2] WHERE n0 > 0)) AS r
      """
    Then the result should be, in order:
      | r     |
      | true |
    And no side effects

  Scenario: Github issue number 13432 query 3
    Given an empty graph
    When executing query:
      """
        RETURN toBoolean(all(n0 IN [1, 2] WHERE n0 > 1)) AS r
      """
    Then the result should be, in order:
      | r     |
      | false |
    And no side effects

  Scenario: Github issue number 13484
    Given an empty graph
    When executing query:
      """
      with *,'a test'  as this
      //===================================================
      call{ with this
          with *, '"' as DQe
          with *, '\'' as SQe
          with *, '\\' as BS1
          with *, '
      ' as LF
          return  SQe, LF, BS1
      }
      return LF+BS1+LF+LF as x
      """
    Then the result should be, in order:
      | x     |
      | '\n\\\\\n\n' |
    And no side effects