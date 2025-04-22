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

  Scenario: Issue with generated code and value population
    Given an empty graph
    When executing query:
      """
     UNWIND [1, 2, 3] AS row
     MERGE (node:L {id:row})
       ON CREATE SET node.id=row, node.bar = true
     SET node.foo = node.bar
     WITH node WHERE node.bar
     REMOVE node.sendEventStream
     RETURN node
      """
    Then the result should be, in any order:
      | node                               |
      | (:L {bar: true, foo: true, id: 1}) |
      | (:L {bar: true, foo: true, id: 2}) |
      | (:L {bar: true, foo: true, id: 3}) |
    And the side effects should be:
      | +nodes      | 3 |
      | +labels     | 1 |
      | +properties | 9 |


  Scenario: Issue aZDSO5do example 1
    Given an empty graph
    And having executed:
      """
      WITH true AS x
      CREATE (:C:D {id:0, x:x})
      CREATE (:C:D {id:1, x:x})-[:R]->(:Target {id:5})
      CREATE (:C:D {id:2, x:x})-[:R]->(:Target:A {id:6})
      CREATE (:C:D:A:B {id:3, x:x})-[:R]->(:Target:A:B {id:7})
      CREATE (:C:D:A:B {id:4, x:x})-[:R]->(:Target {id:8})
      """
    When executing query:
      """
      MATCH (n)
      WITH [{node: n}] AS nodes
      UNWIND nodes AS map
      WITH map.node AS node
      MATCH (x:C:D {x: (NOT node:A:B)})-->(:Target)
      RETURN *
      ORDER BY node.id, x.id
      """
    Then the result should be, in order:
      | node                  | x                         |
      | (:C:D {x:true, id:0}) | (:C:D {x:true, id:1})     |
      | (:C:D {x:true, id:0}) | (:C:D {x:true, id:2})     |
      | (:C:D {x:true, id:0}) | (:A:B:C:D {x:true, id:3}) |
      | (:C:D {x:true, id:0}) | (:A:B:C:D {x:true, id:4}) |
      | (:C:D {x:true, id:1}) | (:C:D {x:true, id:1})     |
      | (:C:D {x:true, id:1}) | (:C:D {x:true, id:2})     |
      | (:C:D {x:true, id:1}) | (:A:B:C:D {x:true, id:3}) |
      | (:C:D {x:true, id:1}) | (:A:B:C:D {x:true, id:4}) |
      | (:C:D {x:true, id:2}) | (:C:D {x:true, id:1})     |
      | (:C:D {x:true, id:2}) | (:C:D {x:true, id:2})     |
      | (:C:D {x:true, id:2}) | (:A:B:C:D {x:true, id:3}) |
      | (:C:D {x:true, id:2}) | (:A:B:C:D {x:true, id:4}) |
      | (:Target {id:5})      | (:C:D {x:true, id:1})     |
      | (:Target {id:5})      | (:C:D {x:true, id:2})     |
      | (:Target {id:5})      | (:A:B:C:D {x:true, id:3}) |
      | (:Target {id:5})      | (:A:B:C:D {x:true, id:4}) |
      | (:A:Target {id:6})    | (:C:D {x:true, id:1})     |
      | (:A:Target {id:6})    | (:C:D {x:true, id:2})     |
      | (:A:Target {id:6})    | (:A:B:C:D {x:true, id:3}) |
      | (:A:Target {id:6})    | (:A:B:C:D {x:true, id:4}) |
      | (:Target {id:8})      | (:C:D {x:true, id:1})     |
      | (:Target {id:8})      | (:C:D {x:true, id:2})     |
      | (:Target {id:8})      | (:A:B:C:D {x:true, id:3}) |
      | (:Target {id:8})      | (:A:B:C:D {x:true, id:4}) |
    And no side effects


  Scenario: Issue aZDSO5do example 2
    Given an empty graph
    And parameters are:
      | pid           | 'target-pid'                   |
      | tags_for_aggs | ['target-pid', 'target-pid-2'] |
    And having executed:
      """
      // Dummy data.

      CREATE (partition:PolNode {_pid: $pid})<-[:MEMBER_OF]-
             (item:PolNode:Item)-[:DESCRIBES_RESOURCE]->
             (resource:PolNode:Resource:Record {hit_count: 10, is_deleted: false})
      CREATE (item)-[:DESCRIBES_RESOURCE]->
             (resource2:PolNode:TreeNode:Resource {hit_count: 100})
      CREATE (resource)-[parent_rel:TREE_NODE_OF {_creation_date: 7345}]->
             (parent {_pid: 'parentpid'})-[depth_rel:TREE_NODE_OF {depth: 3679}]->
             (grandpa {_pid: 'granpid'})
      CREATE (resource)-[:HAS_TAG]->(t:PolNode:Tag {_pid: 'target-pid', name: 't1'})
      CREATE (resource2)-[:TREE_NODE_OF]->(:PolNode:Resource:Record)-[:HAS_TAG]->
             (:PolNode:Tag {_pid: 'target-pid-2', name: 't2'})
      CREATE (resource2)-[:MATCHES_RULE {hit_count: 23}]->
             (:PolNode:DceRule {is_enabled: true})-[:MEMBER_OF]->
             (:PolNode:DceRuleCategory)
      CREATE (resource)<-[:TREE_NODE_OF]-
             (:PolNode:TreeNode:Resource)-[:MATCHES_RULE {hit_count: 27}]->
             ()-[:MATCHES_RULE]->(:PolNode:DceRule {is_enabled: true})-[:MEMBER_OF]->
             (:PolNode:DceRuleCategory)
      CREATE (resource)<-[:LLM_DESCRIBES_TOPIC]-(:PolNode:Topic {name: 'n1'})
      CREATE (resource)<-[:LLM_DESCRIBES_TOPIC]-()<-[:LLM_DESCRIBES_TOPIC]-
             (:PolNode:Topic {name: 'n2'})
      CREATE (resource)<-[:LLM_DESCRIBES_DATA_SUBJECT]-(:PolNode:Subject {name: 'n3'})
      CREATE (resource)<-[:LLM_DESCRIBES_DATA_SUBJECT]-
             ()<-[:LLM_DESCRIBES_DATA_SUBJECT]-(:PolNode:Subject {name: 'n4'})
      CREATE (resource)<-[:LLM_DESCRIBES_DATA_CLASS]-
             (:PolNode:Metadata {data_class: 'dc1'})
      CREATE (resource)<-[:LLM_DESCRIBES_DATA_CLASS]-()<-[:LLM_DESCRIBES_DATA_CLASS]-
             (:PolNode:Metadata {data_class: 'dc1'})
      CREATE (resource2)<-[:LLM_DESCRIBES_TOPIC]-(:PolNode:Topic {name: 'n5'})
      CREATE (resource2)<-[:LLM_DESCRIBES_TOPIC]-()<-[:LLM_DESCRIBES_TOPIC]-
             (:PolNode:Topic {name: 'n6'})
      CREATE (resource2)<-[:LLM_DESCRIBES_DATA_SUBJECT]-
             (:PolNode:Subject {name: 'n7'})
      CREATE (resource2)<-[:LLM_DESCRIBES_DATA_SUBJECT]-
             ()<-[:LLM_DESCRIBES_DATA_SUBJECT]-(:PolNode:Subject {name: 'n8'})
      CREATE (resource2)<-[:LLM_DESCRIBES_DATA_CLASS]-
             (:PolNode:Metadata {data_class: 'dc1'})
      CREATE (resource2)<-[:LLM_DESCRIBES_DATA_CLASS]-()<-[:LLM_DESCRIBES_DATA_CLASS]-
             (:PolNode:Metadata {data_class: 'dc1'})
      """
    When executing query:
      """
      MATCH (partition:PolNode {_pid: $pid})<-[:MEMBER_OF]-
            (:PolNode:Item)-[:DESCRIBES_RESOURCE]->(resource:PolNode:Resource)
      WITH resource
      WHERE resource.hit_count > 0 AND COALESCE(resource.is_deleted, false) = false
      WITH resource SKIP 0
      LIMIT 10

      CALL {
        WITH resource
        OPTIONAL MATCH (resource)-[parent_rel:TREE_NODE_OF]->
                       (parent)-[depth_rel:TREE_NODE_OF]->(grandpa)
        WHERE coalesce(parent.is_deleted, false) = false
        RETURN parent._pid AS parent_pid_using_creation_date,
               depth_rel.depth AS depth, grandpa._pid AS container_parent_pid
               ORDER BY parent_rel._creation_date DESC
        LIMIT 1
      }

      WITH resource, depth, container_parent_pid,
           coalesce(parent_pid_using_creation_date, resource.container_pid)
           AS parent_pid
      WHERE parent_pid IS NOT NULL
      WITH collect({parent_pid: parent_pid, resource: resource, depth: depth,
                    container_parent_pid: container_parent_pid})
           AS resources_with_parents

      CALL {
        WITH resources_with_parents

        UNWIND resources_with_parents AS resource_with_parent
        WITH resource_with_parent.parent_pid AS parent_pid,
             resource_with_parent.resource AS resource

        CALL {
          WITH resource
          MATCH (resource)-[:HAS_TAG]->(t:PolNode:Tag)
          WHERE t._pid IN $tags_for_aggs
          RETURN t
            UNION
          WITH resource
          MATCH (resource:PolNode:TreeNode:Resource)-[:TREE_NODE_OF]->
                (r:PolNode:Resource:Record)-[:HAS_TAG]->(t:PolNode:Tag)
          WHERE t._pid IN $tags_for_aggs
          RETURN t
        }
        WITH parent_pid, t, count(t) AS tag_count
        WITH parent_pid AS c_pid,
             {name: t.name, id: t._pid, count: tag_count} AS tag_info
        WITH c_pid, collect(tag_info) AS tags_info
        RETURN collect({c_pid: c_pid, tags_info: tags_info})
               AS container_with_tags_info
      }

      CALL {
        WITH resources_with_parents

        UNWIND resources_with_parents AS resource_with_parent
        WITH resource_with_parent.parent_pid AS parent_pid,
             resource_with_parent.resource AS resource

        CALL {
          WITH resource
          WITH resource
          WHERE NOT resource:PolNode:Resource:Record
          MATCH (resource)-[mr:MATCHES_RULE]->
                (rule:PolNode:DceRule {is_enabled: true})
          MATCH (rule)-[:MEMBER_OF]->(cat:PolNode:DceRuleCategory)

          WHERE mr.hit_count > 0
          RETURN cat, rule, mr.hit_count AS resource_rule_hit_count

            UNION
          WITH resource
          WITH resource
          WHERE resource:PolNode:Resource:Record
          MATCH (resource)<-[:TREE_NODE_OF]-
                (:PolNode:TreeNode:Resource)-[mr:MATCHES_RULE]->
                (frc)-[:MATCHES_RULE]->
                (rule:PolNode:DceRule {is_enabled: true})-[:MEMBER_OF]->
                (cat:PolNode:DceRuleCategory)

          WHERE mr.hit_count > 0
          RETURN cat, rule, mr.hit_count AS resource_rule_hit_count
        }

        WITH parent_pid AS c_pid, cat.name AS cat_name, cat._pid AS cat_id,
             rule.db_id AS rule_db_id,
             COLLECT(DISTINCT resource._pid) AS rule_matched_pids,
             SUM(resource_rule_hit_count) AS rule_hit_count

        WITH c_pid, cat_name, cat_id, rule_db_id, rule_matched_pids, rule_hit_count,
             SIZE(rule_matched_pids) AS rule_resources_count

        WITH c_pid, cat_name, cat_id, SUM(rule_hit_count) AS cat_total_hit_count,
             COUNT(DISTINCT reduce(acc = [], item IN rule_matched_pids | acc + item))
             AS cat_count,
             COLLECT({name: rule_db_id, id: rule_db_id, count: rule_resources_count,
                      value: rule_hit_count}) AS rule_aggs

        WITH c_pid, COLLECT({name: cat_name, id: cat_id, value: cat_total_hit_count,
                             count: cat_count, rule_aggs: rule_aggs}) AS dce_info
        RETURN collect({c_pid: c_pid, dce_info: dce_info}) AS container_with_dce_info
      }

      CALL {
        WITH resources_with_parents

        UNWIND resources_with_parents AS resource_with_parent
        WITH resource_with_parent.parent_pid AS parent_pid,
             resource_with_parent.resource AS resource

        OPTIONAL
        MATCH (resource)<-[:LLM_DESCRIBES_TOPIC*1..2]-(llm_topic:PolNode:Topic)
        OPTIONAL MATCH (resource)<-[:LLM_DESCRIBES_DATA_SUBJECT*1..2]-
                       (llm_subject:PolNode:Subject)
        OPTIONAL MATCH (resource)<-[:LLM_DESCRIBES_DATA_CLASS*1..2]-
                       (llm_metadata:PolNode:Metadata)
        OPTIONAL MATCH (resource)<-[describes_locale:LLM_DESCRIBES_LOCALE]-
                       ()<-[:LLM_DESCRIBES_LOCALE*0..1]-(llm_locale:PolNode:Country)

        WITH llm_topic, llm_subject, llm_metadata, llm_locale, describes_locale,
             parent_pid AS c_pid

        WITH c_pid, llm_topic.name AS llm_topic_name,
             COUNT(llm_topic) AS llm_topic_count,
             llm_subject.name AS llm_subject_name,
             COUNT(llm_subject) AS llm_subject_count,
             COALESCE(llm_metadata.manual_override, llm_metadata.data_class)
             AS llm_data_class, COUNT(llm_metadata) AS llm_data_class_count,
             llm_locale.name AS llm_locale_name,
             SUM(COALESCE(describes_locale.count, 1)) AS llm_locale_count
        WITH c_pid, collect({llm_topic_name: llm_topic_name,
                             llm_topic_count: llm_topic_count}) AS llm_topic_agg,
             collect({llm_subject_name: llm_subject_name,
                      llm_subject_count: llm_subject_count}) AS llm_subject_agg,
             collect({llm_data_class: llm_data_class,
                      llm_data_class_count: llm_data_class_count})
             AS llm_data_class_agg,
             COLLECT({llm_locale_name: llm_locale_name,
                      llm_locale_count: llm_locale_count}) AS llm_locale_agg
        WITH c_pid,
             {llm_topic_agg: llm_topic_agg, llm_subject_agg: llm_subject_agg,
              llm_data_class_agg: llm_data_class_agg, llm_locale_agg: llm_locale_agg}
             AS dce_llm_info

        RETURN COLLECT({c_pid: c_pid, dce_llm_info: dce_llm_info})
               AS container_to_dce_llm_info
      }

      UNWIND resources_with_parents AS resource_with_parent
      WITH resource_with_parent.parent_pid AS parent_pid,
           resource_with_parent.resource AS resource,
           resource_with_parent.depth AS depth,
           resource_with_parent.container_parent_pid AS container_parent_pid,
           container_with_tags_info, container_with_dce_info,
           container_to_dce_llm_info

      WITH DISTINCT parent_pid AS c_pid, container_with_tags_info,
                    container_with_dce_info, container_to_dce_llm_info,
                    COLLECT(DISTINCT resource._pid) AS pids,
                    SUM(resource.dce_extracted_size) AS size, depth,
                    container_parent_pid

      RETURN c_pid, reduce(acc = [], item IN container_with_tags_info |
        CASE
          WHEN item.c_pid = c_pid THEN acc + item
          ELSE acc
        END) AS tags_info, reduce(acc = [], item IN container_with_dce_info |
        CASE
          WHEN item.c_pid = c_pid THEN acc + item
          ELSE acc
        END) AS dce_info, reduce(acc = [], item IN container_to_dce_llm_info |
        CASE
          WHEN item.c_pid = c_pid THEN acc + item
          ELSE acc
        END) AS dce_llm_info, pids, size, depth, container_parent_pid
      """
    Then the result should be, in order (ignoring element order for lists):
      | c_pid       | tags_info                                                                     | dce_info                                                                                                                                  | dce_llm_info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | pids | size | depth | container_parent_pid |
      | 'parentpid' | [{c_pid: 'parentpid', tags_info: [{name: 't1', count: 1, id: 'target-pid'}]}] | [{c_pid: 'parentpid', dce_info: [{rule_aggs: [{name: null, count: 0, id: null, value: 27}], name: null, count: 1, id: null, value: 27}]}] | [{c_pid: 'parentpid', dce_llm_info: {llm_data_class_agg: [{llm_data_class_count: 2, llm_data_class: 'dc1'}, {llm_data_class_count: 2, llm_data_class: 'dc1'}, {llm_data_class_count: 2, llm_data_class: 'dc1'}, {llm_data_class_count: 2, llm_data_class: 'dc1'}], llm_topic_agg: [{llm_topic_count: 2, llm_topic_name: 'n1'}, {llm_topic_count: 2, llm_topic_name: 'n2'}, {llm_topic_count: 2, llm_topic_name: 'n1'}, {llm_topic_count: 2, llm_topic_name: 'n2'}], llm_subject_agg: [{llm_subject_name: 'n3', llm_subject_count: 2}, {llm_subject_name: 'n3', llm_subject_count: 2}, {llm_subject_name: 'n4', llm_subject_count: 2}, {llm_subject_name: 'n4', llm_subject_count: 2}], llm_locale_agg: [{llm_locale_count: 2, llm_locale_name: null}, {llm_locale_count: 2, llm_locale_name: null}, {llm_locale_count: 2, llm_locale_name: null}, {llm_locale_count: 2, llm_locale_name: null}]}}] | []   | 0    | 3679  | 'granpid'            |
    And no side effects
