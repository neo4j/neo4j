/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

class Issue13981ArrayCopyRegressionTest extends ExecutionEngineFunSuite {

  private val createData =
    """
      |CREATE
      |(n0:L7 {k47:2000000000000000000})<-[r0:T4]-(n1:L5 {k35:1900420387549331023}),
      |(n2:L1 {k9:'a'})-[r1:T21 {k178:-4866941474805577387}]->(n3:L2:L3 {k14:1687814637493126242,k13:'sRKup'}),
      |(n4:L5:L0 {k4:901107332415277137,k33:-9075383130849221876})-[r2:T27]->(n5:L0:L6 {k0:'34l2iAhX',k4:1}),
      |(n6:L7)-[r3:T0 {k61:'iBoU'}]->(n5),
      |(n6)-[r4:T30 {k230:'nUkabn',k235:'z'}]->(n7 {id:14})-[r5:T26]->(n8:L0 {k5:-8326898867956399889}),
      |(n10:L1 {k8:-2207813004658417566})-[r6:T21]->(n9:L3 {k17:'CpArTRG'}),
      |(n12:L6:L1 {k9:'sOMcxy'})-[r7:T8]->(n11:L3:L6 {k42:true,k22:'0XV3IA'});
      |""".stripMargin

  private val matchQuery =
    """
      |MATCH (n0:L7)<-[r0:T4]-(n1:L5 {k35:1900420387549331023}), (n2:L1)-[r1:T21]->(n3:L2:L3 {k14:1687814637493126242}), (n4:L5:L0 {k4:901107332415277137})-[r2:T27]-(n5:L0:L6 {k0:'34l2iAhX'})<-[r3:T0]-(n6:L7)-[r4:T30]-(n7)-[r5:T26]-(n8:L0 {k5:-8326898867956399889}), (n9:L3 {k17:'CpArTRG'})<-[r6:T21]-(n10:L1 {k8:-2207813004658417566}), (n11:L3:L6)<-[r7:T8]-(n12:L6:L1 {k9:'sOMcxy'})
      |WHERE (((((n11.k42) = true) OR ((n5.k4) = (n5.k4))) AND (true)) AND (((((((((((((((((n0.k47) > 1029174302659070504) AND (true)) AND ((true) AND ((n2.k9) < 'y1njDYZ'))) AND (((true) AND ((r1.k178) = -4866941474805577387)) AND (((r1.k178) >= -4866941474805577388) AND (true)))) AND ((n3.k13) ENDS WITH 'sRKup')) AND (((n4.k33) >= -9075383130849221878) AND ((n4.k33) <= -9075383130849221874))) AND ((r3.k61) CONTAINS 'iBoU')) AND ((((r4.k230) CONTAINS 'nUkabn') AND (((r4.k235) > 'djqOA') AND (true))) AND (true))) AND ((n7.id) = 14)) AND ((((((true) AND (true)) AND ((true) AND (true))) AND ((true) AND (true))) AND (true)) AND ((true) AND (true)))) AND ((true) AND (true))) AND (true)) AND (true)) AND ((n11.k22) STARTS WITH '0XV3IA')) AND ((true) AND (true))) AND (true)))
      |OPTIONAL MATCH (n9:L3)<-[r8:T21]-(n10), (n11)<-[r9:T21]-(n13:L1), (n0:L7)-[r10:T4]-(n1), (n9)<-[r11:T0]-(n14 {k9:'m3EHd8'})-[r12:T26]-(n10 {id:12}), (n9 {k21:'J0pmtwc'})-[r13:T30]-(n7)-[r14:T26]-(n15:L7 {k44:true})-[r15:T5]-(n2 {k10:7684302091931604040})-[r16:T7]-(n8)<-[r17:T32]-(n1 {k33:-6993538653649411471})-[r18:T12]-(n16:L4 {k26:7179354984526712181})-[r19:T12]-(n10 {k10:-8992639014810996456})
      |WHERE true
      |RETURN replace('0St0nTF5O', '1HllxS9m', 'hl0O96DB') AS a0
      |ORDER BY a0 DESC
      |""".stripMargin

  test(
    "multi-component MATCH followed by OPTIONAL MATCH should not throw arraycopy internal error (Cypher 25, slotted runtime)"
  ) {
    execute(createData)
    val result = execute("CYPHER 25 runtime=slotted " + matchQuery)
    result.single("a0") should equal("0St0nTF5O")
  }

  test(
    "multi-component MATCH followed by OPTIONAL MATCH should not throw arraycopy internal error (Cypher 5, slotted runtime)"
  ) {
    execute(createData)
    val result = execute("CYPHER 5 runtime=slotted " + matchQuery)
    result.single("a0") should equal("0St0nTF5O")
  }
}
