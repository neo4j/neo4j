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
package org.neo4j.shell.prettyprint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.driver.internal.summary.InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.summary.InternalDatabaseInfo;
import org.neo4j.driver.internal.summary.InternalResultSummary;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;

class OutputFormatterTest {
    @Test
    void shouldReportTotalDBHits() {
        Value labelScan = buildOperator("NodeByLabelScan", 1002L, 1001L, null);
        Value filter = buildOperator("Filter", 1402, 280, labelScan);
        Value planMap = buildOperator("ProduceResults", 0, 280, filter);

        ProfiledPlan plan = PROFILED_PLAN_FROM_VALUE.apply(planMap);
        ResultSummary summary = new InternalResultSummary(
                new Query("PROFILE MATCH (n:LABEL) WHERE 20 < n.age < 35 return n"),
                new InternalServerInfo("agent", new BoltServerAddress("localhost:7687"), BoltProtocolV5.VERSION),
                new InternalDatabaseInfo("neo4j"),
                QueryType.READ_ONLY,
                null,
                plan,
                plan,
                Collections.emptyList(),
                Collections.emptySet(),
                39,
                55);

        // When
        Map<String, Value> info = OutputFormatter.info(summary);

        // Then
        assertThat(info.get("DbHits").asLong()).isEqualTo(2404L);
    }

    private static Value buildOperator(String operator, long dbHits, long rows, Value child) {
        Map<String, Value> operatorMap = new HashMap<>();
        operatorMap.put("operatorType", Values.value(operator));
        operatorMap.put("dbHits", Values.value(dbHits));
        operatorMap.put("rows", Values.value(rows));
        if (child != null) {
            operatorMap.put("children", new ListValue(child));
        }
        return new MapValue(operatorMap);
    }
}
