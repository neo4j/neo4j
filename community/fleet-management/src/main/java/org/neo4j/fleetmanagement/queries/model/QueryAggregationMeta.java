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
package org.neo4j.fleetmanagement.queries.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.HashSet;
import org.neo4j.kernel.api.query.QuerySnapshot;

public class QueryAggregationMeta {
    @JsonPropertyDescription("The number of recorded executions of the query")
    public long count;

    @JsonPropertyDescription("The fastest execution of the query")
    public long minElapsedMs = Long.MAX_VALUE;

    @JsonPropertyDescription("The slowest execution of the query")
    public long maxElapsedMs = Long.MIN_VALUE;

    @JsonPropertyDescription("The earliest execution of the query within this report")
    public long minTimestamp = 0;

    @JsonPropertyDescription("The latest execution of the query within this report")
    public long maxTimestamp = 0;

    private long sumElapsedMs;
    private long sumWaitTimeMicros;
    private long sumPageHits;
    private long sumPageFaults;
    private long sumAllocatedBytes;

    @JsonPropertyDescription("List of databases this query was executed on")
    public HashSet<String> databases = new HashSet<>();

    @JsonPropertyDescription("List of applications from which the query originated")
    public HashSet<String> applications = new HashSet<>();

    @JsonPropertyDescription("List of initiation types of the query")
    public HashSet<String> types = new HashSet<>();

    @JsonPropertyDescription("List of users that executed the query")
    public HashSet<String> users = new HashSet<>();

    public void addFromQuerySnapshot(QuerySnapshot snapshot) {
        long elapsedMs = snapshot.elapsedTimeMicros() / 1000;
        long pageHits = snapshot.pageHits();
        long pageFaults = snapshot.pageFaults();
        long allocatedBytes = snapshot.allocatedBytes();

        count++;
        minElapsedMs = Math.min(minElapsedMs, elapsedMs);
        maxElapsedMs = Math.max(maxElapsedMs, elapsedMs);

        var now = System.currentTimeMillis();
        if (minTimestamp == 0) {
            minTimestamp = now;
        }
        maxTimestamp = Math.max(maxTimestamp, now);

        sumElapsedMs += elapsedMs;
        sumWaitTimeMicros += snapshot.waitTimeMicros();
        sumPageHits += pageHits;
        sumPageFaults += pageFaults;
        sumAllocatedBytes += allocatedBytes;

        if (snapshot.databaseId().isPresent()) {
            databases.add(snapshot.databaseId().get().name());
        }
        if (snapshot.executingUsername() != null
                && !snapshot.executingUsername().isEmpty()) {
            users.add(snapshot.executingUsername());
        } else {
            users.add("anonymous");
        }
        if (snapshot.transactionAnnotationData().containsKey("app")) {
            applications.add(snapshot.transactionAnnotationData().get("app").toString());
        } else {
            applications.add("unspecified");
        }
        if (snapshot.transactionAnnotationData().containsKey("type")) {
            types.add(snapshot.transactionAnnotationData().get("type").toString());
        } else {
            types.add("unspecified");
        }
    }

    @JsonProperty("avgElapsedMs")
    @JsonPropertyDescription("The average duration")
    public double getAvgElapsedMs() {
        return calculateAverage(sumElapsedMs);
    }

    @JsonProperty("avgWaitTimeMs")
    @JsonPropertyDescription("The average wait time")
    public double getAvgWaitTimeMs() {
        return calculateAverage(sumWaitTimeMicros) / 1000;
    }

    @JsonProperty("avgPageHits")
    @JsonPropertyDescription("The average number of page hits")
    public double getAvgPageHits() {
        return calculateAverage(sumPageHits);
    }

    @JsonProperty("avgPageFaults")
    @JsonPropertyDescription("The average number of page faults")
    public double getAvgPageFaults() {
        return calculateAverage(sumPageFaults);
    }

    @JsonProperty("avgAllocatedBytes")
    @JsonPropertyDescription("The average number of allocated bytes")
    public double getAvgAllocatedBytes() {
        return calculateAverage(sumAllocatedBytes);
    }

    private double calculateAverage(long sum) {
        return count == 0 ? 0 : (double) sum / count;
    }
}
