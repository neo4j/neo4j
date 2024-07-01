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
package org.neo4j.shell.test.bolt;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.Query;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.GqlStatusObject;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ServerInfo;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.shell.test.Util;

/**
 * A fake result summary
 */
class FakeResultSummary implements ResultSummary {
    @Override
    public Query query() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public SummaryCounters counters() {
        return InternalSummaryCounters.EMPTY_STATS;
    }

    @Override
    public QueryType queryType() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public boolean hasPlan() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public boolean hasProfile() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public Plan plan() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public ProfiledPlan profile() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public List<Notification> notifications() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public Set<GqlStatusObject> gqlStatusObjects() {
        throw new Util.NotImplementedYetException("Not implemented yet");
    }

    @Override
    public long resultAvailableAfter(TimeUnit unit) {
        return 0;
    }

    @Override
    public long resultConsumedAfter(TimeUnit unit) {
        return 0;
    }

    @Override
    public ServerInfo server() {
        return new ServerInfo() {
            @Override
            public String address() {
                throw new Util.NotImplementedYetException("Not implemented yet");
            }

            @Override
            public String protocolVersion() {
                return null;
            }

            @Override
            public String agent() {
                return null;
            }
        };
    }

    @Override
    public DatabaseInfo database() {
        return new DatabaseInfo() {
            @Override
            public String name() {
                return null;
            }
        };
    }
}
