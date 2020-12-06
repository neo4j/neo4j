/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.shell.test.bolt;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.ServerInfo;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.shell.test.Util;

/**
 * A fake result summary
 */
class FakeResultSummary implements ResultSummary
{
    @Override
    public Statement statement()
    {
        throw new Util.NotImplementedYetException( "Not implemented yet" );
    }

    @Override
    public SummaryCounters counters()
    {
        return InternalSummaryCounters.EMPTY_STATS;
    }

    @Override
    public StatementType statementType()
    {
        throw new Util.NotImplementedYetException( "Not implemented yet" );
    }

    @Override
    public boolean hasPlan()
    {
        throw new Util.NotImplementedYetException( "Not implemented yet" );
    }

    @Override
    public boolean hasProfile()
    {
        throw new Util.NotImplementedYetException( "Not implemented yet" );
    }

    @Override
    public Plan plan()
    {
        throw new Util.NotImplementedYetException( "Not implemented yet" );
    }

    @Override
    public ProfiledPlan profile()
    {
        throw new Util.NotImplementedYetException( "Not implemented yet" );
    }

    @Override
    public List<Notification> notifications()
    {
        throw new Util.NotImplementedYetException( "Not implemented yet" );
    }

    @Override
    public long resultAvailableAfter( TimeUnit unit )
    {
        return 0;
    }

    @Override
    public long resultConsumedAfter( TimeUnit unit )
    {
        return 0;
    }

    @Override
    public ServerInfo server()
    {
        return new ServerInfo()
        {
            @Override
            public String address()
            {
                throw new Util.NotImplementedYetException( "Not implemented yet" );
            }

            @Override
            public String version()
            {
                return null;
            }
        };
    }
}
