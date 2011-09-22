/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rrd.sampler;

import org.neo4j.server.database.Database;
import org.rrd4j.DsType;

public class RequestMeanTimeSampleable extends StatisticSampleableBase
{

    public RequestMeanTimeSampleable( Database db )
    {
        super( db, DsType.ABSOLUTE );
    }

    @Override
    public String getName()
    {
        return "request_mean_time";
    }

    @Override
    public double getValue()
    {
        return getCurrentSnapshot().getDuration().getAvg();
    }
}
