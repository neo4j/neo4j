/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.query;

import java.util.List;

import javax.annotation.Nonnull;

public class PlannerInfo
{
    private final String planner;
    private final String runtime;
    private final List<IndexUsage> indexes;

    public PlannerInfo( @Nonnull String planner, @Nonnull String runtime, @Nonnull List<IndexUsage> indexes )
    {
        this.planner = planner;
        this.runtime = runtime;
        this.indexes = indexes;
    }

    public String planner()
    {
        return planner.toLowerCase();
    }

    public String runtime()
    {
        return runtime.toLowerCase();
    }

    public List<IndexUsage> indexes()
    {
        return indexes;
    }
}
