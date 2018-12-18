/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.internal;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.collector.DataCollectorModule;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DataCollectorManager extends LifecycleAdapter
{
    private final DatabaseManager databaseManager;
    private final Procedures procedures;
    private final Config config;
    private final List<AutoCloseable> dataCollectors;

    public DataCollectorManager( DatabaseManager databaseManager,
                                 Procedures procedures, Config config )
    {
        this.databaseManager = databaseManager;
        this.procedures = procedures;
        this.config = config;
        this.dataCollectors = new ArrayList<>();
    }

    @Override
    public void start() throws Throwable
    {
        dataCollectors.add( DataCollectorModule.setupDataCollector( procedures, databaseManager, config ) );
    }

    @Override
    public void stop() throws Throwable
    {
        try
        {
            IOUtils.closeAll( dataCollectors );
        }
        finally
        {
            dataCollectors.clear();
        }
    }
}
