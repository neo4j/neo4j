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
package org.neo4j.internal.collector.extension;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.internal.collector.DataCollector;
import org.neo4j.internal.collector.RecentQueryBuffer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.extension.ExtensionType.DATABASE;

@ServiceProvider
public class DataCollectorExtensionFactory extends ExtensionFactory<DataCollectorExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Monitors monitors();

        JobScheduler jobScheduler();

        Database database();

        Config config();

        RecentQueryBuffer recentQueryBuffer();
    }

    public DataCollectorExtensionFactory()
    {
        super( DATABASE, "dataCollector" );
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
    {
        return new DataCollector( dependencies.database(),
                                  dependencies.jobScheduler(),
                                  dependencies.monitors(),
                                  dependencies.config(),
                                  dependencies.recentQueryBuffer() );
    }
}
