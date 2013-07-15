/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop;

import java.util.List;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.OsSpecificEnvironment;
import org.neo4j.desktop.config.OsSpecificExtensionPackagesConfig;
import org.neo4j.desktop.config.OsSpecificHeapSizeConfig;
import org.neo4j.desktop.config.Value;
import org.neo4j.desktop.runtime.DatabaseActions;
import org.neo4j.desktop.ui.MainWindow;

/**
 * The main class for starting the Neo4j desktop app window. The different components and wired up and started.
 */
public class Neo4jDesktop
{
    private void start()
    {
        Environment environment = new OsSpecificEnvironment().get();
        
        Value<List<String>> extensionPackagesConfig =
                new OsSpecificExtensionPackagesConfig( environment ).get();
        DatabaseActions databaseActions = new DatabaseActions( extensionPackagesConfig );
        Value<Integer> heapSizeConfig = new OsSpecificHeapSizeConfig( environment ).get();
        MainWindow window = new MainWindow( databaseActions, environment, heapSizeConfig,
                extensionPackagesConfig );

        window.display();
    }

    public static void main( String[] args )
    {
        new Neo4jDesktop().start();
    }
}
