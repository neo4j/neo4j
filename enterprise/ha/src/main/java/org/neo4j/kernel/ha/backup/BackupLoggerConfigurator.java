/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.backup;


import java.net.URL;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.spi.Configurator;
import org.apache.log4j.spi.LoggerRepository;

/**
 * Simple log4j configurator that ignores the URL and just logs ERROR to
 * System.err
 * Useful for HA backups where Zookeeper needs a logger but we don't want to
 * depend on externally provided configuration.
 */
public class BackupLoggerConfigurator implements Configurator
{
    // TODO: We've removed ZK. Is this still needed?
    @Override
    public void doConfigure( URL url, LoggerRepository repository )
    {
        repository.getRootLogger().setLevel( Level.ERROR );
        repository.getRootLogger().addAppender(
                new ConsoleAppender( new SimpleLayout(), "System.err" ) );
    }
}
