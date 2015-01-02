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
package org.neo4j.consistency.store.windowpool;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

public enum WindowPoolImplementation
{
    MOST_FREQUENTLY_USED
    {
        @Override
        public WindowPoolFactory windowPoolFactory( Config config, StringLogger logger )
        {
            return new DefaultWindowPoolFactory();
        }
    },
    SCAN_RESISTANT
    {
        @Override
        public WindowPoolFactory windowPoolFactory( Config config, StringLogger logger )
        {
            return new ScanResistantWindowPoolFactory( config, logger );
        }
    };

    public abstract WindowPoolFactory windowPoolFactory( Config config, StringLogger logger );
}
