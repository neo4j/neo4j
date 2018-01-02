/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.database;

import java.io.IOException;

import org.rrd4j.core.RrdDb;

/**
 * This provider exists because {@link RrdDb} is inflexible w/ regards to different
 * file system abstractions and {@link RrdDb} being a class makes it hard to override
 * closing it where the backing file should be deleted in, say an ephemeral environment.
 */
public interface RrdDbWrapper
{
    RrdDb get();
    
    void close() throws IOException;
    
    static class Plain implements RrdDbWrapper
    {
        private final RrdDb db;

        public Plain( RrdDb db )
        {
            this.db = db;
        }
        
        @Override
        public RrdDb get()
        {
            return db;
        }

        @Override
        public void close() throws IOException
        {
            db.close();
        }
    }
}
