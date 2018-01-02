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
package org.neo4j.consistency.statistics;

/**
 * Top level interface for managing statistics. The statistics are for human eyes, and so there's basically
 * only a {@link #print(String)} method.
 */
public interface Statistics
{
    void print( String name );

    void reset();

    Counts getCounts();

    Statistics NONE = new Statistics()
    {
        @Override
        public void reset()
        {
        }

        @Override
        public void print( String name )
        {
        }

        @Override
        public Counts getCounts()
        {
            return Counts.NONE;
        }
    };
}
