/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapping;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappings;

public enum IdType
{
    /**
     * Used when node ids int input data are any string identifier.
     */
    STRING( IdMappings.strings( LongArrayFactory.AUTO ) )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.string();
        }
    },

    /**
     * Used when node ids int input data are any integer identifier. It uses 8b longs for storage,
     * but as a user facing enum a better name is integer
     */
    INTEGER( IdMappings.longs( LongArrayFactory.AUTO ) )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.long_();
        }
    },

    /**
     * Used when node ids int input data are specified as long values and points to actual record ids.
     * ADVANCED usage. Performance advantage, but requires carefully planned input data.
     */
    ACTUAL( IdMappings.actual() )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.long_();
        }
    };

    private final IdMapping idMapping;

    private IdType( IdMapping idMapping )
    {
        this.idMapping = idMapping;
    }

    public IdMapping idMapping()
    {
        return idMapping;
    }

    public abstract Extractor<?> extractor( Extractors extractors );
}
