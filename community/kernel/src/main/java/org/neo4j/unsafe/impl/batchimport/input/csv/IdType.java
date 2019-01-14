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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Groups;

/**
 * Defines different types that input ids can come in. Enum names in here are user facing.
 *
 * @see Header.Entry#extractor()
 */
public enum IdType
{
    /**
     * Used when node ids int input data are any string identifier.
     */
    STRING( true )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.string();
        }

        @Override
        public IdMapper idMapper( NumberArrayFactory numberArrayFactory, Groups groups )
        {
            return IdMappers.strings( numberArrayFactory, groups );
        }
    },

    /**
     * Used when node ids int input data are any integer identifier. It uses 8b longs for storage,
     * but as a user facing enum a better name is integer
     */
    INTEGER( true )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.long_();
        }

        @Override
        public IdMapper idMapper( NumberArrayFactory numberArrayFactory, Groups groups )
        {
            return IdMappers.longs( numberArrayFactory, groups );
        }
    },

    /**
     * Used when node ids int input data are specified as long values and points to actual record ids.
     * ADVANCED usage. Performance advantage, but requires carefully planned input data.
     */
    ACTUAL( false )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.long_();
        }

        @Override
        public IdMapper idMapper( NumberArrayFactory numberArrayFactory, Groups groups )
        {
            return IdMappers.actual();
        }
    };

    private final boolean idsAreExternal;

    IdType( boolean idsAreExternal )
    {
        this.idsAreExternal = idsAreExternal;
    }

    public abstract IdMapper idMapper( NumberArrayFactory numberArrayFactory, Groups groups );

    public boolean idsAreExternal()
    {
        return idsAreExternal;
    }

    public abstract Extractor<?> extractor( Extractors extractors );
}
