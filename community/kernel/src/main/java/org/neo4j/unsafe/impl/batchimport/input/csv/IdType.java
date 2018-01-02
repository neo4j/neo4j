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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;

import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;

/**
 * Defines different types that input ids can come in. Enum names in here are user facing.
 *
 * @see InputNode#id()
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
        public IdMapper idMapper()
        {
            return IdMappers.strings( AUTO );
        }

        @Override
        public IdGenerator idGenerator()
        {
            return IdGenerators.startingFromTheBeginning();
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
        public IdMapper idMapper()
        {
            return IdMappers.longs( AUTO );
        }

        @Override
        public IdGenerator idGenerator()
        {
            return IdGenerators.startingFromTheBeginning();
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
        public IdMapper idMapper()
        {
            return IdMappers.actual();
        }

        @Override
        public IdGenerator idGenerator()
        {
            return IdGenerators.fromInput();
        }
    };

    private final boolean idsAreExternal;

    private IdType( boolean idsAreExternal )
    {
        this.idsAreExternal = idsAreExternal;
    }

    public abstract IdMapper idMapper();

    public abstract IdGenerator idGenerator();

    public boolean idsAreExternal()
    {
        return idsAreExternal;
    }

    public abstract Extractor<?> extractor( Extractors extractors );
}
