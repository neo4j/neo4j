/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.File;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;

import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

public class Inputs
{
    public static Input input(
            final InputIterator nodes, final InputIterator relationships,
            final IdMapper idMapper, final IdGenerator idGenerator, final Collector badCollector )
    {
        return new Input()
        {
            @Override
            public InputIterator relationships()
            {
                return relationships;
            }

            @Override
            public InputIterator nodes()
            {
                return nodes;
            }

            @Override
            public IdMapper idMapper()
            {
                return idMapper;
            }

            @Override
            public IdGenerator idGenerator()
            {
                return idGenerator;
            }

            @Override
            public Collector badCollector()
            {
                return badCollector;
            }
        };
    }

    public static Input csv( File nodes, File relationships, IdType idType,
            Configuration configuration, Collector badCollector )
    {
        return new CsvInput(
                datas( data( NO_DECORATOR, defaultCharset(), nodes ) ),
                defaultFormatNodeFileHeader(),
                datas( data( NO_DECORATOR, defaultCharset(), relationships ) ),
                defaultFormatRelationshipFileHeader(), idType, configuration,
                badCollector );
    }
}
