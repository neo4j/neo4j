/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.perftest.enterprise.generator;

enum PropertyGenerator
{
    INTEGER
            {
                @Override
                Object generate()
                {
                    return DataGenerator.RANDOM.nextInt( 16 );
                }
            },
    SINGLE_STRING
            {
                @Override
                Object generate()
                {
                    return name();
                }
            },
    STRING
            {
                @Override
                Object generate()
                {
                    int length = 50 + DataGenerator.RANDOM.nextInt( 70 );
                    StringBuilder result = new StringBuilder( length );
                    for ( int i = 0; i < length; i++ )
                    {
                        result.append( (char) ('a' + DataGenerator.RANDOM.nextInt( 'z' - 'a' )) );
                    }
                    return result.toString();
                }
            },
    BYTE_ARRAY
            {
                @Override
                Object generate()
                {
//                        int length = 4 + RANDOM.nextInt( 60 );
                    int length = 50;
                    int[] array = new int[length];
                    for ( int i = 0; i < length; i++ )
                    {
                        array[i] = DataGenerator.RANDOM.nextInt( 256 );
                    }
                    return array;
                }
            };

    abstract Object generate();
}
