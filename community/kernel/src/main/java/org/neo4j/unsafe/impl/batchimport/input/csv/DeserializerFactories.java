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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputGroupsDeserializer.DeserializerFactory;

/**
 * Common {@link DeserializerFactory} implementations.
 */
public class DeserializerFactories
{
    public static DeserializerFactory<InputNode> defaultNodeDeserializer(
            Groups groups, Configuration config, IdType idType, Collector badCollector )
    {
        return (header,stream,decorator,validator) ->
        {
            InputNodeDeserialization deserialization =
                    new InputNodeDeserialization( header, stream, groups, idType.idsAreExternal() );
            return new InputEntityDeserializer<>( header, stream, config.delimiter(),
                    deserialization, decorator, validator, badCollector );
        };
    }

    public static DeserializerFactory<InputRelationship> defaultRelationshipDeserializer(
            Groups groups, Configuration config, IdType idType, Collector badCollector )
    {
        return (header,stream,decorator,validator) ->
        {
                InputRelationshipDeserialization deserialization =
                        new InputRelationshipDeserialization( header, stream, groups );
                return new InputEntityDeserializer<>( header, stream, config.delimiter(),
                        deserialization, decorator, validator, badCollector );
        };
    }
}
