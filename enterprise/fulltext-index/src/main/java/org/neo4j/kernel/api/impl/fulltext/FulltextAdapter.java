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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.storageengine.api.EntityType;

public interface FulltextAdapter
{
    String FIELD_ENTITY_ID = "__lucene__fulltext__addon__internal__id__";

    Stream<String> propertyKeyStrings( IndexDescriptor descriptor );

    IndexDescriptor indexDescriptorFor( String name, EntityType type, String[] entityTokens, String... properties ) throws InvalidArgumentsException;

    ScoreEntityIterator query( String indexName, String queryString ) throws IOException, IndexNotFoundKernelException, ParseException;
}
