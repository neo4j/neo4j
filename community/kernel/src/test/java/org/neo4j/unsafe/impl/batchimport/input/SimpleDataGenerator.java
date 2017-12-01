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

import java.util.function.Function;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.IdRangeInput.Range;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputNodeDeserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputRelationshipDeserialization;

public class SimpleDataGenerator extends SourceTraceability.Adapter
{
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final long randomSeed;
    private final long nodeCount;
    private final Distribution<String> labels;
    private final Distribution<String> relationshipTypes;
    private final Groups groups = new Groups();
    private final IdType idType;
    private final String className = getClass().getSimpleName();
    private final float factorNodeDuplicates;
    private final float factorBadRelationships;

    public SimpleDataGenerator( Header nodeHeader, Header relationshipHeader, long randomSeed,
            long nodeCount, int labelCount, int relationshipTypeCount, IdType idType,
            float factorNodeDuplicates, float factorBadRelationships )
    {
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.randomSeed = randomSeed;
        this.nodeCount = nodeCount;
        this.idType = idType;
        this.factorNodeDuplicates = factorNodeDuplicates;
        this.factorBadRelationships = factorBadRelationships;
        this.labels = new Distribution<>( tokens( "Label", labelCount ) );
        this.relationshipTypes = new Distribution<>( tokens( "TYPE", relationshipTypeCount ) );
    }

    public Function<Range,InputNode[]>  nodes()
    {
        return batch -> new SimpleDataGeneratorBatch<>( nodeHeader, batch.getStart(), randomSeed + batch.getStart(),
                nodeCount, labels, relationshipTypes,
                new InputNodeDeserialization( nodeHeader, SimpleDataGenerator.this, groups, idType.idsAreExternal() ),
                new InputNode[batch.getSize()], factorNodeDuplicates, factorBadRelationships ).get();
    }

    public Function<Range,InputRelationship[]> relationships()
    {
        return batch -> new SimpleDataGeneratorBatch<>( relationshipHeader, batch.getStart(),
                randomSeed + batch.getStart(), nodeCount, labels, relationshipTypes,
                new InputRelationshipDeserialization( relationshipHeader, SimpleDataGenerator.this, groups ),
                new InputRelationship[batch.getSize()], factorNodeDuplicates, factorBadRelationships ).get();
    }

    private static String[] tokens( String prefix, int count )
    {
        String[] result = new String[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = prefix + (i + 1);
        }
        return result;
    }

    @Override
    public String sourceDescription()
    {
        return className;
    }
}
