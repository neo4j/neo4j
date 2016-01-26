/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.proc.Namespace;
import org.neo4j.kernel.impl.proc.ReadOnlyProcedure;
import org.neo4j.kernel.impl.proc.Resource;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

/**
 * This registers procedures that are expected to be available by default in Neo4j.
 */
public class BuiltInProcedures
{
    public static Class[] BUILTINS = new Class[]{ ReadOnlySystemProcedures.class };

    @Namespace({"sys", "db"})
    public static class ReadOnlySystemProcedures
    {
        @Resource
        public Statement statement;

        @ReadOnlyProcedure
        @Description( "Retrieve a list of all labels that are currently in use." )
        public Stream<LabelOutput> labels()
        {
            ResourceIterator<Label> labels = TokenAccess.LABELS.inUse( statement );
            return stream( spliteratorUnknownSize( labels, 0), false ).map( LabelOutput::new );
        }

        @ReadOnlyProcedure
        @Description( "Retrieve a list of all property keys that are currently in use." )
        public Stream<PropertyKeyOutput> propertyKeys()
        {
            ResourceIterator<String> labels = TokenAccess.PROPERTY_KEYS.inUse( statement );
            return stream( spliteratorUnknownSize( labels, 0), false ).map( PropertyKeyOutput::new );
        }

        @ReadOnlyProcedure
        @Description( "Retrieve a list of all relationship types that are currently in use." )
        public Stream<RelationshipTypeOutput> relationshipTypes()
        {
            ResourceIterator<RelationshipType> relTypes = TokenAccess.RELATIONSHIP_TYPES.inUse( statement );
            return stream( spliteratorUnknownSize( relTypes, 0 ), false ).map( RelationshipTypeOutput::new );
        }

        @ReadOnlyProcedure
        @Description( "Retrieve a list of all procedures that are currently registered." )
        public Stream<ProcedureOutput> procedures()
        {
            Set<ProcedureSignature> procedureSignatures = statement.readOperations().proceduresGetAll();
            ArrayList<ProcedureSignature> sorted = new ArrayList<>( procedureSignatures );
            sorted.sort( (a,b) -> a.name().toString().compareTo( b.name().toString() ) );

            return sorted.stream().map( ProcedureOutput::new );
        }

        public static class LabelOutput
        {
            public String label;

            public LabelOutput(Label in)
            {
                label = in.name();
            }
        }

        public static class PropertyKeyOutput
        {
            public String propertyKey;

            public PropertyKeyOutput(String in)
            {
                propertyKey = in;
            }
        }

        public static class RelationshipTypeOutput
        {
            public String relationshipType;

            public RelationshipTypeOutput(RelationshipType in)
            {
                relationshipType = in.name();
            }
        }

        public static class ProcedureOutput
        {
            public String name;
            public String signature;
            public String description;

            public ProcedureOutput(ProcedureSignature in)
            {
                name = in.name().toString();
                signature = in.toString();
                description = in.description();
            }
        }
    }
}
