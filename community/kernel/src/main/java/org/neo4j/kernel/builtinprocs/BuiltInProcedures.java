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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.procedure.Procedure.Mode.READ;

public class BuiltInProcedures
{
    @Context
    public KernelTransaction tx;

    @Procedure(name = "db.labels", mode = READ)
    public Stream<LabelResult> listLabels()
    {
        return TokenAccess.LABELS.inUse( tx.acquireStatement() ).map( LabelResult::new ).stream();
    }

    @Procedure(name = "db.propertyKeys", mode = READ)
    public Stream<PropertyKeyResult> listPropertyKeys()
    {
        return TokenAccess.PROPERTY_KEYS.inUse( tx.acquireStatement() ).map( PropertyKeyResult::new ).stream();
    }

    @Procedure(name = "db.relationshipTypes", mode = READ)
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        return TokenAccess.RELATIONSHIP_TYPES.inUse( tx.acquireStatement() )
                .map( RelationshipTypeResult::new ).stream();
    }

    @Procedure(name = "db.indexes", mode = READ)
    public Stream<IndexResult> listIndexes() throws ProcedureException
    {
        ReadOperations operations = tx.acquireStatement().readOperations();
        TokenNameLookup tokens = new StatementTokenNameLookup( operations );

        List<IndexDescriptor> indexes = asList( operations.indexesGetAll() );
        indexes.sort( ( a, b ) -> a.userDescription( tokens ).compareTo( b.userDescription( tokens ) ) );
        ArrayList<IndexResult> result = new ArrayList<>();
        for ( IndexDescriptor index : indexes )
        {
            try
            {
                result.add( new IndexResult( "INDEX ON " + index.userDescription( tokens ),
                        operations.indexGetState( index ).toString() ) );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new ProcedureException( Status.Schema.IndexNotFound, e,
                        "No index on ", index.userDescription( tokens ) );
            }
        }
        return result.stream();
    }

    @Procedure(name = "db.awaitIndex", mode = READ)
    public void awaitIndex( @Name("label") String labelName,
                            @Name("property") String propertyKeyName,
                            @Name(value = "timeOutSeconds") long timeout ) throws ProcedureException
    {
        new AwaitIndexProcedure( tx ).execute( labelName, propertyKeyName, timeout, TimeUnit.SECONDS );
    }

    @Procedure(name = "db.constraints", mode = READ)
    public Stream<ConstraintResult> listConstraints()
    {
        ReadOperations operations = tx.acquireStatement().readOperations();
        TokenNameLookup tokens = new StatementTokenNameLookup( operations );

        return asList( operations.constraintsGetAll() )
                .stream()
                .map( ( constraint ) -> constraint.userDescription( tokens ) )
                .sorted()
                .map( ConstraintResult::new );
    }

    @Procedure(name = "dbms.procedures", mode = READ)
    public Stream<ProcedureResult> listProcedures()
    {
        return tx.acquireStatement().readOperations().proceduresGetAll()
                .stream()
                .sorted( ( a, b ) -> a.name().toString().compareTo( b.name().toString() ) )
                .map( ProcedureResult::new );
    }

    public class LabelResult
    {
        public final String label;

        private LabelResult( Label label )
        {
            this.label = label.name();
        }
    }

    public class PropertyKeyResult
    {
        public final String propertyKey;

        private PropertyKeyResult( String propertyKey )
        {
            this.propertyKey = propertyKey;
        }
    }

    public class RelationshipTypeResult
    {
        public final String relationshipType;

        private RelationshipTypeResult( RelationshipType relationshipType )
        {
            this.relationshipType = relationshipType.name();
        }
    }

    public class IndexResult
    {
        public final String description;
        public final String state;

        private IndexResult( String description, String state )
        {
            this.description = description;
            this.state = state;
        }
    }

    public class ConstraintResult
    {
        public final String description;

        private ConstraintResult( String description )
        {
            this.description = description;
        }
    }

    public class ProcedureResult
    {
        public final String name;
        public final String signature;

        private ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
        }
    }
}
