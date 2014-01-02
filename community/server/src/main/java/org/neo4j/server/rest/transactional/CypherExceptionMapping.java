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
package org.neo4j.server.rest.transactional;

import org.neo4j.cypher.ArithmeticException;
import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.CypherExecutionException;
import org.neo4j.cypher.CypherTypeException;
import org.neo4j.cypher.EntityNotFoundException;
import org.neo4j.cypher.FailedIndexException;
import org.neo4j.cypher.IndexHintException;
import org.neo4j.cypher.InternalException;
import org.neo4j.cypher.InvalidSemanticsException;
import org.neo4j.cypher.LabelScanHintException;
import org.neo4j.cypher.MergeConstraintConflictException;
import org.neo4j.cypher.MissingConstraintException;
import org.neo4j.cypher.MissingIndexException;
import org.neo4j.cypher.NodeStillHasRelationshipsException;
import org.neo4j.cypher.ParameterNotFoundException;
import org.neo4j.cypher.ParameterWrongTypeException;
import org.neo4j.cypher.PatternException;
import org.neo4j.cypher.ProfilerStatisticsNotReadyException;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.UniquePathNotUniqueException;
import org.neo4j.helpers.Function;
import org.neo4j.server.rest.transactional.error.Status;

public class CypherExceptionMapping implements Function<CypherException, Status>
{
    @Override
    public Status apply( CypherException e )
    {
        if ( ParameterNotFoundException.class.isInstance( e ) )
        {
            return Status.Statement.ParameterMissing;
        }
        if ( SyntaxException.class.isInstance( e ) )
        {
            return Status.Statement.InvalidSyntax;
        }
        if ( InternalException.class.isInstance( e ) )
        {
            return Status.Statement.ExecutionFailure;
        }
        if ( CypherExecutionException.class.isInstance( e ) )
        {
            // TODO: map the causing KernelException further...
            return Status.Statement.ExecutionFailure;
        }
        if ( UniquePathNotUniqueException.class.isInstance( e ) )
        {
            return Status.Statement.ConstraintViolation;
        }
        if ( EntityNotFoundException.class.isInstance( e ) )
        {
            return Status.Statement.EntityNotFound;
        }
        if ( CypherTypeException.class.isInstance( e ) )
        {
            return Status.Statement.InvalidType;
        }
        if ( ParameterWrongTypeException.class.isInstance( e ) )
        {
            return Status.Statement.InvalidType;
        }
        if ( PatternException.class.isInstance( e ) )
        {
            return Status.Statement.InvalidSemantics;
        }
        if ( MissingIndexException.class.isInstance( e ) )
        {
            return Status.Schema.NoSuchIndex;
        }
        if ( FailedIndexException.class.isInstance( e ) )
        {
            return Status.General.FailedIndex;
        }
        if ( MissingConstraintException.class.isInstance( e ) )
        {
            return Status.Schema.NoSuchConstraint;
        }
        if ( NodeStillHasRelationshipsException.class.isInstance( e ) )
        {
            return Status.Schema.ConstraintViolation;
        }
        if ( IndexHintException.class.isInstance( e ) )
        {
            return Status.Schema.NoSuchIndex;
        }
        if ( LabelScanHintException.class.isInstance( e ) )
        {
            return Status.Statement.InvalidSemantics;
        }
        if ( InvalidSemanticsException.class.isInstance( e ) )
        {
            return Status.Statement.InvalidSemantics;
        }
        if ( MergeConstraintConflictException.class.isInstance( e ) )
        {
            return Status.Statement.ConstraintViolation;
        }
        if ( ArithmeticException.class.isInstance( e ) )
        {
            return Status.Statement.ArithmeticError;
        }
        if ( ProfilerStatisticsNotReadyException.class.isInstance( e ) )
        {
            return Status.Statement.ExecutionFailure;
        }
        return Status.Statement.ExecutionFailure;
    }
}
