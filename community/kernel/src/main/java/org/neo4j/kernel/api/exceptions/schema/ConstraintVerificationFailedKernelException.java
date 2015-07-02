/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.exceptions.schema;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.index.IndexEntryConflictException;

/**
 * Constraint verification happens when a new constraint is created, and the database verifies that existing
 * data adheres to the new constraint.
 *
 * @see ConstraintViolationKernelException
 */
public class ConstraintVerificationFailedKernelException extends KernelException
{
    private final PropertyConstraint constraint;

    public static abstract class Evidence
    {
        Evidence()
        {
        }

        abstract String message( String label, String propertyKey );

        public static Evidence of( IndexEntryConflictException conflict )
        {
            return new IndexEntryConflict( conflict );
        }

        public static Evidence ofNodeWithNullProperty( long nodeId )
        {
            return new MandatoryConstraintConflict( nodeId );
        }
    }

    private final Set<Evidence> evidence;

    public ConstraintVerificationFailedKernelException( PropertyConstraint constraint, Evidence evidence )
    {
        this( constraint, Collections.singleton( evidence ) );
    }

    public ConstraintVerificationFailedKernelException( PropertyConstraint constraint, Set<Evidence> evidence )
    {
        super( Status.Schema.ConstraintVerificationFailure, "Existing data does not satisfy %s.", constraint );
        this.constraint = constraint;
        this.evidence = evidence;
    }

    public ConstraintVerificationFailedKernelException( PropertyConstraint constraint, Throwable failure )
    {
        super( Status.Schema.ConstraintVerificationFailure, failure, "Failed to verify constraint %s: %s", constraint,
                failure.getMessage() );
        this.constraint = constraint;
        this.evidence = null;
    }

    public Set<Evidence> evidence()
    {
        return evidence == null ? Collections.<Evidence>emptySet() : Collections.unmodifiableSet( evidence );
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        StringBuilder message = new StringBuilder();
        for ( Evidence evidenceItem : evidence() )
        {
            message.append( evidenceItem.message(
                    tokenNameLookup.labelGetName( constraint.label() ),
                    tokenNameLookup.propertyKeyGetName( constraint.propertyKeyId() ) ) );
        }
        return message.toString();
    }

    private static class IndexEntryConflict extends Evidence
    {
        final IndexEntryConflictException cause;

        IndexEntryConflict( IndexEntryConflictException cause )
        {
            this.cause = Objects.requireNonNull( cause );
        }

        @Override
        String message( String label, String propertyKey )
        {
            return cause.evidenceMessage( label, propertyKey );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            return cause.equals( ((IndexEntryConflict) o).cause );

        }

        @Override
        public int hashCode()
        {
            return cause.hashCode();
        }

        @Override
        public String toString()
        {
            return "IndexEntryConflict{cause=" + cause + "}";
        }
    }

    private static class MandatoryConstraintConflict extends Evidence
    {
        final long nodeId;

        MandatoryConstraintConflict( long nodeId )
        {
            this.nodeId = nodeId;
        }

        @Override
        String message( String label, String propertyKey )
        {
            return String.format( "Node(%s) with label `%s` has no value for property `%s`",
                    nodeId, label, propertyKey );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            return nodeId == ((MandatoryConstraintConflict) o).nodeId;
        }

        @Override
        public int hashCode()
        {
            return (int) (nodeId ^ (nodeId >>> 32));
        }

        @Override
        public String toString()
        {
            return "MandatoryConstraintConflict{nodeId=" + nodeId + "}";
        }
    }
}
