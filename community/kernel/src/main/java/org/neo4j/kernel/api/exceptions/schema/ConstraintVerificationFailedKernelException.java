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
import java.util.Set;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
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
    private final UniquenessConstraint constraint;

    public static final class Evidence
    {
        private final IndexEntryConflictException conflict;

        public Evidence( IndexEntryConflictException conflict )
        {
            this.conflict = conflict;
        }

        @Override
        public boolean equals( Object o )
        {
            return this == o || !(o == null || getClass() != o.getClass()) &&
                    conflict.equals( ((Evidence) o).conflict );
        }

        @Override
        public int hashCode()
        {
            return conflict.hashCode();
        }

        @Override
        public String toString()
        {
            return "Evidence{" +
                    "conflict=" + conflict +
                    '}';
        }
    }

    private final Set<Evidence> evidence;

    public ConstraintVerificationFailedKernelException( UniquenessConstraint constraint, Set<Evidence> evidence )
    {
        super( Status.Schema.ConstraintVerificationFailure, "Existing data does not satisfy %s.", constraint );
        this.constraint = constraint;
        this.evidence = evidence;
    }

    public ConstraintVerificationFailedKernelException( UniquenessConstraint constraint, Throwable failure )
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
            IndexEntryConflictException conflict = evidenceItem.conflict;
            message.append( conflict.evidenceMessage(
                    tokenNameLookup.labelGetName( constraint.label() ),
                    tokenNameLookup.propertyKeyGetName( constraint.propertyKeyId() ) ) );
        }
        return message.toString();
    }
}
