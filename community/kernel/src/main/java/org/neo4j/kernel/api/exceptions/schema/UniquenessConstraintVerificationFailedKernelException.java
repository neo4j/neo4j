/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.Iterator;
import java.util.Set;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexEntryConflictException;

public class UniquenessConstraintVerificationFailedKernelException extends ConstraintVerificationFailedKernelException
{
    private final UniquenessConstraint constraint;
    private final Set<IndexEntryConflictException> conflicts;

    public UniquenessConstraintVerificationFailedKernelException( UniquenessConstraint constraint,
            Set<IndexEntryConflictException> conflicts )
    {
        super( constraint );
        this.constraint = constraint;
        this.conflicts = conflicts;
    }

    public UniquenessConstraintVerificationFailedKernelException( UniquenessConstraint constraint, Throwable cause )
    {
        super( constraint, cause );
        this.constraint = constraint;
        this.conflicts = Collections.emptySet();
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        StringBuilder message = new StringBuilder();
        for ( Iterator<IndexEntryConflictException> iterator = conflicts.iterator(); iterator.hasNext(); )
        {
            IndexEntryConflictException conflict = iterator.next();
            message.append( conflict.evidenceMessage(
                    tokenNameLookup.labelGetName( constraint.label() ),
                    tokenNameLookup.propertyKeyGetName( constraint.propertyKey() )
            ) );
            if ( iterator.hasNext() )
            {
                message.append( System.lineSeparator() );
            }
        }
        return message.toString();
    }

    @Override
    public UniquenessConstraint constraint()
    {
        return constraint;
    }
}
