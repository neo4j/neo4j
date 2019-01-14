/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.helpers.Exceptions;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;

public class UniquePropertyValueValidationException extends ConstraintValidationException
{
    private final Set<IndexEntryConflictException> conflicts;

    public UniquePropertyValueValidationException( IndexBackedConstraintDescriptor constraint,
            ConstraintValidationException.Phase phase, IndexEntryConflictException conflict )
    {
        this( constraint, phase, Collections.singleton( conflict ) );
    }

    public UniquePropertyValueValidationException( IndexBackedConstraintDescriptor constraint,
            ConstraintValidationException.Phase phase, Set<IndexEntryConflictException> conflicts )
    {
        super( constraint, phase, phase == Phase.VERIFICATION ? "Existing data" : "New data", buildCauseChain( conflicts ) );
        this.conflicts = conflicts;
    }

    private static IndexEntryConflictException buildCauseChain( Set<IndexEntryConflictException> conflicts )
    {
        IndexEntryConflictException chainedConflicts = null;
        for ( IndexEntryConflictException conflict : conflicts )
        {
            chainedConflicts = Exceptions.chain( chainedConflicts, conflict );
        }
        return chainedConflicts;
    }

    public UniquePropertyValueValidationException( IndexBackedConstraintDescriptor constraint,
            ConstraintValidationException.Phase phase, Throwable cause )
    {
        super( constraint, phase, phase == Phase.VERIFICATION ? "Existing data" : "New data", cause );
        this.conflicts = Collections.emptySet();
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        SchemaDescriptor schema = constraint.schema();
        StringBuilder message = new StringBuilder();
        for ( Iterator<IndexEntryConflictException> iterator = conflicts.iterator(); iterator.hasNext(); )
        {
            IndexEntryConflictException conflict = iterator.next();
            message.append( conflict.evidenceMessage( tokenNameLookup, schema ) );
            if ( iterator.hasNext() )
            {
                message.append( System.lineSeparator() );
            }
        }
        return message.toString();
    }

    public Set<IndexEntryConflictException> conflicts()
    {
        return conflicts;
    }
}
