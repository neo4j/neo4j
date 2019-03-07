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
package org.neo4j.kernel.impl.constraints;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.neo4j.annotations.service.Service;
import org.neo4j.service.NamedService;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;

import static java.lang.String.format;
import static org.neo4j.util.Preconditions.checkState;

/**
 * Implements semantics of constraint creation and enforcement.
 */
@Service
public abstract class ConstraintSemantics implements NamedService, ConstraintValidator, ConstraintRuleAccessor
{
    private final int priority;

    public static ConstraintSemantics getConstraintSemantics()
    {
        final Collection<ConstraintSemantics> candidates = Services.loadAll( ConstraintSemantics.class );
        checkState( !candidates.isEmpty(), format( "At least one implementation of %s should be available.", ConstraintSemantics.class ) );
        return Collections.max( candidates, Comparator.comparingInt( ConstraintSemantics::getPriority ) );
    }

    protected ConstraintSemantics( int priority )
    {
        this.priority = priority;
    }

    public int getPriority()
    {
        return priority;
    }
}
