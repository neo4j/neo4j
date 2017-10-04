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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;

import static java.lang.String.format;

/**
 * Attempting to validate constraints but the apparatus for validation was not available. For example,
 * this exception is thrown when an index required to implement a uniqueness constraint is not available.
 */
public class UnableToValidateConstraintException extends ConstraintValidationException
{
    public UnableToValidateConstraintException( ConstraintDescriptor constraint, Throwable cause )
    {
        super( constraint, Phase.VERIFICATION,
                format( "Unable to validate constraint %s",
                        constraint.userDescription( SchemaUtil.idTokenNameLookup ) ),
                cause );
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( "Unable to validate constraint %s", constraint.userDescription( tokenNameLookup ) );
    }
}
