/**
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
package org.neo4j.server.configuration.validation;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;

import org.neo4j.kernel.logging.ConsoleLogger;

public class Validator
{
    private final ArrayList<ValidationRule> validationRules = new ArrayList<ValidationRule>();

    public Validator( ValidationRule... rules )
    {
        if ( rules == null )
        {
            return;
        }
        for ( ValidationRule r : rules )
        {
            this.validationRules.add( r );
        }
    }

    public boolean validate( Configuration configuration, ConsoleLogger log )
    {
        for ( ValidationRule vr : validationRules )
        {
            try
            {
                vr.validate( configuration );
            }
            catch ( RuleFailedException rfe )
            {
                log.warn( rfe.getMessage() );
                return false;
            }
        }
        return true;
    }

    public static final Validator NO_VALIDATION = new Validator();
}
