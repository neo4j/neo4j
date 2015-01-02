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

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import org.neo4j.kernel.logging.ConsoleLogger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidatorTest
{
    @Test
    public void shouldFailWhenRuleFails()
    {
        Validator v = new Validator( new ValidationRule()
        {
            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                throw new RuleFailedException( "dummy rule failed during unit test" );
            }
        } );

        assertFalse( v.validate( null, ConsoleLogger.DEV_NULL ) );
    }

    @Test
    public void shouldFailWhenAtLeastOneRuleFails()
    {
        Validator v = new Validator( new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                // do nothing
            }
        }, new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                throw new RuleFailedException( "dummy rule failed during unit test" );
            }
        }, new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                // do nothing
            }
        }, new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                // do nothing
            }
        } );

        assertFalse( v.validate( null, ConsoleLogger.DEV_NULL ) );
    }

    @Test
    public void shouldPassWhenAllRulesComplete()
    {
        Validator v = new Validator( new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                // do nothing
            }
        }, new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                // do nothing
            }
        }, new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                // do nothing
            }
        }, new ValidationRule()
        {

            @Override
            public void validate( Configuration configuration ) throws RuleFailedException
            {
                // do nothing
            }
        } );

        assertTrue( v.validate( null, ConsoleLogger.DEV_NULL ) );
    }
}
