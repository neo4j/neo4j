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
package org.neo4j.test.mockito.matcher;

import org.assertj.core.api.AbstractAssert;

import java.util.Objects;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;

public class KernelExceptionUserMessageAssert extends AbstractAssert<KernelExceptionUserMessageAssert, SchemaKernelException>
{
    private final TokenNameLookup tokenNameLookup;

    public KernelExceptionUserMessageAssert( SchemaKernelException e, TokenNameLookup tokenNameLookup )
    {
        super( e, KernelExceptionUserMessageAssert.class );
        this.tokenNameLookup = tokenNameLookup;
    }

    public static KernelExceptionUserMessageAssert assertThat( SchemaKernelException e, TokenNameLookup tokenNameLookup )
    {
        return new KernelExceptionUserMessageAssert( e, tokenNameLookup );
    }

    public KernelExceptionUserMessageAssert hasUserMessage( String expectedMessage )
    {
        isNotNull();
        final String exceptionMessage = actual.getUserMessage( tokenNameLookup );
        if ( !Objects.equals( exceptionMessage, expectedMessage ) )
        {
            failWithMessage( "Expected user message to be <%s> but was <%s>", expectedMessage, exceptionMessage );
        }
        return this;
    }
}
