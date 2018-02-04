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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Parameter;

import static org.mockito.Mockito.mock;

/**
 * Mockito extension based on sample provided bu junit team at
 * https://github.com/junit-team/junit5-samples/blob/master/junit5-mockito-extension/src/main/java/com/example/mockito/MockitoExtension.java
 */
public class MockitoExtension implements TestInstancePostProcessor, ParameterResolver
{

    @Override
    public void postProcessTestInstance( Object testInstance, ExtensionContext context )
    {
        MockitoAnnotations.initMocks( testInstance );
    }

    @Override
    public boolean supportsParameter( ParameterContext parameterContext, ExtensionContext extensionContext )
    {
        return parameterContext.getParameter().isAnnotationPresent( Mock.class );
    }

    @Override
    public Object resolveParameter( ParameterContext parameterContext, ExtensionContext extensionContext )
    {
        return getMock( parameterContext.getParameter(), extensionContext );
    }

    private Object getMock( Parameter parameter, ExtensionContext extensionContext )
    {
        Class<?> mockType = parameter.getType();
        ExtensionContext.Store mocks =
                extensionContext.getStore( ExtensionContext.Namespace.create( MockitoExtension.class, mockType ) );
        String mockName = getMockName( parameter );

        if ( mockName != null )
        {
            return mocks.getOrComputeIfAbsent( mockName, key -> mock( mockType, mockName ) );
        }
        else
        {
            return mocks.getOrComputeIfAbsent( mockType.getCanonicalName(), key -> mock( mockType ) );
        }
    }

    private String getMockName( Parameter parameter )
    {
        String explicitMockName = parameter.getAnnotation( Mock.class ).name().trim();
        if ( !explicitMockName.isEmpty() )
        {
            return explicitMockName;
        }
        else if ( parameter.isNamePresent() )
        {
            return parameter.getName();
        }
        return null;
    }

}
