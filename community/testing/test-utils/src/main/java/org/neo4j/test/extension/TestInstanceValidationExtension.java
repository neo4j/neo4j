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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.tryToReadFieldValue;

/**
 * Validation junit extension to verify that all fields that were marked as injectable with @{@link Inject} annotation were assigned by test container.
 * In case if field is still null right before test execution its a signal that extension misconfiguration is in place and validation exception will be thrown.
 */
public class TestInstanceValidationExtension implements BeforeTestExecutionCallback
{
    @Override
    public void beforeTestExecution( ExtensionContext context )
    {
        Optional<Object> testInstance = context.getTestInstance();
        testInstance.ifPresent( instance ->
        {
            Class<?> instanceClass = instance.getClass();
            List<Field> annotatedFields = findAnnotatedFields( instanceClass, Inject.class );
            for ( Field annotatedField : annotatedFields )
            {
                Optional<Object> fieldValue = tryToReadFieldValue( annotatedField, instance ).toOptional();
                if ( fieldValue.isEmpty() )
                {
                    throw new ExtensionConfigurationException( format( "Field %s that is marked for injection in class %s is null. " +
                                    "Please check that you have configured all desired extensions or double check fields that should be injected.",
                            annotatedField.getName(), instanceClass.getName() ) );
                }
            }
        } );
    }
}

