/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.test.extension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Objects;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;

public class SkipTestExtension implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return AnnotationUtils.findAnnotation(context.getElement(), Skip.class)
                .filter(s -> {
                    String property = System.getProperty(s.key());
                    String[] values = s.values();
                    for (String value : values) {
                        if (Objects.equals(property, value)) {
                            return true;
                        }
                    }
                    return false;
                })
                .map(s -> ConditionEvaluationResult.disabled(String.format(
                        "Skipping test annotated with @%s(%s=%s), matching system property.",
                        s.annotationType().getSimpleName(), s.key(), Arrays.toString(s.values()))))
                .orElse(ConditionEvaluationResult.enabled("Test enabled. No matching system property found."));
    }

    @Inherited
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @ExtendWith(SkipTestExtension.class)
    public @interface Skip {
        String key();

        String[] values();
    }
}
