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

import static java.lang.String.format;
import static org.neo4j.test.TestDatabaseManagementServiceFactorySupplier.FACTORY_SUPPLIER;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.Preconditions;

final class StorageFormatTestCondition implements ExecutionCondition {

    private static final String NEO4J_OVERRIDE_STORE_FORMAT = "NEO4J_OVERRIDE_STORE_FORMAT";
    private static final String MATCH_TEMPLATE = NEO4J_OVERRIDE_STORE_FORMAT + " matches %s [%s]";
    private static final String NO_MATCH_TEMPLATE = NEO4J_OVERRIDE_STORE_FORMAT + " does not match %s [%s]";

    private final ConditionEvaluationResult DEFAULT_ENABLED =
            ConditionEvaluationResult.enabled("No %s condition was encountered.".formatted(getClass()));

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        var result = AnnotationUtils.findAnnotation(context.getElement(), OnStorageFormatOverride.class)
                .map(this::evaluate)
                .orElse(DEFAULT_ENABLED);

        logger.trace(() -> format(
                "Evaluation of [%s] on %s -> [enabled = %s, reason = %s]",
                getClass().getSimpleName(), context.getElement(), !result.isDisabled(), result.getReason()));
        return result;
    }

    public ConditionEvaluationResult evaluate(OnStorageFormatOverride annotation) {
        StorageFormat fmt = annotation.format();
        Preconditions.notNull(fmt, () -> "The 'format' attribute must not be null in " + annotation);
        String regex = getRegex(fmt);
        Preconditions.condition(
                annotation.skip() ^ annotation.require(),
                () -> "Exactly one of the values 'skip' or 'require' must be true in " + annotation);

        boolean enabledOnMatch = annotation.require(); // Note that 'skip' XOR 'require' is true
        String overrideFormat = System.getProperty(NEO4J_OVERRIDE_STORE_FORMAT);
        if (overrideFormat == null) {
            overrideFormat = FACTORY_SUPPLIER;
        }

        if (overrideFormat == null) {
            return ConditionEvaluationResult.enabled(format(MATCH_TEMPLATE, fmt, regex));
        }

        if (overrideFormat.matches(regex)) {
            if (enabledOnMatch) {
                return ConditionEvaluationResult.enabled(format(MATCH_TEMPLATE, fmt, regex));
            } else {
                return ConditionEvaluationResult.disabled(format(NO_MATCH_TEMPLATE, fmt, regex));
            }
        } else {
            if (enabledOnMatch) {
                return ConditionEvaluationResult.disabled(format(MATCH_TEMPLATE, fmt, regex));
            } else {
                return ConditionEvaluationResult.enabled(format(NO_MATCH_TEMPLATE, fmt, regex));
            }
        }
    }

    static String getRegex(StorageFormat storageFormat) {
        return switch (storageFormat) {
            case ALIGNED -> "aligned";
            case BLOCK -> "(?:multiversion_)?block";
            case SPD -> "spd";
        };
    }

    @Inherited
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @ExtendWith(StorageFormatTestCondition.class)
    public @interface OnStorageFormatOverride {
        StorageFormat format();

        boolean skip() default false;

        boolean require() default false;
    }

    public enum StorageFormat {
        ALIGNED,
        BLOCK,
        SPD
    }
}
