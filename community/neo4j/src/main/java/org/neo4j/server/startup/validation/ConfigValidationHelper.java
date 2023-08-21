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
package org.neo4j.server.startup.validation;

import static org.neo4j.configuration.GraphDatabaseSettings.server_logging_config_path;
import static org.neo4j.configuration.GraphDatabaseSettings.user_logging_config_path;
import static org.neo4j.server.startup.validation.ConfigValidationSummary.ValidationResult.ERRORS;

import java.nio.file.Path;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.util.VisibleForTesting;

public class ConfigValidationHelper {
    private final ConfigValidator.Factory validatorFactory;

    public ConfigValidationHelper(Path configPath) {
        this(new DefaultValidatorFactory(configPath));
    }

    @VisibleForTesting
    public ConfigValidationHelper(ConfigValidator.Factory validatorFactory) {
        this.validatorFactory = validatorFactory;
    }

    /**
     * Uses the config supplier to build the Neo4j config and capture any errors.
     * If that passes, the Log4j config will also be validated, based on the Neo4j config.
     *
     * @param config A supplier that produces a config, typically using {@link Config.Builder}.
     *               Use a memoized supplier to avoid constructing the config multiple times.
     * @return A summary of the validation results.
     */
    public ConfigValidationSummary validateAll(Supplier<Config> config) {
        var summary = new ConfigValidationSummary();

        summary.add(validate(validatorFactory.getNeo4jValidator(config)));
        if (summary.result() == ERRORS) {
            summary.add(new ConfigValidationSummary.MessageEvent("Skipping Log4j validation due to previous issues."));
            return summary;
        }

        // If Neo4j config is valid, we can go ahead and validate the Log4j config as well
        summary.add(validate(validatorFactory.getLog4jUserValidator(config)));
        summary.add(validate(validatorFactory.getLog4jServerValidator(config)).treatErrorsAsWarnings());

        return summary;
    }

    private ConfigValidationSummary.Event validate(ConfigValidator validator) {
        try {
            var issues = validator.validate();
            return new ConfigValidationSummary.ResultEvent(validator.getLabel(), issues);
        } catch (Exception e) {
            return new ConfigValidationSummary.ErrorEvent(validator.getLabel(), e);
        }
    }

    private static class DefaultValidatorFactory implements ConfigValidator.Factory {
        private final Path configPath;

        private DefaultValidatorFactory(Path configPath) {
            this.configPath = configPath;
        }

        @Override
        public ConfigValidator getNeo4jValidator(Supplier<Config> config) {
            return new Neo4jConfigValidator(config, configPath);
        }

        @Override
        public ConfigValidator getLog4jUserValidator(Supplier<Config> config) {
            var configPath = config.get().get(user_logging_config_path);
            return new Log4jConfigValidator(config, "user", configPath);
        }

        @Override
        public ConfigValidator getLog4jServerValidator(Supplier<Config> config) {
            var configPath = config.get().get(server_logging_config_path);
            return new Log4jConfigValidator(config, "server", configPath);
        }
    }
}
