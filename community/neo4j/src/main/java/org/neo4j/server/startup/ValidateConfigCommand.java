/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.startup;

import static org.neo4j.configuration.GraphDatabaseSettings.server_logging_config_path;
import static org.neo4j.configuration.GraphDatabaseSettings.user_logging_config_path;
import static org.neo4j.server.startup.ValidateConfigCommand.ValidationResult.ERRORS;
import static org.neo4j.server.startup.ValidateConfigCommand.ValidationResult.OK;
import static org.neo4j.server.startup.ValidateConfigCommand.ValidationResult.WARNINGS;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.ObjectUtils;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cli.ExitCode;
import org.neo4j.server.startup.validation.ConfigValidationIssue;
import org.neo4j.server.startup.validation.ConfigValidator;
import org.neo4j.server.startup.validation.Log4jConfigValidator;
import org.neo4j.server.startup.validation.Neo4jConfigValidator;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine.Command;

@Command(name = "validate-config", description = "Validate configuration including Log4j.")
public class ValidateConfigCommand extends AbstractCommand {
    private final ConfigValidator.Factory validatorFactory;

    public ValidateConfigCommand(ExecutionContext ctx) {
        this(ctx, new DefaultValidatorFactory());
    }

    @VisibleForTesting
    public ValidateConfigCommand(ExecutionContext ctx, ConfigValidator.Factory validatorFactory) {
        super(ctx);
        this.validatorFactory = validatorFactory;
    }

    @Override
    protected void execute() throws CommandFailedException, IOException {
        var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
        try (var bootloader = enhancedCtx.createDbmsBootloader()) {
            var result = validateAll(bootloader);
            if (result == ERRORS) {
                ctx.out().println("Validation failed.");
                throw new CommandFailedException("Configuration contains errors.", ExitCode.CONFIG);
            } else if (result == WARNINGS) {
                ctx.out().println("Validation successful (with warnings).");
            } else {
                ctx.out().println("Validation successful.");
            }
        }
    }

    private ValidationResult validateAll(Bootloader bootloader) {
        ValidationResult result = validate(validatorFactory.getNeo4jValidator(bootloader));
        if (result == ERRORS) {
            ctx.out().println("Skipping Log4j validation due to previous issues.");
            return ERRORS;
        }

        // If Neo4j config is valid, the bootloader now has the full config loaded.
        result = result.and(validate(validatorFactory.getLog4jServerValidator(bootloader)));
        result = result.and(validate(validatorFactory.getLog4jUserValidator(bootloader)));
        return result;
    }

    private ValidationResult validate(ConfigValidator validator) {
        ctx.out().printf("Validating %s%n", validator.getLabel());

        ValidationResult result = OK;
        try {
            var issues = validator.validate();
            printIssueCount(issues);

            for (var issue : issues) {
                printIssue(issue);
                result = result.and(issue.isError() ? ERRORS : WARNINGS);
            }
        } catch (Exception e) {
            result = ERRORS;
            ctx.out().printf("Error: %s%n", e.getMessage());
            if (verbose) {
                e.printStackTrace(ctx.out());
            }
        }

        ctx.out().println();
        return result;
    }

    private void printIssueCount(List<ConfigValidationIssue> issues) {
        if (issues.isEmpty()) {
            ctx.out().printf("No issues found.%n");
        } else {
            ctx.out().printf("%d issue%s found.%n", issues.size(), issues.size() == 1 ? "" : "s");
        }
    }

    private void printIssue(ConfigValidationIssue issue) {
        ctx.out().printf("%s%n", issue.getMessage());
        if (verbose) {
            issue.printStackTrace(ctx.out());
        }
    }

    @VisibleForTesting
    enum ValidationResult {
        OK,
        WARNINGS,
        ERRORS;

        public ValidationResult and(ValidationResult other) {
            return ObjectUtils.max(this, other);
        }
    }

    private static class DefaultValidatorFactory implements ConfigValidator.Factory {
        @Override
        public ConfigValidator getNeo4jValidator(Bootloader bootloader) {
            return new Neo4jConfigValidator(bootloader);
        }

        @Override
        public ConfigValidator getLog4jUserValidator(Bootloader bootloader) {
            var configPath = bootloader.config().get(server_logging_config_path);
            return new Log4jConfigValidator(bootloader, "user", configPath);
        }

        @Override
        public ConfigValidator getLog4jServerValidator(Bootloader bootloader) {
            var configPath = bootloader.config().get(user_logging_config_path);
            return new Log4jConfigValidator(bootloader, "server", configPath);
        }
    }
}
