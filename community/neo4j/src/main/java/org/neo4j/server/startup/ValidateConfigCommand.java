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
package org.neo4j.server.startup;

import static org.neo4j.server.startup.validation.ConfigValidationSummary.ValidationResult.ERRORS;

import java.io.IOException;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cli.ExitCode;
import org.neo4j.server.startup.validation.ConfigValidationHelper;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine.Command;

@Command(name = "validate-config", description = "Validate configuration including Log4j.")
public class ValidateConfigCommand extends AbstractCommand {
    private ConfigValidationHelper helper;
    public static final String COMMAND = "neo4j-admin server validate-config";

    public ValidateConfigCommand(ExecutionContext ctx) {
        this(ctx, null);
    }

    @VisibleForTesting
    public ValidateConfigCommand(ExecutionContext ctx, ConfigValidationHelper helper) {
        super(ctx);
        this.helper = helper;
    }

    @Override
    protected void execute() throws CommandFailedException, IOException {
        var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
        try (var bootloader = enhancedCtx.createDbmsBootloader()) {
            if (helper == null) {
                helper = new ConfigValidationHelper(bootloader.confFile());
            }

            var summary = helper.validateAll(() -> bootloader.fullConfig().getUnfiltered());
            summary.print(ctx.out(), verbose);
            summary.printClosingStatement(ctx.out());

            if (summary.result() == ERRORS) {
                throw new CommandFailedException("Configuration contains errors.", ExitCode.CONFIG);
            }
        }
    }
}
