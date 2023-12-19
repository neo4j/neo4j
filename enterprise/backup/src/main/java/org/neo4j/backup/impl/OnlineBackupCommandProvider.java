/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import java.nio.file.Path;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import org.neo4j.OnlineBackupCommandSection;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProvider.getProvidersByPriority;

public class OnlineBackupCommandProvider extends AdminCommand.Provider
{
    public OnlineBackupCommandProvider()
    {
        super( "backup" );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return OnlineBackupContextBuilder.arguments();
    }

    @Override
    @Nonnull
    public String description()
    {
        return format( "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup service must " +
                "have been configured on the server beforehand.%n" +
                "%n" +
                "All consistency checks except 'cc-graph' can be quite expensive so it may be useful to turn them off" +
                " for very large databases. Increasing the heap size can also be a good idea." +
                " See 'neo4j-admin help' for details.%n" +
                "%n" +
                "For more information see: https://neo4j.com/docs/operations-manual/current/backup/" );
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Perform an online backup from a running Neo4j enterprise server.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return OnlineBackupCommandSection.instance();
    }

    @Override
    @Nonnull
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        boolean debug = System.getenv().get( "NEO4J_DEBUG") != null;
        LogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( debug ? Level.DEBUG : Level.NONE ).toOutputStream( outsideWorld.outStream() );
        Monitors monitors = new Monitors();

        OnlineBackupContextBuilder contextBuilder = new OnlineBackupContextBuilder( homeDir, configDir );
        BackupModule backupModule = new BackupModule( outsideWorld, logProvider, monitors );

        BackupSupportingClassesFactoryProvider classesFactoryProvider =
                getProvidersByPriority().findFirst().orElseThrow( noProviderException() );
        BackupSupportingClassesFactory supportingClassesFactory =
                classesFactoryProvider.getFactory( backupModule );
        BackupStrategyCoordinatorFactory coordinatorFactory = new BackupStrategyCoordinatorFactory( backupModule );

        return new OnlineBackupCommand(
                outsideWorld, contextBuilder, supportingClassesFactory, coordinatorFactory );
    }

    private static Supplier<IllegalStateException> noProviderException()
    {
        return () -> new IllegalStateException(
                "Unable to find a suitable backup supporting classes provider in the classpath" );
    }
}
