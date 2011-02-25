/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Start webadmin.
 */
require([
    "lib/jquery.1.4.2",
    "lib/json2",
    "lib/jquery-jtemplates",
    "lib/jquery.bbq",
    "lib/jquery.flot",
    "lib/jquery.simplemodal",

    "lib/neo4js",

    "wa/__init__",
    "wa/escape",
    "wa/events",
    "wa/PropertyStorage",
    "wa/prop",
    "wa/Servers",
    "wa/FormValidator",
                
    "wa/connectionmonitor",

    "wa/ui/__init__",
    "wa/ui/Tooltip",
    "wa/ui/Dialog",
    "wa/ui/Loading",
    "wa/ui/Pages",
    "wa/ui/Helpers",
    "wa/ui/MainMenu",
    "wa/ui/ErrorBox",

    "wa/widgets/__init__",
    "wa/widgets/LifecycleWidget",

    "wa/components/__init__",

    "wa/components/dashboard/__init__",
    "wa/components/dashboard/Dashboard",
    "wa/components/dashboard/PrimitiveCountWidget",
    "wa/components/dashboard/JmxValueTracker",
    "wa/components/dashboard/DiskUsageWidget",
    "wa/components/dashboard/CacheWidget",
    "wa/components/dashboard/CacheWidget",
    "wa/components/dashboard/MonitorChart",

    "wa/components/jmx/__init__",
    "wa/components/jmx/Jmx",

    "wa/components/backup/__init__",
    "wa/components/backup/parseJobData",
    "wa/components/backup/Backup",
    "wa/components/console/__init__",
    "wa/components/console/Console",

    "wa/components/config/__init__",
    "wa/components/config/Config",

    "wa/components/io/__init__",
    "wa/components/io/GraphIO",
    "wa/components/io/ExportSupport",
    "wa/components/io/ImportSupport",

    "wa/components/data/__init__",
    "wa/components/data/PropertiesToListManager",
    "wa/components/data/DataBrowser",
    "wa/components/data/PropertyEditor",
    "wa/components/data/NodeManager",
    "wa/components/data/RelationshipManager",
  ], function() {
    $(function() {
        $.jTemplatesDebugMode(false);
        
        // Load UI
        wa.ui.MainMenu.init();
        wa.ui.Pages.init();
        wa.ui.Helpers.init();

        // Trigger init event
        wa.trigger( "init" );
        
    });
});

