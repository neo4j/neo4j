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
 * A widget for starting, stopping and restarting the neo4j backend.
 */
wa.widgets.LifecycleWidget = function( server, template )
{

    var me = {};

    // 
    // PRIVATE
    //

    me.template = template || "templates/widgets/LifecycleWidget.tp";
    
    me.element = false;

    me.status = "UNKOWN";
    me.watingForResponse = false;
    me.server = server ? server : false;

    me.getWidget = function()
    {
        return me.element;
    };

    me.start = function( ev )
    {
        if ( me.status !== "RUNNING" )
        {
            me.serverAction( "start", "Starting server.." );
        }
        if ( ev && ev.preventDefault ) ev.preventDefault();
    };

    me.stop = function( ev )
    {
        if ( me.status !== "STOPPED" )
        {
            me.serverAction( "stop", "Stopping server.." );
        }
        if ( ev ) ev.preventDefault();
    };

    me.restart = function( ev )
    {
        me.serverAction( "restart", "Restarting server.." );
        if ( ev ) ev.preventDefault();
    };

    me.check = function( ev )
    {
        if ( me.server )
        {
            me.enable();
            me.serverAction( "getStatus", me.statusElement.html(), "GET" );
        }
        else
        {
            // No server connected
            me.disable();
            me.statusElement.html( "N/A" );
        }
        
        if ( ev && ev.preventDefault ) ev.preventDefault();
    };

    me.disable = function()
    {
        me.buttons.start.hide();
        me.buttons.stop.hide();
        me.buttons.restart.hide();
    };

    me.enable = function() {
        me.buttons.start.show();
        me.buttons.stop.show();
        me.buttons.restart.show();
        
    };
    
    me.serverAction = function( action, message, type )
    {
        var type = type || "POST";
        if ( !me.watingForResponse )
        {
            me.statusElement.html( message );
            me.watingForResponse = true;

            $("ul.mor_lifecycle_actions a").addClass("disabled");
            
            // Allow UI update
            me.server.manage.lifecycle[action]( 
                    function( data ) {
                        me.watingForResponse = false;
                        me.setStatus( data.current_status );
                    });
        }
    };

    me.setStatus = function( status )
    {
        me.status = status;
        if ( me.statusActions[status] )
        {
            me.statusActions[status]();
        }
    };

    me.statusActions =
    {
    	RUNNING : function()
        {
            me.statusElement.html( "Running" );
            
            var wrap = $("div.mor_lifecycle");
            wrap.addClass("mor_lifecycle_running");
            wrap.removeClass("mor_lifecycle_stopped");
            
            $("ul.mor_lifecycle_actions a").removeClass("disabled");
            $("ul.mor_lifecycle_actions a.mor_lifecycle_start").addClass("disabled");
        },

        STOPPED : function()
        {
            me.statusElement.html( "Stopped" );
            
            var wrap = $("div.mor_lifecycle");
            wrap.addClass("mor_lifecycle_stopped");
            wrap.removeClass("mor_lifecycle_running");
            
            $("ul.mor_lifecycle_actions a").removeClass("disabled");
            $("ul.mor_lifecycle_actions a.mor_lifecycle_stop").addClass("disabled");
            $("ul.mor_lifecycle_actions a.mor_lifecycle_restart").addClass("disabled");
        }
    };
    
    //
    // CONSTRUCT
    //

    // Create UI
    var tmpElement = $( "<div></div>" );
    tmpElement.setTemplateURL( me.template );
    tmpElement.processTemplate({domain:server.domain});
    
    me.element = tmpElement.children();

    // Cache element lookups
    me.buttons = {};
    me.buttons.start = $( ".mor_lifecycle_start", me.element );
    me.buttons.restart = $( ".mor_lifecycle_restart", me.element );
    me.buttons.stop = $( ".mor_lifecycle_stop", me.element );

    me.statusElement = $( ".mor_lifecycle_status", me.element );

    // Event listeners
    me.buttons.start.click( me.start );
    me.buttons.restart.click( me.restart );
    me.buttons.stop.click( me.stop );

    // Check server status at regular intervals
    me.check();
    
    //
    // PUBLIC INTERFACE
    //

    me.api =
    {
        init      : me.init,
        check     : me.check,
        stop      : me.stop,
        start     : me.start,
        restart   : me.restart,
        getWidget : me.getWidget,
        render    : me.getWidget
    };

    return me.api;

};
