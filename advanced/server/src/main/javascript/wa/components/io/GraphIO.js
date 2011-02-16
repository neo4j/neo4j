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
 * Handles imports and exports.
 */
wa.components.io.GraphIO = (function($) {
	
	var me = {};
	
	me.basePage = $("<div></div>");
	me.uiLoaded  = false;
	me.uploadUrl = "";
	
	
	//
	// INTERNALS
	//
	
	me.render = function() {
	    if( me.uiLoaded ) {
	        me.basePage.processTemplate({uploadUrl:me.uploadUrl});
	    }
    };
    
    me.pageChanged = function(ev) {
        if(ev.data === "io") {
            
            if( me.uiLoaded === false ) {
                me.uiLoaded = true;
                me.basePage.setTemplateURL("templates/components/io/index.tp");
                me.render();
            }
        }
    };
    
	
	//
	// LISTEN TO THE WORLD
	//
	
	wa.bind("ui.page.changed", me.pageChanged);
	wa.bind("servers.current.changed", function(ev) {
	    var server = wa.Servers.getCurrentServer();
	    server.manage.importing.getUploadUrl(function(url){
	        me.uploadUrl = url;
	        me.render();
	    });
	});
	
	return {
        getPage :  function() {
            return me.basePage;
        }
    };;
	
})(jQuery);


wa.ui.Pages.add("io",wa.components.io.GraphIO);
wa.ui.MainMenu.add({ label : "Import / Export", pageKey:"io", index:8, requiredServices:['importing','exporting'], perspectives:['server']});