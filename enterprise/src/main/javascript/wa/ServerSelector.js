/*
 * Copyright (c) 2002-2010 "Neo Technology,"
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
 * Handles the server picking UI, adding and removing servers.
 * 
 * If only one server is available, this will automatically set that server as the current one.
 */
wa.ServerSelector = (function($) {

	var me = {};
	
	// 
	// PRIVATE
	//
	
	me.reload = function() {

		var servers = wa.Servers.getServers();
		var currentServer = wa.Servers.getCurrentServer();
		
		if( currentServer == null ) {
		    for( var key in servers ) {
		        $.bbq.pushState( {"s":key} );
		        return;
		    }
		}
		
		var list = $("#mor_servers ul.mor_servers_list");
		list.empty();
		
		for(var key in servers ) {
			var extraClasses = "";
			if( servers[key] !== currentServer ) {
				list.append('<li><a class="'+extraClasses+'" href="/index.html#p=morpheus.monitor&s='+key+'">'+key+'</a></li>');
			} else {
				$("#mor_servers_current").html(key);
			}
	    }
		
		list.append('<li><a href="#" class="mor_servers_list_manage">Manage servers</a></li>');

    	$("ul.mor_servers_list").addClass("hidden");
	};
	
	//
	// CONSTRUCT
	//
	
	// Keep track of when servers are available
	wa.bind("init", function() {
	    if( wa.Servers.isLoaded() ) {
	    	me.reload();
	    } else {
	        wa.bind( "servers.loaded", function(ev) { me.reload(); });
	    }
	
	    wa.bind("servers.current.changed",  function() { me.reload(); });
	    wa.bind("servers.changed",  function() { me.reload(); });
	});
	
    $("a.mor_servers_list_manage").live("click",function(ev){
    	ev.preventDefault();
    	$("ul.mor_servers_list").addClass("hidden");
    	wa.ui.Dialog.showUsingTemplate("Manage servers","templates/manageServers.tp", wa.Servers.getServers());
    });
    
    $("#mor_servers_add_button").live("click", function(ev) {
    	ev.preventDefault();
    	
    	var key = $("#mor_servers_add_name").val();
    	var dataUrl = $("#mor_servers_add_dataUrl").val();
    	var manageUrl = $("#mor_servers_add_manageUrl").val();
    	
    	if( key.length < 1 ) {
    		$("#mor_servers_add_name").addClass("error");
    	} else {
    		$("#mor_servers_add_name").removeClass("error");
    	}
    	
    	if( dataUrl.length < 1 ) {
    		$("#mor_servers_add_dataUrl").addClass("error");
    	} else {
    		$("#mor_servers_add_dataUrl").removeClass("error");
    	}
    	
    	if( manageUrl.length < 1 ) {
    		$("#mor_servers_add_manageUrl").addClass("error");
    	} else {
    		$("#mor_servers_add_manageUrl").removeClass("error");
    	}
    	
    	if( dataUrl.length > 0 && key.length > 0 && manageUrl.length > 0 ) {
    		wa.Servers.setServer(key, dataUrl, manageUrl);
    		wa.ui.Dialog.updateTemplateContext(wa.Servers.getServers());
    	}
    });
    
    $("#mor_servers_current").live("click", function(ev) {
    	$("ul.mor_servers_list").toggleClass("hidden");
    	ev.preventDefault();
    });
    
    $("a.mor_servers_remove_button").live("click",function(ev) {
    	ev.preventDefault();
    	var key = $(ev.target).closest("tr").find("td.mor_servers_name").html();
    	wa.Servers.removeServer(key);
		wa.ui.Dialog.updateTemplateContext(wa.Servers.getServers());
    });
	
	return {};
	
})(jQuery);