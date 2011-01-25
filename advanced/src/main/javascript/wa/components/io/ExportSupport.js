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
 * Handles exporting the database.
 */
wa.components.io.ExportSupport = (function($) {
	
	var me = {};
	
	//
	// PRIVATE
	//
	
	// 
	// LISTEN TO THE WORLD
	//
	
	$("button.mor_io_export_button").live("click",function(ev) {
		ev.preventDefault();

		$(".mor_io_export_error_wrap").hide();
		$(".mor_io_export_button_wrap").hide();
		$(".mor_io_export_progress_wrap").show();
		
		var server = wa.Servers.getCurrentServer();
		server.manage.exporting.all(function(data){
			$(".mor_io_export_button_wrap").show();
			$(".mor_io_export_progress_wrap").hide();
			
			var url = data.url;
			window.open(url,'Neo4j export download','');
			
		});
		
	});
	
	return {};
	
})(jQuery);