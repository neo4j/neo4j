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
 * Used to keep track of the current disk usage.
 * 
 * @param server
 *            is the server instance to track
 * @param interval
 *            (optional) is the update interval in milliseconds. The default is
 *            10000.
 */
wa.components.dashboard.DiskUsageWidget = function(server,
		interval) {

	var me = {};

	me.server = server;
	me.tracker = null;
	me.ui = $("<div class='mor_module mor_span-3'></div>");
	
	//
	// PUBLIC
	//

	me.api = {
			
		/**
		 * Render this widget.
		 * @return a ui element to insert into the DOM
		 */
		render : function() {
			
			if ( ! me.uiLoaded ) {
				me.uiLoaded = true;
				me.ui.setTemplateURL("templates/components/monitor/DiskUsageWidget.tp");
			}
			
			if ( ! me.runnning ) {
				me.api.startPolling();
			}
			
			return me.ui;
		
		},
		
		stopPolling : function() {
			me.tracker.stop();
			me.running = false;
		},
		
		startPolling : function() {
			me.tracker.run();
			me.running = true;
		}
			
	};

	//
	// INTERNALS
	//

	me.extractor = function(bean) {
		var values = {};
		for( var i = 0, l = bean.attributes.length; i < l; i++ ){
			values[bean.attributes[i].name] = bean.attributes[i];
		}
		
		return values;
	};

	me.valueChanged = function(data) {
		me.data = data;
		
		if ( me.uiLoaded ) {
			me.ui.processTemplate({
				data : me.data.value
			});
		}
		
		return true;
	};

	//
	// CONSTRUCT
	// 

	me.tracker = wa.components.dashboard.JmxValueTracker(me.server,
			"neo4j", "Store file sizes", me.extractor, me.valueChanged,
			interval || 10000);

	return me.api;
};