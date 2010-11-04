
/**
 * Handles creating and removing nodes.
 */
wa.components.data.NodeManager = (function($) { 
	
	var me = {};
	
	me.dataCore = wa.components.data.DataBrowser;
	
	//
	// INTERNALS
	//
	
	me.server = function() {
		return me.dataCore.getServer();
	};
	
	me.addNode = function(ev) {
		ev.preventDefault();
		me.server().post("node", {}, function(data) {
			var url = data.self;
			// Strip the server's URL
			url = url.substring(me.server().url.length );
			// Show the node
			$.bbq.pushState({ dataurl: url });
		});
	};
	
	me.deleteItem = function(ev) {
		ev.preventDefault();
		if( confirm("Are you sure?")) {
			neo4j.Web.del(me.dataCore.getItem().self, function(data) {
				// Go to root node
				$.bbq.pushState({ dataurl: "node/0" });
			});
		}
	};
	
	$(".mor_data_add_node_button").live("click", me.addNode);
	$(".mor_data_delete_node_button").live("click", me.deleteItem);
	
	return {};
	
})(jQuery);