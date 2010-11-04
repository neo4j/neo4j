
/**
 * Handles creating and removing relationships, changing relationship type and setting start and end nodes.
 */
wa.components.data.RelationshipManager = (function($) {
	
	var me = {};
	
	me.dataCore = wa.components.data.DataBrowser;
	
	//
	// INTERNALS
	//
	
	me.server = function() {
		return me.dataCore.getServer();
	};
	
	me.addRelatiohship = function(ev) {
		ev.preventDefault();
		wa.ui.Dialog.showUsingTemplate("New relationship","templates/components/data/new_relationship.tp", me.dialogLoaded);
	};
	
	me.saveNewRelationship = function(ev) {
		ev.preventDefault();
		var from = $("#mor_data_relationship_dialog_from").val();
		var type = $("#mor_data_relationship_dialog_type").val();
		var to = $("#mor_data_relationship_dialog_to").val();
		
		if( from.indexOf("://") ) {
			from = from.substring(from.lastIndexOf("/")+1);
		}
		
		if( ! to.indexOf("://")) {
			to = me.server().url + to;
		}
		
		me.server().post("node/" + from + "/relationships", {
				"to" : to,
				"data" : {},
				"type": type
			}, function(data) {
			    wa.components.data.DataBrowser.reload();
			}
		);
		
		wa.ui.Dialog.close();
	};
	
	/**
	 * This is called each time the create relationship dialog is shown.
	 */
	me.dialogLoaded = function() {
		// Populate from field
		var id = me.dataCore.getItem().self;
		id = id.substring(id.lastIndexOf("/") + 1);
		
		$("#mor_data_relationship_dialog_from").val(id);
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
	
	$(".mor_data_delete_relationship_button").live("click", me.deleteItem);
	$(".mor_data_add_relationship").live("click", me.addRelatiohship);
	$(".mor_data_relationship_dialog_save").live("click", me.saveNewRelationship);
	
	return {};
	
})(jQuery);