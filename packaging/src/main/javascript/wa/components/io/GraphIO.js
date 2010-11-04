
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