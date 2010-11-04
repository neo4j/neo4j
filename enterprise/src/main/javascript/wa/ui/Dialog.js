wa.ui.Dialog = (function($){
    var me = {};
    
    me.showImpl = function() {
        $("#mor_dialog_content").modal({
            overlayId: 'mor_dialog_overlay',
            containerId: 'mor_dialog_container',
            closeHTML: null,
            minHeight: 80,
            opacity: 65, 
            position: ['100',],
            overlayClose: true
        });
    	me.adjustHeight();
    };
    
    me.adjustHeight = function () {
    	setTimeout(function() { 
	        var container = $("#mor_dialog_container");
	        var h = $("#mor_dialog_data", container).height()
	            +   $("#mor_dialog_title", container).height()
	            + 30; // padding
	        
	        container.css( { height: h, top: "100px" } );
    	}, 10);
    };
    
    return {
        show : function(title, body, cb) {
            me.cb = cb;
            $("#mor_dialog_title").html(title);
            $("#mor_dialog_data").html(body);
            me.showImpl();
        },
        
        showUsingTemplate : function( title, templateUrl, templateContext, cb ) {
            cb = typeof(templateContext) === "function" ? templateContext : cb;
            me.cb = cb;
            $("#mor_dialog_title").html(title);
            $("#mor_dialog_data").setTemplateURL(templateUrl);
            $("#mor_dialog_data").processTemplate(templateContext || {});
            me.showImpl();
        },
        
        updateTemplateContext : function(templateContext) {
        	$("#mor_dialog_data").processTemplate(templateContext || {});
        	me.adjustHeight();
        },
        
        close : function() {
            $.modal.close();
        }
    };
})(jQuery);
