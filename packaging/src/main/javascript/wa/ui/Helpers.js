/**
 * Handles foldout help available throughout webadmin.
 */
wa.ui.Helpers = function() {
    return {
        init : function() {
            $("a.mor_module_foldout_trigger").live(
                    "click",
                    function(ev) {
                        ev.preventDefault();
                        $(".mor_module_foldout_content",
                                $(ev.target).closest(".mor_module_foldout"))
                                .toggleClass("visible");
                    });
        }
    };
}();