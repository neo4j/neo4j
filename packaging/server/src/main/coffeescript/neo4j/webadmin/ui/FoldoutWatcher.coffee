
define ->
  class FoldoutWatcher
    
    init : ->
      $("a.foldout_trigger").live "click", (ev) ->
        ev.preventDefault();
        $(".foldout_content", $(ev.target).closest(".foldout")).toggleClass "visible"
