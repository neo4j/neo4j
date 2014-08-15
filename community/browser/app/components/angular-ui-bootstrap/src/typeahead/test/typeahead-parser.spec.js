describe('syntax parser', function () {

  var typeaheadParser, scope, filterFilter;

  beforeEach(module('ui.bootstrap.typeahead'));
  beforeEach(inject(function (_$rootScope_, _filterFilter_, _typeaheadParser_) {
    typeaheadParser = _typeaheadParser_;
    scope = _$rootScope_;
    filterFilter = _filterFilter_;
  }));

  it('should parse the simplest array-based syntax', function () {
    scope.states = ['Alabama', 'California', 'Delaware'];
    var result = typeaheadParser.parse('state for state in states | filter:$viewValue');

    var itemName = result.itemName;
    var locals = {$viewValue:'al'};
    expect(result.source(scope, locals)).toEqual(['Alabama', 'California']);

    locals[itemName] = 'Alabama';
    expect(result.viewMapper(scope, locals)).toEqual('Alabama');
    expect(result.modelMapper(scope, locals)).toEqual('Alabama');
  });

  it('should parse the simplest function-based syntax', function () {
    scope.getStates = function ($viewValue) {
      return filterFilter(['Alabama', 'California', 'Delaware'], $viewValue);
    };
    var result = typeaheadParser.parse('state for state in getStates($viewValue)');

    var itemName = result.itemName;
    var locals = {$viewValue:'al'};
    expect(result.source(scope, locals)).toEqual(['Alabama', 'California']);

    locals[itemName] = 'Alabama';
    expect(result.viewMapper(scope, locals)).toEqual('Alabama');
    expect(result.modelMapper(scope, locals)).toEqual('Alabama');
  });

  it('should allow to specify custom model mapping that is used as a label as well', function () {

    scope.states = [
      {code:'AL', name:'Alabama'},
      {code:'CA', name:'California'},
      {code:'DE', name:'Delaware'}
    ];
    var result = typeaheadParser.parse('state.name for state in states | filter:$viewValue | orderBy:"name":true');

    var itemName = result.itemName;
    expect(itemName).toEqual('state');
    expect(result.source(scope, {$viewValue:'al'})).toEqual([
      {code:'CA', name:'California'},
      {code:'AL', name:'Alabama'}
    ]);

    var locals = {$viewValue:'al'};
    locals[itemName] = {code:'AL', name:'Alabama'};
    expect(result.viewMapper(scope, locals)).toEqual('Alabama');
    expect(result.modelMapper(scope, locals)).toEqual('Alabama');
  });

  it('should allow to specify custom view and model mappers', function () {

    scope.states = [
      {code:'AL', name:'Alabama'},
      {code:'CA', name:'California'},
      {code:'DE', name:'Delaware'}
    ];
    var result = typeaheadParser.parse('state.code as state.name + " ("+state.code+")" for state in states | filter:$viewValue | orderBy:"name":true');

    var itemName = result.itemName;
    expect(result.source(scope, {$viewValue:'al'})).toEqual([
      {code:'CA', name:'California'},
      {code:'AL', name:'Alabama'}
    ]);

    var locals = {$viewValue:'al'};
    locals[itemName] = {code:'AL', name:'Alabama'};
    expect(result.viewMapper(scope, locals)).toEqual('Alabama (AL)');
    expect(result.modelMapper(scope, locals)).toEqual('AL');
  });
});