describe('typeaheadHighlight', function () {

  var highlightFilter;

  beforeEach(module('ui.bootstrap.typeahead'));
  beforeEach(inject(function (typeaheadHighlightFilter) {
    highlightFilter = typeaheadHighlightFilter;
  }));

  it('should higlight a match', function () {
    expect(highlightFilter('before match after', 'match')).toEqual('before <strong>match</strong> after');
  });

  it('should higlight a match with mixed case', function () {
    expect(highlightFilter('before MaTch after', 'match')).toEqual('before <strong>MaTch</strong> after');
  });

  it('should higlight all matches', function () {
    expect(highlightFilter('before MaTch after match', 'match')).toEqual('before <strong>MaTch</strong> after <strong>match</strong>');
  });

  it('should do nothing if no match', function () {
    expect(highlightFilter('before match after', 'nomatch')).toEqual('before match after');
  });

  it('should do nothing if no or empty query', function () {
    expect(highlightFilter('before match after', '')).toEqual('before match after');
    expect(highlightFilter('before match after', null)).toEqual('before match after');
    expect(highlightFilter('before match after', undefined)).toEqual('before match after');
  });

  it('issue 316 - should work correctly for regexp reserved words', function () {
    expect(highlightFilter('before (match after', '(match')).toEqual('before <strong>(match</strong> after');
  });

  it('issue 1777 - should work correctly with numeric values', function () {
    expect(highlightFilter(123, '2')).toEqual('1<strong>2</strong>3');
  });
});
