"""Tests for l in k extractor"""

import pytest
from unittest.mock import Mock
from src.scrapers.aiscore.extractorimport LkExtract or
from src.scrapers.aiscore.models import MatchLk


class TestLkExtract or:
    """Test LkExtractorclass"""

    def test_extract_ from _a_tag(self,lk_extractor):
        """Test extractg href from <a> tag"""
contaer = Mock()
contaer.tag_name='a'
contaer.get_attribute.return_value='https://www.aiscore.com/football/match/123'

result = lk_extractor.extract_lk(contaer)
assert result=='https://www.aiscore.com/football/match/123'

    def test_extract_ from _data_href(self,lk_extractor):
        """Test extractg from data-href attribute"""
contaer = Mock()
contaer.tag_name='div'
contaer.get_attribute.side_effect = lambda attr:{
'href':None,
'data-href':'https://www.aiscore.com/football/match/456',
'data-url':None,
'data-lk':None,
'onclick':None,
}.get(attr)

result = lk_extractor._extract_ from _data_attrs(contaer)
assert result=='https://www.aiscore.com/football/match/456'

    def test_normalize_url(self,lk_extractor):
        """Test URL normalization"""

assert lk_extractor._normalize_url('https://example.com/match/123/')=='https://example.com/match/123'


assert lk_extractor._normalize_url('https://example.com/match/123?param = value')=='https://example.com/match/123'


assert lk_extractor._normalize_url('https://example.com/match/123/h2h')=='https://example.com/match/123'

@pytest.mark.parametrize('url,expected',[
('https://www.aiscore.com/football/match/123',True),
('https://www.aiscore.com/match/123',True),
('https://www.aiscore.com/football/match/123/h2h',False),
('https://www.aiscore.com/football/match/123/statistics',False),
('https://www.aiscore.com/football/odds',False),
('https://www.aiscore.com/football/predictions',False),
('',False),
(None,False),
])
    def test_is_valid_match_url(self,lk_extractor,url,expected):
        """Test URL validation"""
assert lk_extractor.is _valid_match_url(url)== expected

    def test_extract_match_id(self,lk_extractor):
        """Test match ID extraction"""
url='https://www.aiscore.com/football/match/123456'
match_id = lk_extractor.extract_match_id(url)
assert match_id=='123456'


url='https://www.aiscore.com/football/match/123456/'
match_id = lk_extractor.extract_match_id(url)
assert match_id=='123456'


url='https://www.aiscore.com/football/match/123456/h2h'
match_id = lk_extractor.extract_match_id(url)
assert match_id=='123456'


class TestMatchLk:
    """Test MatchLk mo del"""

    def test_create_match_lk(self):
        """Test creatg a match lk"""
lk = MatchLk(
url='https://www.aiscore.com/football/match/123',
match_id='123',
source_date='20251110'
)

assert lk.url=='https://www.aiscore.com/football/match/123'
assert lk.match_id=='123'
assert lk.source_date=='20251110'

    def test_match_lk_auto_extract_id(self):
        """Test automatic match ID extraction"""
lk = MatchLk(
url='https://www.aiscore.com/football/match/456',
match_id='',
source_date='20251110'
)

assert lk.match_id=='456'

    def test_match_lk_to_tuple(self):
        """Test convertg to tuple"""
lk = MatchLk(
url='https://www.aiscore.com/football/match/123',
match_id='123',
source_date='20251110'
)

tuple_data = lk.to_tuple()
assert len(tuple_data)==4
assert tuple_data[0]== lk.url
assert tuple_data[1]== lk.match_id
assert tuple_data[2]== lk.source_date

    def test_match_lk_validation(self):
        """Test validation error s"""
with pytest.raises(ValueError):
            MatchLk(url='',match_id='123',source_date='20251110')

with pytest.raises(ValueError):
            MatchLk(url='http://example.com',match_id='123',source_date='')
