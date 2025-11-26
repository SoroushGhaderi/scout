"""Tests for link extractor"""

import pytest
from unittest.mock import Mock
from src.scrapers.aiscore.extractor import LinkExtractor
from src.scrapers.aiscore.models import MatchLink


class TestLinkExtractor:
    """Test LinkExtractor class"""
    
    def test_extract_from_a_tag(self, link_extractor):
        """Test extracting href from <a> tag"""
        container = Mock()
        container.tag_name = 'a'
        container.get_attribute.return_value = 'https://www.aiscore.com/football/match/123'
        
        result = link_extractor.extract_link(container)
        assert result == 'https://www.aiscore.com/football/match/123'
    
    def test_extract_from_data_href(self, link_extractor):
        """Test extracting from data-href attribute"""
        container = Mock()
        container.tag_name = 'div'
        container.get_attribute.side_effect = lambda attr: {
            'href': None,
            'data-href': 'https://www.aiscore.com/football/match/456',
            'data-url': None,
            'data-link': None,
            'onclick': None,
        }.get(attr)
        
        result = link_extractor._extract_from_data_attrs(container)
        assert result == 'https://www.aiscore.com/football/match/456'
    
    def test_normalize_url(self, link_extractor):
        """Test URL normalization"""
        # Test trailing slash removal
        assert link_extractor._normalize_url('https://example.com/match/123/') == 'https://example.com/match/123'
        
        # Test query parameter removal
        assert link_extractor._normalize_url('https://example.com/match/123?param=value') == 'https://example.com/match/123'
        
        # Test h2h suffix removal
        assert link_extractor._normalize_url('https://example.com/match/123/h2h') == 'https://example.com/match/123'
    
    @pytest.mark.parametrize('url,expected', [
        ('https://www.aiscore.com/football/match/123', True),
        ('https://www.aiscore.com/match/123', True),
        ('https://www.aiscore.com/football/match/123/h2h', False),
        ('https://www.aiscore.com/football/match/123/statistics', False),
        ('https://www.aiscore.com/football/odds', False),
        ('https://www.aiscore.com/football/predictions', False),
        ('', False),
        (None, False),
    ])
    def test_is_valid_match_url(self, link_extractor, url, expected):
        """Test URL validation"""
        assert link_extractor.is_valid_match_url(url) == expected
    
    def test_extract_match_id(self, link_extractor):
        """Test match ID extraction"""
        url = 'https://www.aiscore.com/football/match/123456'
        match_id = link_extractor.extract_match_id(url)
        assert match_id == '123456'
        
        # Test with trailing slash
        url = 'https://www.aiscore.com/football/match/123456/'
        match_id = link_extractor.extract_match_id(url)
        assert match_id == '123456'
        
        # Test with invalid suffix
        url = 'https://www.aiscore.com/football/match/123456/h2h'
        match_id = link_extractor.extract_match_id(url)
        assert match_id == '123456'


class TestMatchLink:
    """Test MatchLink model"""
    
    def test_create_match_link(self):
        """Test creating a match link"""
        link = MatchLink(
            url='https://www.aiscore.com/football/match/123',
            match_id='123',
            source_date='20251110'
        )
        
        assert link.url == 'https://www.aiscore.com/football/match/123'
        assert link.match_id == '123'
        assert link.source_date == '20251110'
    
    def test_match_link_auto_extract_id(self):
        """Test automatic match ID extraction"""
        link = MatchLink(
            url='https://www.aiscore.com/football/match/456',
            match_id='',
            source_date='20251110'
        )
        
        assert link.match_id == '456'
    
    def test_match_link_to_tuple(self):
        """Test converting to tuple"""
        link = MatchLink(
            url='https://www.aiscore.com/football/match/123',
            match_id='123',
            source_date='20251110'
        )
        
        tuple_data = link.to_tuple()
        assert len(tuple_data) == 4
        assert tuple_data[0] == link.url
        assert tuple_data[1] == link.match_id
        assert tuple_data[2] == link.source_date
    
    def test_match_link_validation(self):
        """Test validation errors"""
        with pytest.raises(ValueError):
            MatchLink(url='', match_id='123', source_date='20251110')
        
        with pytest.raises(ValueError):
            MatchLink(url='http://example.com', match_id='123', source_date='')

