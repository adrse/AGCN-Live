import pytest
from unittest.mock import patch, MagicMock
from backend.product_link import validate_product_link, resolve_redirect, ProductPageError


def test_direct_shopee_url_still_works():
    result = validate_product_link("https://shopee.com.br/product-i.123456.789")
    assert result["platform"] == "shopee"
    assert result["item_id"] == "789"


def test_direct_tiktok_url_still_works():
    result = validate_product_link("https://shop.tiktok.com/br/pdp/123456789")
    assert result["platform"] == "tiktok"
    assert result["item_id"] == "123456789"


@patch("backend.product_link.urlopen")
def test_short_shopee_resolves(mock_urlopen):
    mock_response = MagicMock()
    mock_response.geturl.return_value = "https://shopee.com.br/product-i.123456.789"
    mock_urlopen.return_value = mock_response

    result = validate_product_link("https://s.shopee.com.br/BU2jyrNid")
    assert result["platform"] == "shopee"


@patch("backend.product_link.urlopen")
def test_short_tiktok_resolves(mock_urlopen):
    mock_response = MagicMock()
    mock_response.geturl.return_value = "https://shop.tiktok.com/br/pdp/123456789"
    mock_urlopen.return_value = mock_response
    mock_urlopen.return_value.__enter__ = lambda s: mock_response
    mock_urlopen.return_value.__exit__ = lambda s, *args: None

    result = validate_product_link("https://vt.tiktok.com/ZS9AHbTHKcoJp-2HMVp/")
    assert result["platform"] == "tiktok"


@patch("backend.product_link.urlopen")
def test_redirect_to_unsafe_host_rejected(mock_urlopen):
    mock_response = MagicMock()
    mock_response.geturl.return_value = "https://127.0.0.1/product"
    mock_urlopen.return_value = mock_response

    with pytest.raises(ProductPageError):
        validate_product_link("https://s.shopee.com.br/malicious")


def test_invalid_scheme_rejected():
    with pytest.raises(ProductPageError):
        validate_product_link("http://shopee.com.br/product")


def test_unsupported_domain_rejected():
    with pytest.raises(ProductPageError):
        validate_product_link("https://example.com/product")
