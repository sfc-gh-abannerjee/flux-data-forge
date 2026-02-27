"""
Tests for SQL sanitization utilities.

Validates that validate_identifier() and sanitize_sql_value() properly
prevent SQL injection attacks.

Run with: pytest tests/test_sanitize.py -v
"""

import pytest
import os
import sys

# Add spcs_app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spcs_app'))

from utils.sanitize import validate_identifier, sanitize_sql_value


class TestValidateIdentifier:
    """Tests for validate_identifier()"""

    def test_simple_identifier(self):
        assert validate_identifier("MY_TABLE") == "MY_TABLE"

    def test_qualified_two_part(self):
        assert validate_identifier("MY_DB.MY_TABLE") == "MY_DB.MY_TABLE"

    def test_qualified_three_part(self):
        assert validate_identifier("MY_DB.PUBLIC.MY_TABLE") == "MY_DB.PUBLIC.MY_TABLE"

    def test_identifier_with_dollar(self):
        assert validate_identifier("TABLE$1") == "TABLE$1"

    def test_identifier_starting_with_underscore(self):
        assert validate_identifier("_INTERNAL") == "_INTERNAL"

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="non-empty"):
            validate_identifier("")

    def test_rejects_none(self):
        with pytest.raises(ValueError, match="non-empty"):
            validate_identifier(None)

    def test_rejects_sql_injection_semicolon(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("TABLE1; DROP TABLE users")

    def test_rejects_sql_injection_quotes(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("TABLE1' OR '1'='1")

    def test_rejects_sql_injection_comment(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("TABLE1--comment")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("MY TABLE")

    def test_rejects_parentheses(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("FUNC()")

    def test_rejects_four_part_qualifier(self):
        with pytest.raises(ValueError, match="Too many qualifier"):
            validate_identifier("A.B.C.D")

    def test_rejects_qualified_when_disallowed(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("DB.TABLE", allow_qualified=False)

    def test_strips_whitespace(self):
        assert validate_identifier("  MY_TABLE  ") == "MY_TABLE"

    def test_rejects_very_long_identifier(self):
        long_name = "A" * 256
        with pytest.raises(ValueError, match="too long"):
            validate_identifier(long_name)

    def test_accepts_max_length_identifier(self):
        name = "A" * 255
        assert validate_identifier(name) == name

    def test_rejects_starting_with_digit(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("1TABLE")


class TestSanitizeSqlValue:
    """Tests for sanitize_sql_value()"""

    def test_normal_string(self):
        assert sanitize_sql_value("hello") == "hello"

    def test_single_quote_escaped(self):
        assert sanitize_sql_value("O'Brien") == "O''Brien"

    def test_multiple_quotes(self):
        assert sanitize_sql_value("it's a 'test'") == "it''s a ''test''"

    def test_sql_injection_attempt(self):
        malicious = "'; DROP TABLE users; --"
        result = sanitize_sql_value(malicious)
        assert result == "''; DROP TABLE users; --"
        # When used as WHERE col = '{result}', the SQL becomes:
        # WHERE col = '''; DROP TABLE users; --'
        # which is a safe string literal, not executable SQL

    def test_integer_input(self):
        assert sanitize_sql_value(123) == "123"

    def test_empty_string(self):
        assert sanitize_sql_value("") == ""

    def test_unicode_characters(self):
        assert sanitize_sql_value("café") == "café"

    def test_newlines_preserved(self):
        assert sanitize_sql_value("line1\nline2") == "line1\nline2"


class TestSecurityIntegration:
    """Integration tests verifying SQL injection patterns are fixed"""

    def test_fastapi_app_imports_sanitize(self):
        """Verify fastapi_app.py imports the sanitize module"""
        app_path = os.path.join(
            os.path.dirname(__file__), '..', 'spcs_app', 'fastapi_app.py'
        )
        with open(app_path, 'r') as f:
            content = f.read()

        assert 'from utils.sanitize import' in content, \
            "fastapi_app.py must import sanitize utilities"

    def test_no_bare_excepts(self):
        """Verify no bare except: clauses remain"""
        import re
        app_path = os.path.join(
            os.path.dirname(__file__), '..', 'spcs_app', 'fastapi_app.py'
        )
        with open(app_path, 'r') as f:
            content = f.read()

        bare_excepts = re.findall(r'^\s+except:\s*$', content, re.MULTILINE)
        assert len(bare_excepts) == 0, \
            f"Found {len(bare_excepts)} bare except: clauses"

    def test_no_deprecated_utcnow(self):
        """Verify no datetime.utcnow() calls remain"""
        app_path = os.path.join(
            os.path.dirname(__file__), '..', 'spcs_app', 'fastapi_app.py'
        )
        with open(app_path, 'r') as f:
            content = f.read()

        assert 'datetime.utcnow()' not in content, \
            "datetime.utcnow() is deprecated; use datetime.now(timezone.utc)"

    def test_xss_escaped_in_alerts(self):
        """Verify error messages in alerts are HTML-escaped"""
        import re
        app_path = os.path.join(
            os.path.dirname(__file__), '..', 'spcs_app', 'fastapi_app.py'
        )
        with open(app_path, 'r') as f:
            content = f.read()

        # Find alert patterns with unescaped {e}
        unsafe = re.findall(r"alert\('Failed to .+?: \{e\}'\)", content)
        assert len(unsafe) == 0, \
            f"Found {len(unsafe)} XSS-vulnerable alert patterns"

    def test_html_import_present(self):
        """Verify html module is imported for XSS escaping"""
        app_path = os.path.join(
            os.path.dirname(__file__), '..', 'spcs_app', 'fastapi_app.py'
        )
        with open(app_path, 'r') as f:
            content = f.read()

        assert 'import html' in content, \
            "html module must be imported for XSS escaping"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
