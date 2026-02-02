# Contributing to Flux Data Forge

Thank you for your interest in contributing to Flux Data Forge!

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Create a feature branch (`git checkout -b feature/amazing-feature`)

## Development Setup

```bash
# Clone and setup
git clone https://github.com/YOUR_USERNAME/flux-data-forge.git
cd flux-data-forge

# Copy environment template
cp .env.example .env
# Edit .env with your Snowflake configuration

# Install dependencies
cd spcs_app
pip install -r requirements.txt

# Run tests
cd ../tests
pytest
```

## Code Standards

- **Python**: Follow PEP 8 style guidelines
- **SQL**: Use uppercase keywords, lowercase identifiers
- **Commits**: Use conventional commit messages (`feat:`, `fix:`, `docs:`)

## Pull Request Process

1. Ensure tests pass locally
2. Update documentation if needed
3. Submit PR with clear description of changes
4. Link related issues

## Testing

```bash
# Unit tests
pytest tests/test_unit.py

# Smoke tests (requires Snowflake connection)
pytest tests/smoke_test.py
```

## Reporting Issues

- Use GitHub Issues for bugs and feature requests
- Include reproduction steps and environment details
- For security issues, see [SECURITY.md](./SECURITY.md)

## Related Repositories

Flux Data Forge is part of the Flux Utility Platform:

| Repository | Purpose |
|------------|---------|
| [Flux Utility Solutions](https://github.com/sfc-gh-abannerjee/flux-utility-solutions) | Core platform with Cortex AI |
| **Flux Data Forge** | Synthetic data generation (this repo) |
| [Flux Ops Center](https://github.com/sfc-gh-abannerjee/flux-ops-center-spcs) | Grid visualization |

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
