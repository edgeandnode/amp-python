"""Exception classes for Amp DBT."""


class AmpDbtError(Exception):
    """Base exception for all Amp DBT errors."""

    pass


class ProjectNotFoundError(AmpDbtError):
    """Raised when a project directory is not found or invalid."""

    pass


class ConfigError(AmpDbtError):
    """Raised when configuration is invalid or missing."""

    pass


class CompilationError(AmpDbtError):
    """Raised when SQL compilation fails."""

    pass


class DependencyError(AmpDbtError):
    """Raised when dependency resolution fails."""

    pass

