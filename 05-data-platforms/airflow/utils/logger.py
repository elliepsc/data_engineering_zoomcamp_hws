"""
Structured Logger for Airflow Data Platform

Provides JSON-formatted logging for better observability in production environments.
This enables easy parsing by log aggregation tools (e.g., Datadog, CloudWatch, ELK).

Usage:
    from utils.logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("Task started", extra={'task_id': 'download_data', 'records': 1000})
"""

import logging
import json
import os
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """
    Custom formatter that outputs logs in JSON format.
    
    Benefits:
    - Structured data for analytics
    - Easy filtering in log aggregation tools
    - Consistent format across all tasks
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON string.
        
        Args:
            record: Python logging record
            
        Returns:
            JSON-formatted log string
        """
        log_data: Dict[str, Any] = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add extra fields if provided
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger with JSON formatting.
    
    Args:
        name: Logger name (typically __name__ of the calling module)
        
    Returns:
        Configured logger instance
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started", extra={'records': 1000})
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    # Set log level from environment or default to INFO
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logger.setLevel(getattr(logging, log_level))
    
    # Create console handler
    handler = logging.StreamHandler()
    handler.setLevel(logger.level)
    
    # Use JSON formatter for production, simple format for development
    log_format = os.getenv('LOG_FORMAT', 'json').lower()
    
    if log_format == 'json':
        formatter = JSONFormatter()
    else:
        # Simple format for local development
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


class LoggerAdapter(logging.LoggerAdapter):
    """
    Adapter to add consistent context to all log messages.
    
    Usage:
        logger = get_logger(__name__)
        task_logger = LoggerAdapter(logger, {'task_id': 'download_data'})
        task_logger.info("Started")  # Automatically includes task_id
    """
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Add extra fields from context to log record."""
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].update(self.extra)
        # Store in custom attribute for JSON formatter
        if 'extra_fields' not in kwargs['extra']:
            kwargs['extra']['extra_fields'] = {}
        kwargs['extra']['extra_fields'].update(self.extra)
        return msg, kwargs
