import json


class SchemaValidationException(Exception):
    """Exception for schema validation errors."""

    def __init__(
        self, error_dictionary: dict[str, list[str] | dict[str, dict[str, list[str]]]]
    ):
        """Serialize error message as a string"""
        super().__init__(error_dictionary)

    def __str__(self):
        return json.dumps(self.args[0])
