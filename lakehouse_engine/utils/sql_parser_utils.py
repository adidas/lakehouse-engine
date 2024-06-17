"""Module to parse sql files."""

from typing import Union

from lakehouse_engine.core.definitions import SQLParser


class SQLParserUtils(object):
    """Parser utilities class."""

    def split_sql_commands(
        self,
        sql_commands: str,
        delimiter: str,
        advanced_parser: bool,
    ) -> list[str]:
        """Read the sql commands of a file to choose how to split them.

        Args:
            sql_commands: commands to be split.
            delimiter: delimiter to split the sql commands.
            advanced_parser: boolean to define if we need to use a complex split.

        Returns:
            List with the sql commands.
        """
        if advanced_parser:
            self.sql_commands: str = sql_commands
            self.delimiter: str = delimiter
            self.separated_sql_commands: list[str] = []
            self.split_index: int = 0
            return self._split_sql_commands()
        else:
            return sql_commands.split(delimiter)

    def _split_sql_commands(self) -> list[str]:
        """Read the sql commands of a file to split them based on a delimiter.

        Returns:
            List with the sql commands.
        """
        single_quotes: int = 0
        double_quotes: int = 0
        one_line_comment: int = 0
        multiple_line_comment: int = 0

        for index, char in enumerate(self.sql_commands):
            if char == SQLParser.SINGLE_QUOTES.value and self._character_validation(
                value=[double_quotes, one_line_comment, multiple_line_comment]
            ):
                single_quotes = self._update_value(
                    value=single_quotes,
                    condition=self._character_validation(
                        value=self._get_substring(first_char=index - 1, last_char=index)
                    ),
                    operation="+-",
                )
            elif char == SQLParser.DOUBLE_QUOTES.value and self._character_validation(
                value=[single_quotes, one_line_comment, multiple_line_comment]
            ):
                double_quotes = self._update_value(
                    value=double_quotes,
                    condition=self._character_validation(
                        value=self._get_substring(first_char=index - 1, last_char=index)
                    ),
                    operation="+-",
                )
            elif char == SQLParser.SINGLE_TRACE.value and self._character_validation(
                value=[double_quotes, single_quotes, multiple_line_comment]
            ):
                one_line_comment = self._update_value(
                    value=one_line_comment,
                    condition=(
                        self._get_substring(first_char=index, last_char=index + 2)
                        == SQLParser.DOUBLE_TRACES.value
                    ),
                    operation="+",
                )
            elif (
                char == SQLParser.SLASH.value or char == SQLParser.STAR.value
            ) and self._character_validation(
                value=[double_quotes, single_quotes, one_line_comment]
            ):
                multiple_line_comment = self._update_value(
                    value=multiple_line_comment,
                    condition=self._get_substring(first_char=index, last_char=index + 2)
                    in SQLParser.MULTIPLE_LINE_COMMENT.value,
                    operation="+-",
                )

            one_line_comment = self._update_value(
                value=one_line_comment,
                condition=char == SQLParser.PARAGRAPH.value,
                operation="-",
            )

            self._validate_command_is_closed(
                index=index,
                dependencies=self._character_validation(
                    value=[
                        single_quotes,
                        double_quotes,
                        one_line_comment,
                        multiple_line_comment,
                    ]
                ),
            )

        return self.separated_sql_commands

    def _get_substring(self, first_char: int = None, last_char: int = None) -> str:
        """Get the substring based on the indexes passed as arguments.

        Args:
            first_char: represents the first index of the string.
            last_char: represents the last index of the string.

        Returns:
            The substring based on the indexes passed as arguments.
        """
        return self.sql_commands[first_char:last_char]

    def _validate_command_is_closed(self, index: int, dependencies: int) -> None:
        """Validate based on the delimiter if we have the closing of a sql command.

        Args:
            index: index of the character in a string.
            dependencies: represents an int to validate if we are outside of quotes,...
        """
        if (
            self._get_substring(first_char=index, last_char=index + len(self.delimiter))
            == self.delimiter
            and dependencies
        ):
            self._add_new_command(
                sql_command=self._get_substring(
                    first_char=self.split_index, last_char=index
                )
            )
            self.split_index = index + len(self.delimiter)

        if self._get_substring(
            first_char=index, last_char=index + len(self.delimiter)
        ) != self.delimiter and index + len(self.delimiter) == len(self.sql_commands):
            self._add_new_command(
                sql_command=self._get_substring(
                    first_char=self.split_index, last_char=len(self.sql_commands)
                )
            )

    def _character_validation(self, value: Union[str, list]) -> bool:
        """Validate if character is the opening/closing/inside of a comment.

        Args:
            value: represent the value associated to different validated
            types or a character to be analyzed.

        Returns:
            Boolean that indicates if character found is the opening
            or closing of a comment, is inside of quotes, comments,...
        """
        if value.__class__.__name__ == "list":
            return sum(value) == 0
        else:
            return value != SQLParser.BACKSLASH.value

    def _add_new_command(self, sql_command: str) -> None:
        """Add a newly found command to list of sql commands to execute.

        Args:
            sql_command: command to be added to list.
        """
        self.separated_sql_commands.append(str(sql_command))

    def _update_value(self, value: int, operation: str, condition: bool = False) -> int:
        """Update value associated to different types of comments or quotes.

        Args:
            value: value to be updated
            operation: operation that we want to perform on the value.
            condition: validate if we have a condition associated to the value.

        Returns:
            A integer that represents the updated value.
        """
        if condition and operation == "+-":
            value = value + 1 if value == 0 else value - 1
        elif condition and operation == "+":
            value = value + 1 if value == 0 else value
        elif condition and operation == "-":
            value = value - 1 if value == 1 else value

        return value
