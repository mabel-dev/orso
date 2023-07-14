# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class MissingDependencyError(Exception):
    def __init__(self, dependency):
        self.dependency = dependency
        message = f"No module named '{dependency}' can be found, "
        "please install or include in requirements.txt"
        super().__init__(message)


class DataError(Exception):
    pass


class DataValidationError(DataError):
    def __init__(self, column, value, error):
        self.field = column.name
        self.expected_type = column.type
        self.value = value
        self.nullable = column.nullable
        self.error = error

        text = str(value)
        truncated_text = (
            f"{text[:16]}{'...' if len(text) > 16 else ''}... [{len(text) - 16} more]"
            if len(text) > 16
            else text
        )

        message = (
            f"Data did not pass validation checks; field `{self.field}` "
            f"with value `{truncated_text}` did not pass "
            f"`{column.type}` {'(nullable)' if self.nullable else ''} check. "
            f"({error})"
        )
        super().__init__(message)


class ColumnDefinitionError(DataError):
    def __init__(self, attribute):
        self.attribute = attribute
        message = f"Column is missing attribute {attribute}"
