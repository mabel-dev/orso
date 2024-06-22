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
    def __init__(self, errors: dict):
        truncated_text = lambda text: (
            f"{text[:16]}{'...' if len(text) > 16 else ''}... [{len(text) - 16} more]"
            if len(text) > 16
            else text
        )

        self.errors = errors

        message = "Data did not pass validation checks; "
        for err, detail in errors.items():
            message += f"\n{err}: "
            if all(isinstance(e, str) for e in detail):
                message += ", ".join(f"`{d}`" for d in detail)
            else:
                message += ", ".join(
                    [
                        f"`{d[0]}` value of `{truncated_text(str(d[1]))}` is not a {d[2].name}"
                        for d in detail
                    ]
                )

        super().__init__(message)


class ExcessColumnsInDataError(DataError):
    def __init__(self, columns):
        self.columns = columns
        message = (
            f"Data did not pass validation checks; "
            f"Additional fields, not defined in the schema, were present in the record - "
            + ", ".join(columns)
        )
        super().__init__(message)


class ColumnDefinitionError(DataError):
    def __init__(self, attribute):
        self.attribute = attribute
        message = f"Column is missing attribute {attribute}"
        super().__init__(message)
