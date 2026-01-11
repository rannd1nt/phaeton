from typing import List, Dict, Optional, Literal, Union, Any, get_args
from phaeton.exceptions import ValueError as PhaetonValueError, EngineError, ConfigurationError, SchemaError
from tabulate import tabulate
from ._internal import ACCESS_TOKEN
import copy

# --- Type Definitions ---
HeaderCase = Literal["snake", "camel", "pascal", "kebab", "constant"]
ScrubMode = Literal["email", "html", "trim", "lower", "upper", "currency", "numeric_only"]
MatchMode = Literal["exact", "contains", "startswith", "endswith", "regex"]
FillMethod = Literal["fixed", "ffill"]
DateFmt = Literal["iso", "us", "eu", "auto"]
CastType = Literal["int", "float", "str", "bool"]
ExportFormat = Literal["csv", "parquet", "arrow"]
OnError = Literal["quarantine", "null", "ignore"]

class Pipeline:
    """
    Builder class for constructing data cleaning workflows.
    
    This class employs a Lazy Builder pattern: methods record instructions ("steps") 
    but do not execute them immediately. Execution is triggered only via `.run()`, 
    `Engine.exec()`, or `.peek()`.
    
    Attributes:
        alias (str): A unique identifier for tracking this pipeline in logs/previews.
    """
    def __init__(self, source: str, config: dict, alias: str, token: Any = None):

        if token is not ACCESS_TOKEN:
            raise PermissionError(
                "Pipeline cannot be instantiated directly. "
                "You must use 'eng.ingest()' to create a pipeline."
            )
        
        self._source = source
        self._config = config
        self._steps: List[Dict] = []
        self._quarantine_path: Optional[str] = None
        self._output_target: Optional[str] = None
        self._output_format: str = "csv"

        self._strict = config.get("strict", False)
        self._alias = alias 
        self._child_counter = 0
        self._has_peeked: bool = False

    def _validate(self) -> None:
        """
        Internal method to perform strict schema and parameter validation.
        
        This method will RAISE an exception immediately if any check fails.
        It is NOT responsible for printing or returning booleans.
        """
        try:
            from . import probe
        except ImportError:
            return

        VALIDATION_RULES = {
            "scrub":   {"mode": get_args(ScrubMode)},
            "keep":    {"mode": get_args(MatchMode)},
            "discard": {"mode": get_args(MatchMode)},
            "fill":    {"method": get_args(FillMethod)},
            
            "cast":    {
                "type": get_args(CastType),
                "on_error": get_args(OnError) 
            },
            
            "headers": {"style": get_args(HeaderCase)},
            
            "dump": {"format": get_args(ExportFormat)}
        }

        try:
            meta = probe(self._source)
        except Exception as e:
            raise EngineError(f"[{self._alias}] Source Error: Could not read '{self._source}'. {e}")

        if not meta or "headers" not in meta:
            raise SchemaError(f"[{self._alias}] Invalid Schema: No headers found in '{self._source}'.")

        raw_headers = meta["headers"]
        if isinstance(raw_headers, str):
            actual_headers = [h.strip() for h in raw_headers.split(",")]
        else:
            actual_headers = raw_headers

        for step in self._steps:
            action = step.get("action")
            
            if action in VALIDATION_RULES:
                for param_name, allowed_values in VALIDATION_RULES[action].items():
                    user_val = step.get(param_name)
                    
                    if user_val is None:
                        continue

                    if user_val not in allowed_values:
                        raise PhaetonValueError(
                            f"[{self._alias}] Invalid parameter '{param_name}' value '{user_val}' for action '{action}'. "
                            f"Allowed: {allowed_values}"
                        )

            col_target = step.get("col")
            
            if not col_target or col_target == "*":
                continue
            
            targets = col_target if isinstance(col_target, list) else [col_target]
            
            for col in targets:
                if action in {"combine", "split"}: 
                    continue
                
                if col not in actual_headers:
                    raise SchemaError(
                        f"[{self._alias}] Schema Mismatch! Column '{col}' not found in source. "
                        f"Available: {actual_headers}"
                    )
                
    def _prepare_match_pattern(self, match: Union[str, int, float, List, tuple], mode: str) -> tuple:
        if mode == 'regex' and isinstance(match, (list, tuple)):
            raise PhaetonValueError("Regex mode does not support list input directly.")

        if isinstance(match, (list, tuple)):
            return [str(m) for m in match], mode
            
        return str(match), mode
    
    # ==========================================
    # 1. UTILITY & PREVIEW
    # ==========================================

    def peek(self, n: int = 5, col: Union[str, List[str], None] = None) -> "Pipeline":
        """
        Executes a DRY RUN to preview the cleaning results.
        
        This runs the actual Rust pipeline on a stream of data until `n` valid rows 
        are collected, then prints them in a formatted table. 
        
        **Note:** This does not write to disk. It is safe to chain `.peek()` before `.dump()`.

        Args:
            n (int): Number of kept/valid rows to display. Defaults to 5.
            col (Union[str, List[str], None]): Specific column(s) to inspect. 
                If None, displays all columns.
        """
        if self._strict:
            self._validate()

        try:
            from . import _phaeton

            if not isinstance(n, int):
                raise PhaetonValueError("Parameter 'n' must be an integer.")

            n = 5 if n <= 0 else n
            target_cols = [col] if isinstance(col, str) else col
            headers, rows = _phaeton.preview_pipeline(self._source, self._steps, n, target_cols)
            
            if not rows:
                print(f"WARN: Pipeline Result is empty for {self._source}")
            else:
                display_cols = col if col else "All Columns"
                print(f"\nPREVIEW: {self._source} [{self._alias}]") 
                print(f"(Top {n} rows | Cols: {display_cols})")
                print(tabulate(rows, headers=headers, tablefmt="rounded_outline", disable_numparse=True))

        except ImportError as e:
            raise EngineError("Phaeton Rust Core Missing!") from e

        self._has_peeked = True 
        return self

    # ==========================================
    # 2. ELIMINATION (Row Filtering)
    # ==========================================

    def keep(self, col: str,
            match: Union[str, int, float, List, tuple], mode: MatchMode = "exact") -> "Pipeline":
        """
        Retains only the rows where the specified column matches the pattern.
        
        **Strict Mode**: If the target column is missing or null, the row is 
        automatically discarded (or quarantined).

        Args:
            col (str): The column to evaluate.
            match (Union[str, int, float]): The value or pattern to match.
            mode (MatchMode): The matching strategy:
                - 'exact': Strict equality.
                - 'contains': Substring check.
                - 'startswith' / 'endswith': Prefix/Suffix check.
                - 'regex': Rust-flavored Regex pattern.
        """
        match, mode = self._prepare_match_pattern(match, mode)

        self._steps.append({"action": "keep", "col": col, "match": match, "mode": mode})
        return self

    def discard(self, col: str,
                match: Union[str, int, float, List, tuple], mode: MatchMode = "exact") -> "Pipeline":
        """
        Discards rows where the specified column matches the pattern. 
        The inverse operation of `keep`.

        **Strict Mode**: If the target column is missing or null, it is treated 
        as an error state and quarantined.

        Args:
            col (str): The column to check.
            match (str): The value or pattern to discard.
            mode (MatchMode, optional): Matching strategy. Defaults to "exact".
        """
        match, mode = self._prepare_match_pattern(match, mode)

        self._steps.append({"action": "discard", "col": col, "match": match, "mode": mode})
        return self

    def prune(self, col: Union[str, List[str], None] = None) -> "Pipeline":
        """
        Drops rows containing empty or NULL strings.
        
        Use this to remove rows that are 
        missing essential keys (e.g., Transaction ID, User Email).

        Args:
            col (Union[str, List[str], None]): 
                - None: Drops row if *ANY* column is empty.
                - str: Drops row if the *specific* column is empty.
                - List[str]: Drops row if *ANY* of the specified columns are empty.
        """
        target = col if col is not None else "*"
        self._steps.append({"action": "prune", "col": target})
        return self

    # ==========================================
    # 3. TRANSFORMATION (Content Cleaning)
    # ==========================================

    def scrub(self, col: str, mode: ScrubMode) -> "Pipeline":
        """
        Applies advanced string sanitization to a column.

        Modes:
            - 'currency': Uses **Skeleton Extraction** to handle global formats 
                (US, EU, ID), converts string to a float-ready format.
            - 'numeric_only': Aggressively strips non-digits (e.g., for Phone/NIK).
            - 'html': Efficiently strips HTML tags.
            - 'email': Masks email username for privacy (e.g., 'j***e@gmail.com').
            - 'trim': Removes leading/trailing whitespace.
            - 'lower' / 'upper': Case conversion.

        Args:
            col (str): The target column.
            mode (ScrubMode): The cleaning mode to apply.
        """
        self._steps.append({"action": "scrub", "col": col, "mode": mode})
        return self

    def fill(self, col: str, value: Union[str, int, float, None] = None, method: FillMethod = 'fixed') -> "Pipeline":
        """
        Imputes missing (empty/null/whitespace) values.
        
        Args:
            col (str): The column to check.
            value (Any): The constant value to use (required if method='fixed').
            method (str): 
                - 'fixed': Replaces empty strings with the provided `value`.
                - 'ffill': Forward Fill. Replaces empty strings with the last valid 
                    value seen in the stream. Ideal for time-series or sensor data.
        """
        if method == "fixed" and value is None:
            raise PhaetonValueError("Must provide a 'value' when method='fixed'.")
        self._steps.append({"action": "fill", "col": col, "value": value, "method": method})
        return self

    def reformat(self, col: str, to_fmt: str = "%Y-%m-%d", from_fmt: DateFmt = "auto") -> "Pipeline":
        """
        [NOT IMPLEMENTED] Parses and standardizes date/time strings into a unified format.

        Args:
            col (str): The column containing date strings.
            to_fmt (str): Target Python strftime format (e.g., "%Y-%m-%d").
            from_fmt (DateFmt): Hint for source format ('iso', 'us', 'eu', 'auto').
        """
        print(f"WARN: .reformat() is currently a placeholder in v0.3.0")
        self._steps.append({"action": "reformat", "col": col, "to": to_fmt, "from": from_fmt})
        return self

    def fuzzyalign(self, col: str, ref: List[str], threshold: float = 0.85) -> "Pipeline":
        """
        Corrects typos by aligning values to a reference list using Jaro-Winkler distance.
        
        Useful for fixing categorical inconsistencies (e.g. "Jkrta" -> "Jakarta").

        Args:
            col (str): The column to align.
            ref (List[str]): A list of valid canonical strings.
            threshold (float): Similarity threshold (0.0 to 1.0). Defaults to 0.85.
        """
        self._steps.append({"action": "align", "col": col, "ref": ref, "threshold": threshold})
        return self

    def decode(self, encoding: str = "utf-8-sig") -> "Pipeline":
        """
        Enforces a specific character encoding during file ingestion.
        Executed before any other step.

        Args:
            encoding (str): Encoding name (e.g., 'utf-8', 'windows-1252', 'latin-1').
        """
        self._steps.insert(0, {"action": "decode", "encoding": encoding})
        return self

    # ==========================================
    # 4. STRUCTURAL MANIPULATION
    # ==========================================

    def rename(self, mapping: Dict[str, str]) -> "Pipeline":
        """
        Renames specific columns in the output.
        
        This operation is applied to the headers *before* writing to the file.
        It does not affect column reference names in other pipeline steps.

        Args:
            mapping (Dict[str, str]): A dictionary mapping {old_name: new_name}.
        """
        self._steps.append({"action": "rename", "mapping": mapping})
        return self

    def headers(self, style: HeaderCase = "snake") -> "Pipeline":
        """
        Standardizes all output column headers to a specific casing style.
        
        Args:
            style (HeaderCase): ('snake', 'camel', 'pascal', 'kebab', 'constant').
        """
        self._steps.append({"action": "headers", "style": style})
        return self

    def cast(self, col: str, dtype: CastType, clean: bool = False, on_error: OnError = "quarantine") -> "Pipeline":
        """
        Enforces data type on a column (String, Integer, Float, Boolean).

        If `clean` is True, Phaeton attempts to strip non-numeric characters before casting 
        (e.g., "$ 5,000" -> 5000.0).

        Args:
            col (str): The target column.
            type (CastType): The target data type.
            clean (bool): If True, applies intelligent scrubbing before casting 
                (e.g., stripping currency symbols from "$ 5,000" before float cast).
            on_error (OnError, optional): Action if cast fails ('quarantine', 'null', 'ignore'). Defaults to "quarantine".
        """
        self._steps.append({
            "action": "cast", 
            "col": col, 
            "type": dtype, 
            "clean": clean,
            "on_error": on_error
        })
        return self

    def split(self, col: str, delimiter: str, into: List[str]) -> "Pipeline":
        """
        [NOT IMPLEMENTED] Splits a single column into multiple columns based on a delimiter.

        Args:
            col (str): The source column.
            delimiter (str): The character to split by.
            into (List[str]): Names for the new columns.
        """
        print(f"WARN: .split() is currently a placeholder in v0.3.0")
        self._steps.append({"action": "split", "col": col, "delimiter": delimiter, "into": into})
        return self

    def combine(self, cols: List[str], glue: str, into: str) -> "Pipeline":
        """
        [NOT IMPLEMENTED] Combines multiple columns into a single column.

        Args:
            cols (List[str]): List of columns to combine.
            glue (str): The separator string.
            into (str): The name of the new column.
        """
        print(f"WARN: .combine() is currently a placeholder in v0.3.0")
        self._steps.append({"action": "combine", "cols": cols, "glue": glue, "into": into})
        return self

    # ==========================================
    # 5. MAPPING & SECURITY
    # ==========================================

    def map(self, col: str, mapping: Dict[str, str], default: Optional[str] = None) -> "Pipeline":
        """
        Maps values in a column using a dictionary lookup (VLOOKUP style).

        Args:
            col (str): The column to map.
            mapping (Dict[str, str]): Dictionary defining the mapping {old_val: new_val}.
            default (Optional[str]): Value to use if the key is not found in mapping.
                If None, original value is kept.
        """
        self._steps.append({"action": "map", "col": col, "mapping": mapping, "default": default})
        return self

    def hash(self, col: Union[str, List[str]], salt: Optional[str] = None) -> "Pipeline":
        """
        Anonymizes data in the specified column(s) using SHA-256 hashing.
        
        The output will be a hexadecimal string. This is a one-way transformation
        intended for pseudonymization (GDPR compliance) or creating unique keys.

        Args:
            col (Union[str, List[str]]): The column(s) to hash. 
                Cannot be None (must specify target).
            salt (Optional[str]): Optional salt string to prevent Rainbow Table attacks.
                If None, uses a secure default system salt.
        """
        if col is None:
            raise PhaetonValueError("Hash step requires a target column (str or list).")
        
        self._steps.append({"action": "hash", "col": col, "salt": salt})
        return self

    def dedupe(self, col: Union[str, List[str], None] = None) -> "Pipeline":
        """
        Removes duplicate rows based on unique content.
        
        Uses an efficient in-memory sharded HashSet to track seen rows.

        Args:
            col (Union[str, List[str], None]): 
                - None: Dedupe based on ALL columns (entire row must be identical).
                - str: Dedupe based on a SINGLE column (e.g., 'id').
                - List[str]: Dedupe based on a COMBINATION of columns (Composite Key).
        """

        self._steps.append({"action": "dedupe", "col": col})
        return self

    # ==========================================
    # 6. OUTPUT CONFIGURATION
    # ==========================================

    def quarantine(self, path: str) -> "Pipeline":
        """
        Defines the output path for 'Quarantine' (Rejected) rows.
        
        Rows rejected by strict filters or type errors will be saved here 
        with an extra `_phaeton_reason` column.
        """
        self._quarantine_path = path
        return self

    def dump(self, path: str, format: ExportFormat = "csv") -> "Pipeline":
        """
        Defines the final output destination for Cleaned Data.

        Args:
            path (str): Target file path.
            format (ExportFormat): File format ('parquet', 'csv', 'arrow'). Defaults to "csv".
        """
        self._output_target = path
        self._output_format = format
        self._steps.append({"action": "dump", "path": path, "format": format})
        return self

    def fork(self, tag: Optional[str] = None) -> "Pipeline":
        """
        Creates a deep copy of the current pipeline state. 
        Inherits all previous steps but RESETS output targets.
        Useful for creating multiple output branches from a shared preprocessing base.

        Args:
            tag (str, optional): Label for the new branch. 
                If None, auto-generates a name like "PIPE-1 - Fork-1 - Fork-1".
        """

        self._child_counter += 1
        
        if tag:
            suffix = tag
        else:
            suffix = f"Fork-{self._child_counter}"
            
        child_alias = f"{self._alias} - {suffix}"

        new_obj = copy.copy(self)
        new_obj._steps = copy.deepcopy(self._steps)
        
        new_obj._output_target = None 
        new_obj._quarantine_path = None 
        new_obj._has_peeked = False 
        
        new_obj._alias = child_alias
        new_obj._child_counter = 0 
        
        return new_obj
    
    def run(self):
        """
        Triggers execution for this single pipeline.
        For parallel execution of multiple pipelines, use `engine.exec([p1, p2])`.
        """
        if self._strict:
            self._validate()

        if not self._output_target and not self._quarantine_path and not self._has_peeked:
            raise ConfigurationError(
                f"Pipeline {self._alias} (source: {self._source}) has no output defined. "
                "Call .dump() or .quarantine() before .run()."
            )

        try:
            from . import _phaeton
        except ImportError:
            raise EngineError("Phaeton Rust Core not found. Cannot execute pipeline.")

        payload = {
            "source": self._source,
            "steps": self._steps,
            "quarantine": self._quarantine_path,
            "output": self._output_target,
            "format": self._output_format,
            "config": self._config
        }
        return _phaeton.execute_pipeline(payload)
    
    def __repr__(self):
        return f"<Phaeton Pipeline | Source: {self._source} | Steps: {len(self._steps)}>"