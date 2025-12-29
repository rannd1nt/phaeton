from typing import List, Dict, Optional, Literal, Union

# --- Type Definitions ---
HeaderCase = Literal["snake", "camel", "pascal", "kebab", "constant"]
ScrubMode = Literal["email", "html", "phone", "cc", "trim", "lower", "upper", "currency", "numeric_only"]
MatchMode = Literal["exact", "contains", "startswith", "endswith", "regex"]
DateFmt = Literal["iso", "us", "eu", "auto"]
CastType = Literal["int", "float", "str", "bool"]
ExportFormat = Literal["csv", "parquet", "json", "arrow"]
OnError = Literal["quarantine", "null", "ignore"]

class Pipeline:
    """
    Builder class for constructing data cleaning workflows.
    
    All methods in this class are lazy; they record instructions ("steps") 
    that are executed only when `.run()` or `Engine.exec()` is called.
    """
    def __init__(self, source: str, config: dict):
        self.source = source
        self.config = config
        self.steps: List[Dict] = []
        self.quarantine_path: Optional[str] = None
        self.output_target: Optional[str] = None
        self.output_format: str = "csv"

    # ==========================================
    # 1. UTILITY & PREVIEW
    # ==========================================

    def peek(self, n: int = 5):
        """
        Previews the first `n` rows of the pipeline result without full execution.
        
        Useful for debugging logic on large datasets.

        Args:
            n (int, optional): Number of rows to preview. Defaults to 5.

        Returns:
            List[Dict]: A list of dictionaries representing the transformed rows.
        """
        try:
            from . import _phaeton
            return _phaeton.preview_pipeline(self.source, self.steps, n)
        except ImportError:
            print(f"⚠️ [MOCK] Peeking {n} lines from {self.source}...")
            return [{"mock": "data"}] * n

    # ==========================================
    # 2. ELIMINATION (Row Filtering)
    # ==========================================

    def keep(self, col: str, match: str, mode: MatchMode = "exact") -> "Pipeline":
        """
        Retains only the rows where the column matches the pattern. All other rows are discarded.

        Args:
            col (str): The column to check.
            match (str): The value or pattern to match.
            mode (MatchMode, optional): Matching strategy ('exact', 'contains', 'regex', etc.). Defaults to "exact".
        """
        self.steps.append({"action": "keep", "col": col, "match": match, "mode": mode})
        return self

    def discard(self, col: str, match: str, mode: MatchMode = "exact") -> "Pipeline":
        """
        Discards rows where the column matches the pattern. The opposite of `keep`.

        Args:
            col (str): The column to check.
            match (str): The value or pattern to discard.
            mode (MatchMode, optional): Matching strategy. Defaults to "exact".
        """
        self.steps.append({"action": "discard", "col": col, "match": match, "mode": mode})
        return self

    def prune(self, col: Optional[str] = None) -> "Pipeline":
        """
        Prunes (drops) rows containing empty or NULL values.

        Args:
            col (Optional[str]): The specific column to check for emptiness. 
                If None, the row is dropped if *any* column is empty.
        """
        self.steps.append({"action": "prune", "col": col or "*"})
        return self

    # ==========================================
    # 3. TRANSFORMATION (Content Cleaning)
    # ==========================================

    def scrub(self, col: str, mode: ScrubMode) -> "Pipeline":
        """
        Applies string cleaning operations to a column.

        Modes:
            - 'currency': Removes currency symbols (Rp, $), thousand separators, and whitespace.
            - 'numeric_only': Removes all non-numeric characters.
            - 'html': Strips HTML tags.
            - 'trim': Trims leading/trailing whitespace.
            - 'email': Masks email addresses.

        Args:
            col (str): The column to scrub.
            mode (ScrubMode): The cleaning mode to apply.
        """
        self.steps.append({"action": "scrub", "col": col, "mode": mode})
        return self

    def fill(self, col: str, value: Union[str, int, float]) -> "Pipeline":
        """
        Imputes missing (empty/null) values with a specified default value.
        Crucial for preparing data for Machine Learning.

        Args:
            col (str): The column to check.
            value (Union[str, int, float]): The value to use for imputation.
        """
        self.steps.append({"action": "fill", "col": col, "value": value})
        return self

    def reformat(self, col: str, to_fmt: str = "%Y-%m-%d", from_fmt: DateFmt = "auto") -> "Pipeline":
        """
        Standardizes date/time strings to a specific format.

        Args:
            col (str): The column containing date strings.
            to_fmt (str, optional): Target format (Python strftime style). Defaults to "%Y-%m-%d".
            from_fmt (DateFmt, optional): Source format hint. Use 'auto' to let the engine guess. Defaults to "auto".
        """
        self.steps.append({"action": "reformat", "col": col, "to": to_fmt, "from": from_fmt})
        return self

    def fuzzyalign(self, col: str, ref: List[str], threshold: float = 0.85) -> "Pipeline":
        """
        Corrects typos by aligning values to a reference list using Fuzzy Logic (Jaro-Winkler).

        Example:
            fuzzyalign("city", ["Jakarta", "Amsterdam"]) will correct "Jkarta" to "Jakarta".

        Args:
            col (str): The column to align.
            ref (List[str]): A list of valid reference strings.
            threshold (float, optional): Similarity threshold (0.0 to 1.0). Defaults to 0.85.
        """
        self.steps.append({"action": "align", "col": col, "ref": ref, "threshold": threshold})
        return self

    def decode(self, encoding: str = "utf-8-sig") -> "Pipeline":
        """
        Forces the file to be read with a specific encoding.
        
        This step is prioritized and executed before reading the file content.

        Args:
            encoding (str): The encoding name (e.g., 'windows-1252', 'latin-1').
        """
        self.steps.insert(0, {"action": "decode", "encoding": encoding})
        return self

    # ==========================================
    # 4. STRUCTURAL MANIPULATION
    # ==========================================

    def rename(self, mapping: Dict[str, str]) -> "Pipeline":
        """
        Renames specific columns.

        Args:
            mapping (Dict[str, str]): A dictionary mapping old names to new names.
        """
        self.steps.append({"action": "rename", "mapping": mapping})
        return self

    def headers(self, style: HeaderCase = "snake") -> "Pipeline":
        """
        Standardizes the naming convention of all column headers.

        Args:
            style (HeaderCase): Target style ('snake', 'camel', 'pascal', 'kebab').
        """
        self.steps.append({"action": "headers", "style": style})
        return self

    def cast(self, col: str, type: CastType, clean: bool = False, on_error: OnError = "quarantine") -> "Pipeline":
        """
        Enforces a data type on a column.

        If `clean` is True, Phaeton attempts to strip non-numeric characters before casting 
        (e.g., "$ 5,000" -> 5000.0).

        Args:
            col (str): The column to cast.
            type (CastType): Target type ('int', 'float', 'str', 'bool').
            clean (bool, optional): Smart clean before casting. Defaults to False.
            on_error (OnError, optional): Action if cast fails ('quarantine', 'null', 'ignore'). Defaults to "quarantine".
        """
        self.steps.append({
            "action": "cast", 
            "col": col, 
            "type": type, 
            "clean": clean,
            "on_error": on_error
        })
        return self

    def split(self, col: str, delimiter: str, into: List[str]) -> "Pipeline":
        """
        Splits a single column into multiple columns based on a delimiter.

        Args:
            col (str): The source column.
            delimiter (str): The character to split by.
            into (List[str]): Names for the new columns.
        """
        self.steps.append({"action": "split", "col": col, "delimiter": delimiter, "into": into})
        return self

    def combine(self, cols: List[str], glue: str, into: str) -> "Pipeline":
        """
        Combines multiple columns into a single column.

        Args:
            cols (List[str]): List of columns to combine.
            glue (str): The separator string.
            into (str): The name of the new column.
        """
        self.steps.append({"action": "combine", "cols": cols, "glue": glue, "into": into})
        return self

    # ==========================================
    # 5. MAPPING & SECURITY
    # ==========================================

    def map(self, col: str, mapping: Dict[str, str], default: Optional[str] = None) -> "Pipeline":
        """
        Maps values in a column using a dictionary lookup.

        Args:
            col (str): The column to map.
            mapping (Dict[str, str]): Dictionary defining the mapping {old_val: new_val}.
            default (Optional[str]): Value to use if the key is not found.
        """
        self.steps.append({"action": "map", "col": col, "mapping": mapping, "default": default})
        return self

    def hash(self, col: str, salt: Optional[str] = None) -> "Pipeline":
        """
        Anonymizes data using SHA-256 hashing.

        Args:
            col (str): The column to hash (e.g., 'email', 'user_id').
            salt (Optional[str]): Optional salt string for added security. 
                If None, the engine uses a secure system-generated salt.
        """
        self.steps.append({"action": "hash", "col": col, "salt": salt})
        return self

    def dedupe(self, col: Optional[str] = None, strictness: float = 1.0) -> "Pipeline":
        """
        Removes duplicate rows.

        Args:
            col (Optional[str]): If provided, dedupes based on this specific column. 
                If None, dedupes based on the entire row.
            strictness (float): 1.0 for exact match. Lower values allow fuzzy deduplication.
        """
        self.steps.append({"action": "dedupe", "col": col, "strictness": strictness})
        return self

    # ==========================================
    # 6. OUTPUT CONFIGURATION
    # ==========================================

    def quarantine(self, path: str) -> "Pipeline":
        """
        Sets the target file for rejected rows (Audit Trail).

        Any row that fails a check (prune, discard, cast error) is written here 
        with an additional column `_phaeton_reason`.

        Args:
            path (str): Path to the quarantine CSV file.
        """
        self.quarantine_path = path
        return self

    def dump(self, path: str, format: ExportFormat = "parquet") -> "Pipeline":
        """
        Sets the target file for cleaned data.

        Args:
            path (str): Path to the output file.
            format (ExportFormat): Output format ('parquet', 'csv', 'json'). Defaults to "parquet".
        """
        self.output_target = path
        self.output_format = format
        self.steps.append({"action": "dump", "path": path, "format": format})
        return self

    def run(self):
        """
        Triggers a single pipeline execution immediately.
        
        For better performance with multiple files, use `engine.exec([pipelines])`.

        Returns:
            Dict[str, int]: A dictionary containing execution statistics.
        """
        try:
            from . import _phaeton
        except ImportError:
            print("[FATAL] Phaeton Core not found.")
            return None

        payload = {
            "source": self.source,
            "steps": self.steps,
            "quarantine": self.quarantine_path,
            "output": self.output_target,
            "format": self.output_format,
            "config": self.config
        }
        return _phaeton.execute_pipeline(payload)
    
    def __repr__(self):
        return f"<Phaeton Pipeline | Source: {self.source} | Steps: {len(self.steps)}>"