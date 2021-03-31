from pathlib import Path

# ----------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------Output Converter---------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------


def output_converter(in_path: str, col_names: str):
    combined_tsv = col_names
    search_path = Path(in_path)
    files_tbs = (file for file in search_path.iterdir() if file.is_file())
    for file in files_tbs:
        if file.name.endswith('.tsv'):
            with open(file) as f:
                for line in f.readlines():
                    combined_tsv += line


# ----------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------ TZ Offset Calculator ------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

def tz_offset_calc(location: str) -> int:
    eastern_tz = {"ME", "NH", "VT", "MA", "RI", "CT", "NJ", "NY", "PA", "WV", "DC",
                  "GA", "VA", "NC", "SC", "FL", "OH", "MI", "IN", "KY", "DE", "ON"}
    central_tz = {"WI", "MN", "ND", "SD", "NE", "IA", "MO", "IL", "KS", "OK", "AR",
                  "LA", "MA", "AL", "TX"}
    mountain_tz = {"NM", "CO", "WY", "MT", "ID", "UT", "AZ"}
    pacific_tz = {"WA", "OR", "NV", "CA"}

    state = location[-2:]

    if location[0:3] == "El P":
        return -25200000
    elif state in eastern_tz:
        return -18000000
    elif state in central_tz:
        return -21600000
    elif state in mountain_tz:
        return -25200000
    elif state in pacific_tz:
        return -28800000
    elif state == "AK":
        return -32400000
    elif state == "HI":
        return -36000000
