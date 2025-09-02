import json
import math
import sys
import lancedb
import numpy as np

def check_value(value):
    # Check if the value is None
    if value is None:
        return False

    if isinstance(value, str):
        # if value is a string
        if value.lower() == "nan":  # check if the string is "nan"
            return False

    if isinstance(value, str):
        # if value is a string
        if value not in [None, "", "nan", "NaN"] and not (isinstance(value, float) and math.isnan(value)):
            return True
        else:
            return False
    else:
        # assuming value is a list of strings or similar iterable
        check_list = []
        for item in value:
            if item not in [None, "", "nan", "NaN"] and not (isinstance(item, float) and math.isnan(item)):
                check_list.append(True)
            else:
                check_list.append(False)

        # Return True if all items pass the check, False otherwise
        return all(check_list)

def check_filter(value):
    # print(f"check_filter: {value}")

    if value is None:
        return False

    if isinstance(value, str):
        if value.lower() == "nan" or value == "":
            return False

    if isinstance(value, float) and math.isnan(value):
        return False

    # turned off July 22 2024
    # if isinstance(value, list) and len(value) == 0:
    #     return False

    return True

def to_list(value):
    if isinstance(value, np.ndarray):
        # Convert ndarray to list which is serializable
        return value.tolist()
    else:
        return value

def jsonl_mode(source):
    print(f"JSONL mode: Processing data from lancedb table {source} to a file")

    dblocation = "./stores/lancedb"
    table_name = source

    # Connect to LanceDB
    db = lancedb.connect(dblocation)
    table = db[table_name]
    output_jsonl = f"./stores/solrInputFiles/{table_name}.jsonl"

    all_records = table.to_pandas().to_dict('records')

    # HACK  should be moved to augment?
    # convert drop geom, copy wkt column to geom
    for record in all_records:
        # Check if 'geom' column doesn't exist and 'wkt' column exists
        if 'wkt' in record and 'geom' not in record:
            # Add 'geom' column with value from 'wkt'
            record['geom'] = record['wkt']
        # If both columns exist, update 'geom' with 'wkt' value
        elif 'wkt' in record and 'geom' in record:
            record['geom'] = record['wkt']

    for record in all_records:
        if 'wkt' in record and 'the_geom' in record:
            record['the_geom'] = record['wkt']

    for record in all_records:
        if 'geojson' in record and 'geojson_geom' not in record:
            record['geojson_geom'] = record['geojson']

    for record in all_records:
        if 'txt_variableMeasured' in record and 'variableMeasured' not in record:
            record['variableMeasured'] = record['txt_variableMeasured']

    for record in all_records:
        if 'txt_educationalCredentialAwarded' in record and 'educationalCredentialAwarded' not in record:
            record['educationalCredentialAwarded'] = record['txt_educationalCredentialAwarded']

    # Convert 'the_geom' from list to string of its first element
    for record in all_records:
        if 'the_geom' in record:
            the_geom_value = record['the_geom']
            if isinstance(the_geom_value, list):
                if the_geom_value:  # If the list is not empty
                    record['the_geom'] = str(the_geom_value[0])
                else:  # If the list is empty
                    record['the_geom'] = "" # Convert to an empty string

    # print(all_records[100])
    # sys.exit(0)

    for dict_item in all_records: # Note: 'dict' was renamed to 'dict_item' to avoid shadowing built-in
        for key, value in dict_item.items():
            if isinstance(value, float):
                dict_item[key] = str(value)

    # Open JSON(L) file for writing
    with open(output_jsonl, 'w') as jsonlfile:
        # Convert each row to JSON
        for row in all_records:
            # Remove: None, nan, NaNTType
            filtered_row = {key: value for key, value in row.items() if check_filter(value)}

            # convert numpy ndarray to list
            converted_row = {key: to_list(value) for key, value in filtered_row.items()}

            # filtered_row = {key: value for key, value in row.items() if value not in [None, "", "nan", "NaN"] and not (
            #             isinstance(value, float) and math.isnan(value))}
            try:
                # Add a new "keys" entry that contains all other keys
                converted_row["keys"] = [
                    key for key, value in converted_row.items()
                    if check_value(value)
                ]

                # TODO 7/31/2025 add in txt_variableMeasured list item to see if it now shows up everywhere
                # if it does, then we must be removing it above, so revisit that code
                # if "txt_variableMeasured" not in converted_row:
                #     converted_row["txt_variableMeasured"] = []


                # Write the updated row JSON to the file with compact formatting
                jsonlfile.write(json.dumps(converted_row, separators=(',', ':')) + '\n')
            except Exception as e:
                print(f"Error processing row: {converted_row}, Exception: {str(e)}")

    print(f"Converted table {table_name} to {output_jsonl}")
