import json
import os

import pandas as pd
import pytest

from services.ssr import (_get_dataset_stats, create_ssr_parsed_file,
                          load_sample_data, remove_ssr_parsed_files)

OS_PATH_EXISTS = "os.path.exists"
OS_REMOVE = "os.remove"
DF_LOC = "services.ssr.DF_LOC"


def test_remove_ssr_parsed_files(mocker):
    # Assert files exist and are removed
    mocker.patch(OS_PATH_EXISTS, return_value=True)
    mocker.patch(OS_REMOVE)
    result = remove_ssr_parsed_files("dxtmp_name")
    assert os.remove.call_count == 2
    assert result == "Success"
    # Assert the ds_name was changed to tmp_name
    os.remove.assert_any_call("./parsed-data-files/tmp_name.json")
    os.path.exists.reset_mock()
    os.remove.reset_mock()

    # Assert files don't exist and no files are removed
    mocker.patch(OS_PATH_EXISTS, return_value=False)
    mocker.patch(OS_REMOVE)
    result = remove_ssr_parsed_files("tmp_name")
    assert os.remove.call_count == 0
    assert result == "Success"

    # Assert exception is raised
    mocker.patch(OS_PATH_EXISTS, side_effect=Exception("Test Exception"))
    result = remove_ssr_parsed_files("tmp_name")
    assert result == "Sorry, something went wrong in our SSR update. Contact the admin for more information."
    os.path.exists.reset_mock()
    os.remove.reset_mock()


def test__get_dataset_stats(test_df):
    # Assertions for successful processing
    stats = _get_dataset_stats(test_df)

    # Assert that column 1 is correctly categorized as 'percentage', and the percentages are correct
    assert stats[0]['name'] == 'Column1'
    assert stats[0]['type'] == 'percentage'
    assert stats[0]['data'][0]['name'] == 1
    assert stats[0]['data'][0]['value'] == 48
    assert stats[0]['data'][1]['name'] == 2
    assert stats[0]['data'][1]['value'] == 32
    assert stats[0]['data'][2]['name'] == 3
    assert stats[0]['data'][2]['value'] == 20

    # Assert that column 2 is correctly categorized as 'bar', and the values are correct
    assert stats[1]['name'] == 'Column2'
    assert stats[1]['type'] == 'bar'
    assert stats[1]['data'][0]['name'] == 100
    assert stats[1]['data'][0]['value'] == 8
    assert stats[1]['data'][1]['name'] == 200
    assert stats[1]['data'][1]['value'] == 8
    assert stats[1]['data'][2]['name'] == 300
    assert stats[1]['data'][2]['value'] == 16
    assert stats[1]['data'][3]['name'] == 400
    assert stats[1]['data'][3]['value'] == 6
    assert stats[1]['data'][4]['name'] == 500
    assert stats[1]['data'][4]['value'] == 6
    assert stats[1]['data'][5]['name'] == 600
    assert stats[1]['data'][5]['value'] == 6

    # Assert that column 3 is correctly categorized as 'unique', and the value is correct
    assert stats[2]['name'] == 'Column3'
    assert stats[2]['type'] == 'unique'
    assert stats[2]['data'][0]['name'] == 'Unique'
    assert stats[2]['data'][0]['value'] == 50

    # Assert that column 4 is correctly categorized as 'percentage', and the percentages are correct
    assert stats[3]['name'] == 'Column4'
    assert stats[3]['type'] == 'percentage'
    assert stats[3]['data'][0]['name'] == 21
    assert stats[3]['data'][0]['value'] == 16
    assert stats[3]['data'][1]['name'] == 20
    assert stats[3]['data'][1]['value'] == 8
    assert stats[3]['data'][2]['name'] == "Others"
    assert stats[3]['data'][2]['value'] == 76

    # Assert that if an exception is raised, an error message is returned
    stats = _get_dataset_stats(None)
    assert stats == "Error"


def test_create_ssr_parsed_file(mocker, tmp_path, test_df, convertable_df):
    # Set up sample and parsed data files path
    d1 = tmp_path / "parsed-data-files"
    d1.mkdir()
    d2 = tmp_path / "sample-data-files"
    d2.mkdir()
    # Patch the .env variable DF_LOC
    mocker.patch(DF_LOC, str(tmp_path)+"/")
    # Run the create_ssr_parsed_file function
    create_ssr_parsed_file(test_df, filename="test")
    # Assert that the parsed data file was created
    assert os.path.exists(str(d1) + "/test.json")
    # Assert that the sample data file was created
    assert os.path.exists(str(d2) + "/test.json")

    # Assert the dx was removed from the filename
    create_ssr_parsed_file(test_df, filename="dxtest2")
    # Assert that the parsed data file was created without dx in the name
    assert os.path.exists(str(d1) + "/test2.json")
    # Assert that the sample data file was created without dx in the name
    assert os.path.exists(str(d2) + "/test2.json")

    # Assert the "column" prefix was removed from the column names
    create_ssr_parsed_file(test_df, filename="test3", prefix="Column")
    # Assert that the parsed data file was created
    assert os.path.exists(str(d1) + "/test3.json")
    # Read the file @ d1/test3
    with open(str(d1) + "/test3.json", "r") as f:
        data = json.load(f)
        datatypes = data["dataTypes"]
        assert "Column1" not in datatypes
        assert "1" in datatypes

    # Assert the convertable columns were processed correctly
    create_ssr_parsed_file(convertable_df, filename="test4")
    with open(str(d1) + "/test4.json", "r") as f:
        data = json.load(f)
        datatypes = data["dataTypes"]
        # Assert the datetypes are correct
        assert datatypes["dates"] == {'type': 'date', 'dateFormat': 'YYYY-MM-DD'}
        assert datatypes["strings"] == 'string'
        assert datatypes["ints"] == 'number'
        assert datatypes["decimals"] == 'number'
        # Assert the fourth item in dataset does not have a value for decimals
        assert "decimals" not in data["dataset"][3].keys()
        # Assert the 2023-04-01 00:00:00 was converted to 2023-04-01
        assert data["dataset"][3]["dates"] == "2023-04-01"


def test_load_sample_data(mocker, tmp_path, test_sample_data):
    # Set up sample data files path
    d = tmp_path / "sample-data-files"
    d.mkdir()
    file = d / "test.json"
    # Store a demo sample data file
    file.write_text(json.dumps(test_sample_data))
    # Patch the .env variable DF_LOC
    mocker.patch(DF_LOC, str(tmp_path)+"/")
    # Run the load_sample_data function
    result = load_sample_data("test")
    # Expected result
    expected_result = {
        "count": test_sample_data["count"],
        "dataTypes": test_sample_data["dataTypes"],
        "sample": test_sample_data["dataset"][:10],
        "filterOptionGroups": list(test_sample_data["dataTypes"].keys()),
        "stats": test_sample_data["stats"],
    }
    assert result == expected_result

    # Assert that if an exception is raised, an error message is returned
    mocker.patch(DF_LOC, None)
    result = load_sample_data("test")
    assert result == "Sorry, something went wrong in our SSR update. Contact the admin for more information."


@pytest.fixture
def test_df(sample_data):
    # This DF is used for testing the _get_dataset_stats function
    data = sample_data
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def test_sample_data(sample_data):
    return {
        "count": 50,
        "dataTypes": {
            "Column1": "number",
            "Column2": "number",
            "Column3": "number",
            "Column4": "number"
        },
        "dataset": [
            {
                "Column1": sample_data["Column1"][i],
                "Column2": sample_data["Column2"][i],
                "Column3": sample_data["Column3"][i],
                "Column4": sample_data["Column4"][i],
            } for i in range(0, 10)
        ],
        "errors": [],
        "stats": _get_dataset_stats(pd.DataFrame(sample_data))
    }


@pytest.fixture
def sample_data():
    return {
        'Column1': [1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,3,3,3,3,3,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,3,3,3,3,3],  # noqa: E231, E501
        'Column2': [100,200,300,400,500,600,100,200,300,400,500,600,100,200,300,400,500,600,100,200,300,300,300,300,300,100,200,300,400,500,600,100,200,300,400,500,600,100,200,300,400,500,600,100,200,300,300,300,300,300],  # noqa: E231, E501
        'Column3': [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50],  # noqa: E231, E501
        'Column4': [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,21,21,21,20,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,21,21,21,20]  # noqa: E231, E501
    }


@pytest.fixture
def convertable_df():
    data = {
        'strings': ['a', 'b', 'c', 'd'],
        'dates': pd.to_datetime(['2023-01-01 00:00:00', '2023-02-01 00:00:00', '2023-03-01 00:00:00', '2023-04-01 00:00:00']),  # noqa: E501
        'ints': [1, 2, 3, 4],
        'decimals': [1.1, 2.2, 3.3, None]
    }
    return pd.DataFrame(data)
