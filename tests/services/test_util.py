import os

from services.util import (remove_files, setup_parsed_loc, setup_solr_url,
                           setup_ssr_loc)


def test_remove_files(mocker):
    # Success test
    file_names = ["file1.txt", "file2.txt", "file3.txt"]
    mocker.patch('os.remove')
    result = remove_files(file_names)
    # Assert remove was called 3 times, once per file name
    assert os.remove.call_count == 3
    # Assert the correct files were removed
    for name in file_names:
        os.remove.assert_any_call(f"./staging/{name}")
    # Assert the output was "Success"
    assert result == "Success"
    os.remove.reset_mock()

    # Exception test - Mock 'os.remove' to raise an exception
    os.remove.side_effect = Exception("Test Exception")
    result = remove_files(file_names)
    # Assert that the remove function was called only once which triggers the error
    assert os.remove.call_count == 1
    # Assert that the function returns "Error removing files" when an exception is raised
    assert result == "Error removing files"

    # Reset the mock for 'os.remove'
    os.remove.reset_mock()


def test_setup_solr_url(mocker):
    # SOLR_SUBDOMAIN, SOLR_ADMIN_USERNAME, AND SOLR_ADMIN_PASSWORD ARE SET
    mocker.patch.dict(os.environ, {
        'SOLR_SUBDOMAIN': 'dx-solr',
        'SOLR_ADMIN_USERNAME': 'admin',
        'SOLR_ADMIN_PASSWORD': 'password'
    })
    solr_url = setup_solr_url()
    assert solr_url == "http://admin:password@dx-solr:8983/solr"
    mocker.patch.dict(os.environ, clear=True)

    # SOLR_SUBDOMAIN IS SET, SOLR_ADMIN_USERNAME AND SOLR_ADMIN_PASSWORD ARE NOT SET
    mocker.patch.dict(os.environ, {
        'SOLR_SUBDOMAIN': 'dx-solr'
    })
    solr_url = setup_solr_url()
    assert solr_url == "http://dx-solr:8983/solr"
    mocker.patch.dict(os.environ, clear=True)

    # SOLR_SUBDOMAIN IS NOT SET, SOLR_ADMIN_USERNAME AND SOLR_ADMIN_PASSWORD ARE SET
    mocker.patch.dict(os.environ, {
        'SOLR_ADMIN_USERNAME': 'admin',
        'SOLR_ADMIN_PASSWORD': 'password'
    })
    solr_url = setup_solr_url()
    assert solr_url == "http://admin:password@localhost:8983/solr"
    mocker.patch.dict(os.environ, clear=True)

    # SOLR_SUBDOMAIN, SOLR_ADMIN_USERNAME, AND SOLR_ADMIN_PASSWORD ARE NOT SET
    solr_url = setup_solr_url()
    assert solr_url == "http://localhost:8983/solr"
    mocker.patch.dict(os.environ, clear=True)


def test_setup_ssr_loc(mocker):
    # DATA_EXPLORER_SSR IS SET
    mocker.patch.dict(os.environ, {
        'DATA_EXPLORER_SSR': 'test/path/'
    })
    solr_url = setup_ssr_loc()
    assert solr_url == "test/path/additionalDatasets.json"
    mocker.patch.dict(os.environ, clear=True)
    # DATA_EXPLORER_SSR IS NOT SET
    solr_url = setup_ssr_loc()
    assert solr_url == "./additionalDatasets.json"


def test_setup_parsed_loc(mocker):
    # DATA_EXPLORER_SSR IS SET
    mocker.patch.dict(os.environ, {
        'DATA_EXPLORER_SSR': 'test/path/'
    })
    solr_url = setup_parsed_loc()
    assert solr_url == "test/path/"
    mocker.patch.dict(os.environ, clear=True)
    # DATA_EXPLORER_SSR IS NOT SET
    solr_url = setup_parsed_loc()
    assert solr_url == "./"
