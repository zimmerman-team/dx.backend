def json_return(code, data=None, error=None, error_message=None):
    """
    Return a jsonify-d flask object with the provided error code. Defaults to 204.

    :param code: the HTTP status code
    :param data: the data to return, can be an object, or a string
    """
    if error:
        return {'code': code, 'error': error, 'error_message': error_message}, code
    if data or data == []:
        return {'code': code, 'result': data}, code
    else:
        return {'code': 204, 'result': 'We were unable to process, please contact your system administrator.'}, 204
