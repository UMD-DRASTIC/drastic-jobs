import requests
import functools
import os
import logging
from contextlib import closing
from cli.client import DrasticClient


drastic_url = os.getenv('DRASTIC_URL', 'http://localhost')
drastic_user = os.getenv('DRASTIC_USER', 'worker')
drastic_password = os.getenv('DRASTIC_PASSWORD', 'password')
cdmi_proxy_url = os.getenv('CDMI_PROXY_URL', 'http://localhost')
__client = None


def get_client():
    global __client
    if __client is None:
        myclient = DrasticClient(drastic_url)
        res = myclient.authenticate(drastic_user, drastic_password)
        if not res.ok():
            raise IOError("Drastic authentication failed: {0}".format(res.msg()))
        __client = myclient
    return __client


def download_tempfile_from_drastic_proxy(path):
    url = '{0}{1}'.format(cdmi_proxy_url, path)
    return download_tempfile(url)


def download_tempfile(url, auth=None):
    from tempfile import NamedTemporaryFile
    tmp = NamedTemporaryFile(delete=False)
    savepath = tmp.name
    tmp.close()
    # NOTE the stream=True parameter
    logging.debug("DOWNLOADING: "+url)
    headers = {'Accept-Encoding': 'identity'}
    with closing(open(savepath, "wb")) as savehandle:
        with closing(requests.get(url, headers=headers, stream=True, auth=auth)) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    savehandle.write(chunk)
    return savepath


def stream_from_drastic_proxy(path):
    url = '{0}{1}'.format(cdmi_proxy_url, path)
    return get_httpfile_content_stream(url)


def get_httpfile_content_stream(url):
    headers = {'Accept-Encoding': 'identity'}
    resp = requests.get(url, headers=headers, stream=True)
    resp.raise_for_status()
    raw = resp.raw
    raw.read = functools.partial(resp.raw.read, decode_content=True)
    if 'content-length' in resp.headers:
        raw.len = int(resp.headers['content-length'])
    return raw


# def get_cdmi_content_stream(path):
#     url = drastic_url + '/api/cdmi/' + path
#     headers = {'Accept-Encoding': 'identity'}
#     resp = requests.get(url, auth=drastic_auth, headers=headers, stream=True)
#     resp.raise_for_status()
#     raw = resp.raw
#     raw.read = functools.partial(resp.raw.read, decode_content=True)
#     if 'content-length' in resp.headers:
#         raw.len = int(resp.headers['content-length'])
#     return raw


# def get_download_content_stream(path):
#     # FIXME this approach doesn't seem to work anymore
#     s = requests.Session()
#     logger.info('get_download_content_stream: checkpoint 0')
#     login = s.get(drastic_url + '/users/login')
#     logger.info('get_download_content_stream: checkpoint 1')
#     login.raise_for_status()
#     logger.info('get_download_content_stream: checkpoint 2')
#     bs = BeautifulSoup(login.text, "lxml")
#     csrfmiddlewaretoken = None
#     for input in bs.select('input'):
#         if input['name'] == 'csrfmiddlewaretoken':
#             csrfmiddlewaretoken = input['value']
#             break
#     data = {'username': drastic_user,
#             'password': drastic_password,
#             'csrfmiddlewaretoken': csrfmiddlewaretoken}
#     logger.info('get_download_content_stream: checkpoint 3')
#     res = s.post(drastic_url + '/users/login', data=data)
#     logger.info('get_download_content_stream: checkpoint 4')
#     res.raise_for_status()
#     logger.info('get_download_content_stream: checkpoint 5')
#     url = drastic_url + '/archive/download' + path
#     headers = {'Accept-Encoding': 'identity'}
#     resp = s.get(url, headers=headers, stream=True)
#     logger.info('get_download_content_stream: checkpoint 6')
#     resp.raise_for_status()
#     logger.info('get_download_content_stream: checkpoint 7')
#     raw = resp.raw
#     raw.read = functools.partial(resp.raw.read, decode_content=True)
#     if 'content-length' in resp.headers:
#         raw.len = int(resp.headers['content-length'])
#     return raw


# def get_cdmi_content(path):
#     url = drastic_url + '/api/cdmi/' + path
#     with closing(requests.get(url, auth=drastic_auth)) as resp:
#         resp.raise_for_status()
#         return resp.content


# def get_download_content(path):
#     s = requests.Session()
#     login = s.get(drastic_url + '/users/login')
#     login.raise_for_status()
#     bs = BeautifulSoup(login.text, "lxml")
#     csrfmiddlewaretoken = None
#     for input in bs.select('input'):
#         if input['name'] == 'csrfmiddlewaretoken':
#             csrfmiddlewaretoken = input['value']
#             break
#     data = {'username': drastic_user,
#             'password': drastic_password,
#             'csrfmiddlewaretoken': csrfmiddlewaretoken}
#     res = s.post(drastic_url + '/users/login', data=data)
#     res.raise_for_status()
#     url = drastic_url + '/archive/download/' + path
#     with closing(s.get(url)) as resp:
#         resp.raise_for_status()
#         return resp.content
