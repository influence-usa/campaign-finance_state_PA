import os
import logging
import time

from multiprocessing.dummy import Pool as ThreadPool

from utils import set_up_logging

log = set_up_logging('download', loglevel=logging.DEBUG)


# GENERAL DOWNLOAD FUNCTIONS
def response_download(response, output_loc):
    if response.ok:
        try:
            with open(output_loc, 'wb') as output_file:
                for chunk in response.iter_content():
                    output_file.write(chunk)
            return response.headers['content-length']
        except Exception as e:
            log.error(e)
    else:
        log.error('response not okay: '+response.reason)
        raise Exception('didn''t work, trying again')


def log_result(result):
    if result[0] == 'success':
        url, loc, content_length = result[1:]
        log.info(
            'success: {source} => {dest}({size})'.format(
                source=url, dest=loc, size=content_length))
    elif result[0] == 'failure':
        url, loc, exception = result[1:]
        log.info(
            'failure: {source} => {dest}\n {e}'.format(
                source=url, dest=loc, e=str(exception)))
    else:
        raise Exception


def download(val, get_response_loc_pair):
    for i in xrange(5):
        _response, _loc = get_response_loc_pair(val)
        _url = _response.url
        if is_not_cached(_response, _loc):
            try:
                content_length = response_download(_response, _loc)
                return ('success', _url, _loc, content_length)
            except Exception:
                log.warn('{url} something went wrong, trying again '
                         '({code} - {reason})'.format(
                             url=_response.url,
                             code=_response.status_code,
                             reason=_response.reason))
                time.sleep(5)
        else:
            log.info('cached, not re-downloading')
            return('success', _url, _loc, 'cached')
    return ('failure', _response.url, _loc, '[{code}] {reason}'.format(
        code=_response.status_code, reason=_response.reason))


def is_not_cached(response, output_loc):
    response, output_loc
    if os.path.exists(output_loc):
        downloaded_size = int(os.path.getsize(output_loc))
        log.debug(
            'found {output_loc}: {size}'.format(
                output_loc=output_loc,
                size=downloaded_size))
        size_on_server = int(response.headers['content-length'])
        if downloaded_size != size_on_server:
            log.debug(
                're-downloading {url}: {size}'.format(
                    url=response.url,
                    size=size_on_server))
            return True
        else:
            response.close()
            return False
    else:
        return True


def download_all(vals, get_response_loc_pair, options):
    threaded = options.get('threaded', False)
    thread_num = options.get('thread_num', 4)

    if threaded:
        log.info("starting threaded download")
        pool = ThreadPool(thread_num)
        for val in vals:
            log.debug("async start for {}".format(str(val)))
            pool.apply_async(download, args=(val, get_response_loc_pair),
                             callback=log_result)
        pool.close()
        pool.join()
    else:
        for val in vals:
            log_result(download(val, get_response_loc_pair))
