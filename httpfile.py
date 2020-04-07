#!/usr/bin/env python
"""
Lazily fetch from HTTP
"""

# Permission to use, copy, modify, and/or distribute this software
# for any purpose with or without fee is hereby granted.

# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
# WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
# AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL
# DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA
# OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
# TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

#<dictproxy {'connection': 'keep-alive',
# 'content-length': '500',
# 'content-range': 'bytes 13199-13698/13699',
# 'content-type': 'application/zip',
# 'date': 'Thu, 28 Jun 2012 14:27:50 GMT',
# 'last-modified': 'Thu, 28 Jun 2012 14:20:43 GMT',
# 'server': 'nginx/1.1.19'}>

import requests
import logging
import bisect
import re
import itertools

log = logging.getLogger(__name__)

class HTTPSlice(object):
    content_range = re.compile('bytes (?P<first>\d*)-(?P<last>\d*)/(?P<length>\d*)')

    def __init__(self, url, backing):
        self.session = requests.session()
        self.backing = open(backing, "w+b")
        self.url = url
        self.segments = []
        self.length = -1
        self.complete = False

    def __getitem__(self, key):
        """Return a slice of the file."""
        assert isinstance(key, slice)
        assert key.step is None
        self.request_slice(key)
        if key.start != None:
            self.backing.seek(key.start)
            if key.stop != None:
                return self.backing.read(key.stop - key.start)
            else:
                return self.backing.read()
        elif key.stop != None and key.stop < 0:
            assert self.length >= 0
            self.backing.seek(self.length + key.stop)
            return self.backing.read()
        raise NotImplementedError(key)

    def request_slice(self, s):
        headers = None
        if not (s.start == None and s.stop == None):
            first = ''
            last = '-'
            if s.start != None:
                first = s.start
                if s.stop != None:
                    last = '-%s' % (s.stop-1)
            elif s.stop != None:
                assert s.stop < 0
                last = s.stop
            headers = {'range':'bytes=%s%s' % (first, last)}
            log.debug("%r", headers)
        self.handle_response(self.session.get(self.url, headers=headers))

    def append_segment(self, segment):
        self.segments.append(segment)
        self.segments.sort()

    def merge_segments(self):
        merged = []
        for a, b in self.segments:
            if merged and a <= merged[-1][1]:
                la, lb = merged[-1]
                merged[-1] = (la, max(lb, b))
            else:
                merged.append((a, b))
        return merged

    def overlaps(self, seg1, seg2):
        """Return parts of seg1 that are not contained in seg2"""
        a, b = seg1        
        x, y = seg2
        if a >= x and b <= y:
            return []
        if b <= x or y <= a:
            return [seg1]
        parts = []
        if a < x:
            parts.append((a, x))
        if b > y:
            parts.append((y, b))
        return parts
            
    def split_segment(self, segment, segments=[]):
        """Return missing parts of segment compared to segments"""
        todo = [segment]
        for test in segments:
            overlaps = self.overlaps(todo[-1], test)
            if not overlaps: # fully contained
                break
            todo[-1:] = overlaps
        return todo

    def write_segment(self, seek, response):
        self.backing.seek(seek)
        begin = self.backing.tell()
        self.backing.writelines(response.iter_content())
        end = self.backing.tell()
        self.append_segment((begin, end))

    def handle_response(self, response):
        """Add a response to our backing file."""
        if response.status_code == 200:
            self.write_segment(0, response)
            self.complete = True
            self.length = self.backing.tell()
        elif response.status_code == 206:
            m = self.content_range.match(response.headers['content-range'])
            if not m:
                raise NotImplementdError('No content-range header %r' % 
                        response.headers)
            length = int(m.group('length')) # of entire object
            self.length = length
            first = int(m.group('first'))
            last = int(m.group('last'))
            self.write_segment(first, response)

class HTTPFile(object):
    def __init__(self, url, filename):
        self.session = requests.session()
        self.shadow = open('/home/dholth/public_html/xmlrpc.zip', 'rb')
        self.segments = []
        self.backing = open(filename + ".part", "w+b")
        self.url = url
        self._seek = (0, 0)
        self._length = -1
        self._tell = 0

    @property
    def length(self):
        if self._length < 0:
            head = self.session.head(self.url)
            head.content
            self._length = int(head.headers['content-length'])
            self.backing.truncate(self._length)
            # store headers... esp. last-modified or etags
        return self._length

    def tell(self):
        t = 0
        if self._seek == (0, 0):
            t = self._tell
        if self._seek == (0, 2):
            t = self.length
        assert t == self.shadow.tell()
        return t

    def seek(self, offset, from_what=0):
        # clear seek after xxx / lazy seek
        self.shadow.seek(offset, from_what)
        self._seek = (offset, from_what)        
        log.debug("seek %r", self._seek)

    def range_header(self, size):
        offset, from_what = self._seek
        if from_what == 0:
            if offset == 0 and size == -1:
                return None
            elif offset != 0:
                if size == -1:
                    return {'range':'bytes=%d-' % offset}
                else:
                    return {'range':'bytes=%d-%d' % (offset, offset+size-1)}
        elif from_what == 2:
            # we can get 'the last 500 bytes' but not 'the first 250 of
            # the last 500 bytes' (without knowing the length)
            assert offset < 0
            if self._length < 0:
                return { 'range':'bytes=%d' % offset }
            else:
                if size < 0:
                    return {'range':'bytes=%d-' % (self.length+offset)}
                return { 'range':'bytes=%d-%d' % \
                            (self.length+offset, self.length+offset+size-1) }

    def read(self, size=-1):
        headers = self.range_header(size)
        log.debug("read %d %r", size, headers)
        r = self.session.get(self.url, headers=headers)
        content = r.content
        sc = self.shadow.read(size)
        assert sc == content, (sc, content)
        return content

def test():
    import logging
    import zipfile
    logging.basicConfig(level=logging.DEBUG)
    url = 'http://localhost/~dholth/xmlrpc.zip'
    hf = HTTPFile(url, '/tmp/backing')
    zf = zipfile.ZipFile(hf)
    print zf.filelist

    hs = HTTPSlice('http://localhost/~dholth/httpfile.py', '/tmp/httpfile.py')
    hs[0:10]
    hs[:-10]
    hs[20:]
    return hs

if __name__ == "__main__":
    hs = test()

