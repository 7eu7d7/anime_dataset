import os
from typing import Iterator, Tuple, Union

from hbutils.system import urlsplit
from pyquery import PyQuery as pq
from waifuc.source import SankakuSource, AnimePicturesSource, ZerochanSource
from waifuc.source.web import NoURL
from waifuc.utils import srequest

try:
    from typing import Literal
except (ImportError, ModuleNotFoundError):
    from typing_extensions import Literal

class SankakuPageSource(SankakuSource):
    def set_page_range(self, start=1, end=2):
        self.page_start = start+1
        self.page_end = end+1

    def _iter_data(self) -> Iterator[Tuple[Union[str, int], str, dict]]:
        self._login()

        page = self.page_start
        while True:
            resp = srequest(self.auth_session, 'GET', 'https://capi-v2.sankakucomplex.com/posts', params={
                'lang':'en',
                'page':str(page),
                'limit':'100',
                'tags':' '.join(self.tags),
            })
            resp.raise_for_status()
            if not resp.json():
                break

            for data in resp.json():
                if 'file_type' not in data or 'image' not in data['file_type']:
                    continue

                try:
                    url = self._select_url(data)
                except NoURL:
                    continue

                _, ext_name = os.path.splitext(urlsplit(url).filename)
                if ext_name.lower() == '.gif':
                    continue

                filename = f'{self.group_name}_{data["id"]}{ext_name}'
                meta = {
                    'sankaku':data,
                    'group_id':f'{self.group_name}_{data["id"]}',
                    'filename':filename,
                    'tags':{key:1.0 for key in [t_item['name'] for t_item in data['tags']]}
                }
                yield data["id"], url, meta

            page += 1
            if page>=self.page_end:
                break

class AnimePicturesPageSource(AnimePicturesSource):
    def set_page_range(self, start=1, end=2):
        self.page_start = start
        self.page_end = end

    def _iter_data(self) -> Iterator[Tuple[Union[str, int], str, dict]]:
        page = self.page_start
        while True:
            resp = srequest(self.session, 'GET', f'{self.__root__}/api/v3/posts', params=self._params(page))
            resp.raise_for_status()

            posts = resp.json()['posts']
            if not posts:
                break

            for post in posts:
                resp_page = srequest(self.session, 'GET', f'{self.__root__}/posts/{post["id"]}?lang=en')
                resp_page.raise_for_status()

                url = self._get_url(post, resp_page)
                tags = [item.text().replace(' ', '_') for item in pq(resp_page.text)('ul.tags li > a').items()]
                _, ext_name = os.path.splitext(urlsplit(url).filename)
                if ext_name.lower() == '.gif':
                    continue

                filename = f'{self.group_name}_{post["id"]}{ext_name}'
                meta = {
                    'anime_pictures':post,
                    'group_id':f'{self.group_name}_{post["id"]}',
                    'filename':filename,
                    'tags':{key:1.0 for key in tags}
                }
                yield post['id'], url, meta

            page += 1
            if page>=self.page_end:
                break

class ZerochanPageSource(ZerochanSource):
    def set_page_range(self, start=1, end=2):
        self.page_start = start+1
        self.page_end = end+1

    def _iter_data(self) -> Iterator[Tuple[Union[str, int], str, dict]]:
        page = self.page_start
        while True:
            resp = srequest(self.session, 'GET', self._base_url,
                            params={**self._params, 'p':str(page), 'l':'200'},
                            raise_for_status=False)
            if resp.status_code in {403, 404}:
                break
            resp.raise_for_status()

            json_ = resp.json()
            if 'items' in json_:
                items = json_['items']
                for data in items:
                    url = self._get_url(data)
                    _, ext_name = os.path.splitext(urlsplit(url).filename)
                    if ext_name.lower() == '.gif':
                        continue
                    filename = f'{self.group_name}_{data["id"]}{ext_name}'
                    meta = {
                        'zerochan':{
                            **data,
                            'url':url,
                        },
                        'group_id':f'{self.group_name}_{data["id"]}',
                        'filename':filename,
                    }
                    yield data["id"], url, meta
            else:
                break

            page += 1
            if page>=self.page_end:
                break