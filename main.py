import os
import time

from waifuc.action import FirstNSelectAction, \
    ModeConvertAction, RandomFilenameAction, AlignMinSizeAction
from waifuc.export import SaveExporter
from page_source import AnimePicturesPageSource
from waifuc.source.anime_pictures import OrderBy
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, ALL_COMPLETED

os.environ['http_proxy'] = 'http://127.0.0.1:1080'
os.environ['https_proxy'] = 'http://127.0.0.1:1080'

def spider(start, end):
    s = AnimePicturesPageSource([], order_by=OrderBy.DATE)
    s.set_page_range(start, end)

    # crawl images, process them, and then save them to directory with given format
    s.attach(
        # preprocess images with white background RGB
        ModeConvertAction('RGB', 'white'),

        # if min(height, weight) > 800, resize it to 800
        AlignMinSizeAction(1024),

        # FilterSimilarAction('all'),  # filter again
        #FirstNSelectAction(100),  # first 200 images
        RandomFilenameAction(ext='.webp'),  # random rename files
    ).export(
        # save to surtr_dataset directory
        SaveExporter(f'data_v1/anime_dataset_page{start}-{end}')
    )

if __name__ == '__main__':
    from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, T5EncoderModel

    with ThreadPoolExecutor(max_workers=20) as t:
        page_step = 10
        all_task = [t.submit(spider, page*page_step, page*page_step+page_step) for page in range(20)]
        wait(all_task, return_when=ALL_COMPLETED)
        print('finished')
        print(wait(all_task))
