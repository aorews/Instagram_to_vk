from datetime import datetime
from typing import Callable, Tuple
import asyncio
import base64
import math
import os
import shutil
import time

import aiohttp
import instaloader
import pandas as pd
import vk_api


CFG_PATH_VK = 'cfg_1'
CFG_PATH_INST = 'cfg_2'
LAST_START_PATH = 'cfg_0'
INST_SESSION_PATH = 'cfg_3'


class AsyncWorker():
    def __init__(self, data: list, func: Callable[..., None], num_workers: int = 4) -> None:
        """Class which asynchronically aplies provided function to given list.

        Args:
            data (list): Data to be working with.
            func (Callable[..., None]): Async function which will be applied.
            num_workers (int, optional): Number of parallel workers. Defaults to 4.
        """
        self.num_workers = num_workers
        self.futures = list()
        self.data = data
        self.func = func
        asyncio.get_event_loop().run_until_complete(self.start_async())

    async def start_async(self) -> None:
        """Applying function to list"""
        ranges = math.ceil(len(self.data)/self.num_workers)

        for worker in range(self.num_workers):
            batch = self.data[ranges*worker: ranges*(worker+1)]
            self.futures.append(asyncio.ensure_future(self.func(batch)))

        for future in self.futures:
            await future


class DataPreparer():
    def __init__(self) -> None:
        """Class to handle login to VK, Instagram,
        storing and manipulating time of previous application start
        and getting links of Instagram pages and VK groups.
        """
        self.vk_login = None
        self.vk_pass = None
        self.vk_session = None

        self.inst_login = None
        self.inst_pass = None
        self.inst_session = None

        self.links = None
        self.broken_links = list()

        self.date_now = datetime.utcnow()
        self.last_start_date = self.get_last_start_date()

        if self.vk_connection() & self.inst_connection() & self.load_links():
            print('OK!')

    def vk_connection(self) -> bool:
        """Establishing connection to VK.

        Returns:
            bool: True if connection is OK, False otherwise.
        """
        self.get_credentials(CFG_PATH_VK, 'VK')

        if self.vk_auth():
            print('VK login ok!')
            return True
        print('VK login failed!')
        return False

    def get_credentials(self, path: str, mode: str) -> None:
        """Getting credentials if they exists, otherwise
        asking user to write them in console and then storing them to file.
        If user provided broken credentials deleting file which holds credentials.

        Args:
            path (str): path to credentials, provided as module constant.
            mode (str): type of credentials to work with: VK or INST.
        """
        if os.path.exists(path):
            with open(path) as file:
                row = file.readline()
                try:
                    login, passw = base64.b64decode(
                        row).decode('utf-8').split(',')
                except ValueError:
                    os.remove(path)
        else:
            login = input('Write login to '+mode+': ')
            passw = input('Write pass to '+mode+': ')
            with open(path, 'w') as file:
                file.write(base64.b64encode(
                    (login+','+passw).encode('utf-8')).decode('utf-8'))

        if mode == 'VK':
            self.vk_login = login
            self.vk_pass = passw
        else:
            self.inst_login = login
            self.inst_pass = passw

    def captcha_handler(self, captcha):
        """ При возникновении капчи вызывается эта функция и ей передается объект
            капчи. Через метод get_url можно получить ссылку на изображение.
            Через метод try_again можно попытаться отправить запрос с кодом капчи
        """

        key = input("Enter captcha code {0}: ".format(captcha.get_url())).strip()

        # Пробуем снова отправить запрос с капчей
        return captcha.try_again(key)

    
    def auth_handler(self):
        code = input("Код для двухфакторной аутентификации: ")
        return code, True


    def vk_auth(self) -> bool:
        """Authenticating to VK.

        Returns:
            bool: True if Auth is done correctly, False otherwise.
        """
        self.vk_session = vk_api.VkApi(self.vk_login, self.vk_pass, auth_handler = self.auth_handler, captcha_handler = self.captcha_handler)

        try:
            self.vk_session.auth()
            return True
        except vk_api.exceptions.AuthError as e:
            print(e)
            os.remove(CFG_PATH_VK)
            return False

    def inst_connection(self) -> bool:
        """Establishing connection to Instagram.

        Returns:
            bool: True if connection is OK, False otherwise.
        """
        self.get_credentials(CFG_PATH_INST, 'INST')

        if self.inst_auth():
            print('Instagram login ok!')
            return True
        print('Instagram login failed!')
        return False

    def inst_auth(self) -> bool:
        """Authenticating to Instagram.

        Returns:
            bool: True if Auth is done correctly, False otherwise.
        """
        self.inst_session = instaloader.Instaloader(
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            request_timeout=3,
        )

        if os.path.exists(INST_SESSION_PATH):
            try:
                self.inst_session.load_session_from_file(self.inst_login, filename=INST_SESSION_PATH)
            except Exception:
                os.remove(INST_SESSION_PATH)
            return True
        else:
            try:
                self.inst_session.login(self.inst_login, self.inst_pass)
                self.inst_session.save_session_to_file(filename=INST_SESSION_PATH)
                return True
            except instaloader.exceptions.InstaloaderException:
                os.remove(CFG_PATH_INST)
                return False

    def load_links(self) -> bool:
        """Loading file with provided links to Instagram profiles and VK albums.

        Returns:
            bool: True if file loaded correctly, False otherwise.
        """
        try:
            self.links = pd.read_csv('Links.csv', delimiter=';')
            return True
        except OSError:
            links = pd.DataFrame(
                {'Instagram Link': ['https://www.instagram.com/PLACE_NAME_1/',
                                    'https://www.instagram.com/PLACE_NAME_2/'],
                 'VK album link post photo': ['https://vk.com/album-PLACE_LINK_1',
                                              'https://vk.com/album-PLACE_LINK_2'],
                 'VK album link stories photo': ['https://vk.com/album-PLACE_LINK_1',
                                                 'https://vk.com/album-PLACE_LINK_2'],
                 'VK album link video': ['https://vk.com/videos-PLACE_LINK_1?section=album_PLACE_LINK_1',
                                         'https://vk.com/videos-PLACE_LINK_2?section=album_PLACE_LINK_2']
                 }
            )
            links.to_csv('Links.csv', index=False)
            print('Add your links to Links.csv!')
            return False

    def check_links(self) -> bool:
        """Checking provided links on their accesibility.
        Not using now because instagram limits amount of page loads and
        this function is too costly in this term.

        Returns:
            bool: True if check is done, False otherwise.
        """
        links_last_change = datetime.fromtimestamp(
            os.path.getmtime('Links.csv'))
        days_delta = (datetime.now() - links_last_change).days
        if days_delta >= 0:
            return True
        AsyncWorker(self.links.values.ravel(), self.response_check, 1)
        if self.broken_links != list():
            print('Change these links:')
            for url in self.broken_links:
                print(url)
            return False
        print('Links ok!')
        return True

    async def response_check(self, links: list) -> None:
        """Async function to be given to AsyncWorker class.

        Args:
            links (list): List of links to be checked on availability.
        """
        async with aiohttp.ClientSession(trust_env=True) as client:
            for url in links:
                async with client.get(aiohttp.client.URL(url, encoded=True)) as response:
                    if response.status == 429:
                        print(
                            'The maximum number of requests per hour has been exceeded. Instagram banned you')
                        self.broken_links.append(' ')
                        break
                    if response.status not in range(200, 300):
                        self.broken_links.append(url)
                    await asyncio.sleep(0.5)

    def get_last_start_date(self) -> datetime:
        """Getting last start date of module if file exists,
        otherwise creating file and writing time.

        Returns:
            datetime: UTC time of last module start if ISO format.
        """
        if os.path.exists(LAST_START_PATH):
            with open(LAST_START_PATH) as file:
                last_start_date = datetime.fromisoformat(file.read())
                return last_start_date
        else:
            with open(LAST_START_PATH, 'w') as file:
                file.write(str(self.date_now.isoformat()))
                return self.date_now


class InstagramDownloader:
    def __init__(self,
                 instagram_session: instaloader.instaloader.Instaloader,
                 links: pd.DataFrame,
                 last_date: datetime,
                 date_now: datetime) -> None:
        """Class to download new posts and stories from provided page links
        between last start date of module and now.

        Args:
            instagram_session (instaloader.instaloader.Instaloader): Instagram session which will be used to download stories.
            links (pd.DataFrame): Dataframe with provided links to Instagram pages and VK albums.
            last_date (datetime): Last date of module start.
            date_now (datetime): Current date of module start.
        """
        self.inst_session = instagram_session
        self.links = links
        self.last_date = last_date
        self.date_now = date_now

        self.profile_ids = list()
        self.pages = self.get_pages()

        print(f'Last downloading date: {self.last_date}!')
        self.folder_counter = self.get_last_folder_counter()
        self.download_posts()
        self.download_stories()
        self.update_last_start_date()

    def get_pages(self) -> list:
        """Get usenames from provided instagram pages.

        Returns:
            list: List of instagram usernames.
        """
        pages = self.links['Instagram Link'].apply(
            lambda x: x.split('/')[3]).values
        return pages

    def download_posts(self) -> None:
        """Download new Instagram posts.
        """
        print('Getting posts!')
        for page in self.pages:
            profile = instaloader.Profile.from_username(
                self.inst_session.context, page)
            self.profile_ids.append(profile.userid)
            posts = profile.get_posts()
            print(page)
            for post in posts:
                if self.last_date < post.date:
                    self.inst_session.download_post(
                        post, f"{self.folder_counter}_posts:{page}")
                    self.folder_counter += 1
                else:
                    break

    def download_stories(self) -> None:
        """Download new Instagram stories.
        """
        print('Getting stories!')
        for story in self.inst_session.get_stories(self.profile_ids):
            username = story.owner_profile.username
            video_required = self.links.loc[self.links['Instagram Link'] ==
                                            f'https://www.instagram.com/{username}/', 'VK album link stories video'].notna().item()
            print(username)
            for item in story.get_items():
                # Bound by date of last script start
                if self.last_date < item.date:
                    # Not downloading video from instagram pages without VK story video link
                    if video_required or not item.is_video:
                        self.inst_session.download_storyitem(
                            item, f'{self.folder_counter}_stories:{username}')
                        self.folder_counter += 1
                else:
                    break

    def update_last_start_date(self) -> None:
        """Updating time of module last start date after finishing downloading new posts and stories
        """
        with open(LAST_START_PATH, 'w') as file:
            file.write(str(self.date_now.isoformat()))

    @staticmethod
    def get_last_folder_counter() -> int:
        """If there are folders which were not uploaded to VK and hence deleted
        then to keep them in order and not override
        start naming new folders with value of last created folder +1

        Returns:
            int: number to start naming with.
        """
        folders = DataCollector.get_folders()
        last_folder = 0
        for folder in folders:
            temp = int(folder[0].split('_')[0])
            if temp > last_folder:
                last_folder = temp

        return last_folder+1


class DataCollector:
    def __init__(self, links: pd.DataFrame) -> None:
        """Collecting all downladed files and sorting them by type
        to feed into VKUploader class.

        Args:
            links (pd.DataFrame): Dataframe with provided links to Instagram pages and VK albums.
        """
        self.links = links
        self.videos = list()
        self.images = list()

        print('Fetching files!')
        self.get_files_to_upload()

    def get_links(self, username: str, is_post: bool) -> Tuple[str, str, str, str]:
        """Get VK group and album ids for image and video files for provided username and type of post.

        Args:
            username (str): Instagram username.
            is_post (bool): True if need links to upload Instagram post, False if uploading stories.

        Returns:
            Tuple[str, str, str, str]: Tuple of ids to VK folders.
        """
        col_name = 'post' if is_post else 'stories'
        vk_image_group, vk_image_album = self.links.loc[self.links['Instagram Link'] ==
                                                        f'https://www.instagram.com/{username}/', f'VK album link {col_name} photo'].item().split('-')[1].split('_')
        try:
            vk_video_temp = self.links.loc[self.links['Instagram Link'] ==
                                           f'https://www.instagram.com/{username}/', f'VK album link {col_name} video'].item().split('-')[1]
            vk_video_group = vk_video_temp.split('?')[0]
            vk_video_album = vk_video_temp.split('_')[1]
        except AttributeError:
            vk_video_group = vk_video_album = None

        return vk_image_group, vk_image_album, vk_video_group, vk_video_album

    @staticmethod
    def get_description(folder: str) -> str:
        """Get descriprion if it exists for downloaded post.

        Args:
            folder (str): folder to be searched.

        Returns:
            str: Description.
        """
        picture_description = None
        for item in os.listdir(folder):
            if '.txt' in item:
                path = folder+'/'+item
                with open(path, encoding='UTF-8') as file:
                    picture_description = file.read()
        return picture_description

    @staticmethod
    def get_folders() -> list:
        """Get folder names which were created by InstagramDownloader class.

        Returns:
            list: List of str paths to folders with donwloaded data.
        """
        folders = list()
        for element in os.listdir():
            if '：' in element:
                folders.append(element)
        return folders

    def get_files_to_upload(self) -> None:
        """Getting and sorting tuples
            (path, video_name, description, vk_video_group, vk_video_album) for videos
            (path, description, vk_image_group, vk_image_album) for images.
        """
        folders = self.get_folders()

        for folder in folders:
            is_post = 'posts：' in folder
            username = folder.split('：')[1]
            vk_image_group, vk_image_album, vk_video_group, vk_video_album = self.get_links(
                username, is_post)

            description = self.get_description(folder)

            for item in os.listdir(folder):
                path = folder+'/'+item
                date = item[2:10].replace('-', '')
                if '.mp4' in item:
                    addition = f' Instagram @{username}' if is_post else ' Instagram stories'
                    video_name = date+addition
                    self.videos.append(
                        (path, video_name, description, vk_video_group, vk_video_album))
                elif '.jpg' in item:
                    self.images.append(
                        (path, description, vk_image_group, vk_image_album))

        self.images = self.sort_folders(self.images)
        self.videos = self.sort_folders(self.videos)

    @staticmethod
    def sort_folders(folders: list) -> list:
        """

        Args:
            folders (list): List of str paths to be sorted based on their folder counter.

        Returns:
            list: Sorted list of str paths.
        """
        sorted_folder = sorted(folders, key=lambda x: int(x[0].split('_')[0]))
        return sorted_folder


class VKUploader:
    def __init__(self, vk_session: vk_api.vk_api.VkApi, images: list, videos: list) -> None:
        """Class to upload data to VK.

        Args:
            vk_session (vk_api.vk_api.VkApi): Session with access to provided VK group folders.
            images (list): List of tuples with information about images to be uploaded in type
            (path, description, vk_image_group, vk_image_album).
            videos (list): List of tuples with information about videos to be uploaded in type
            (path, video_name, description, vk_video_group, vk_video_album).
        """
        self.vk_session = vk_session
        self.upload = vk_api.VkUpload(self.vk_session)
        self.images = images
        self.videos = videos

        self.image_uploader()
        self.video_uploader()
        self.folder_remover()

    def image_uploader(self) -> None:
        """Image uploader to VK. Deletes file after correct uploading.
        """
        for item in self.images:
            cap = None
            if item[1] is not None:
                cap = item[1].replace('@', '@ ')

            photo = self.upload.photo(
                photos=item[0],
                caption=cap,
                group_id=item[2],
                album_id=item[3]
            )
            vk_photo_url = 'https://vk.com/photo{}_{}'.format(
                photo[0]['owner_id'], photo[0]['id'])
            profile_info = item[0].split('/')[0]
            print('\n'+f'{profile_info} Vk upload done to {vk_photo_url}'+'\n')

            os.remove(item[0])

    def video_uploader(self) -> None:
        """Video uploader to VK. Deletes file after correct uploading.
        """
        for item in self.videos:
            desc = None
            if item[2] is not None:
                desc = item[2].replace('@', '@ ')

            video = self.upload.video(
                video_file=item[0],
                name=item[1],
                description=desc,
                group_id=item[3],
                album_id=item[4]
            )
            vk_video_url = 'https://vk.com/video{}_{}'.format(
                video['owner_id'], video['video_id'])
            profile_info = item[0].split('/')[0]
            print('\n'+f'{profile_info} Vk upload done to {vk_video_url}'+'\n')

            os.remove(item[0])
            time.sleep(3)

    @staticmethod
    def folder_remover() -> None:
        """Delets folders in which there are no files of type mp4 and jpg.
        """
        for element in os.listdir():
            if '：' in element:
                delete_folder = True
                for item in os.listdir(element):
                    if ('.mp4' or '.jpg') in item:
                        delete_folder = False
                if delete_folder:
                    shutil.rmtree(element)


if __name__ == '__main__':
    data = DataPreparer()
    InstagramDownloader(data.inst_session, data.links,
                        data.last_start_date, data.date_now)
    fetched = DataCollector(data.links)
    VKUploader(data.vk_session, fetched.images, fetched.videos)
    print('Uploading done!')
    input()
